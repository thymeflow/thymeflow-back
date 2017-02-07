package com.thymeflow.sync

import java.io.{IOException, InputStream}
import java.net.URLEncoder
import java.nio.file.{DirectoryStream, Files, Path}
import java.util.zip.ZipFile

import akka.actor.{ActorRef, Props}
import akka.stream.scaladsl.Source
import com.thymeflow.rdf.model.document.Document
import com.thymeflow.rdf.model.vocabulary.Personal
import com.thymeflow.rdf.model.{ModelDiff, StatementSet}
import com.thymeflow.service._
import com.thymeflow.service.source.PathSource
import com.thymeflow.sync.Synchronizer.Update
import com.thymeflow.sync.converter._
import com.thymeflow.sync.publisher.ScrollDocumentPublisher
import com.thymeflow.update.UpdateResults
import com.typesafe.config.Config
import org.apache.commons.io.FilenameUtils
import org.eclipse.rdf4j.model.{IRI, ValueFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.Future

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
object FileSynchronizer extends Synchronizer {

  private val vCardConverter: (ValueFactory, Config) => Converter = new VCardConverter(_)(_)
  private val iCalConverter: (ValueFactory, Config) => Converter = new ICalConverter(_)(_)
  private val emailMessageConverter: (ValueFactory, Config) => Converter = new EmailMessageConverter(_)(_)

  private var registeredConverters: Map[String, (ValueFactory, Config) => Converter] = Map(
    "message/rfc822" -> emailMessageConverter,
    "text/calendar" -> iCalConverter,
    "text/vcard" -> vCardConverter,
    "text/x-vcard" -> vCardConverter
  )
  private var registeredExtensions: Map[String, String] = Map(
    "eml" -> "message/rfc822",
    "ics" -> "text/calendar",
    "vcf" -> "text/vcard",
    "zip" -> "application/zip"
  ).withDefaultValue("application/octet-stream")

  def source(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory, supervisor)))

  def registerConverter(mimeType: String, converter: (ValueFactory, Config) => Converter) = {
    registeredConverters += mimeType -> converter
  }

  def registerExtension(extension: String, mimeType: String) = {
    registeredExtensions += extension -> mimeType
  }

  private sealed trait State

  private case class PathState(serviceAccountSource: ServiceAccountSource, filePath: Path, documentPath: Path, mimeType: Option[String] = None) extends State

  private case class ZipFileState(serviceAccountSource: ServiceAccountSource, zipFile: ZipFile, documentPath: Path) extends State

  private case class ZipIteration(zipFile: ZipFile, iterator: Iterator[ConvertibleFile]) extends State

  private case class DocumentIteration(task: ServiceAccountSourceTask[Working], iterator: Iterator[Document]) extends State

  private case class DirectoryIteration(directoryStream: DirectoryStream[Path], iterator: Iterator[State], directories: scala.collection.mutable.Builder[PathState, Vector[PathState]]) extends State

  private case class ConvertibleFile(serviceAccountSource: ServiceAccountSource, path: Path, converter: Converter, inputStream: InputStream) extends State

  private class Publisher(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config)
    extends ScrollDocumentPublisher[Document, (Vector[State])] with BasePublisher {

    val converters = registeredConverters.map {
      case (k, converter) =>
        k -> converter(valueFactory, config)
    }

    override def receive: Receive = super.receive orElse {
      case serviceAccountSources: ServiceAccountSources =>
        serviceAccountSources.sources.foreach {
          case (serviceAccountSource, source: PathSource) =>
            val documentPath = source.documentPath.getOrElse(source.path)
            supervisor ! ServiceAccountSourceTask(source = serviceAccountSource, documentPath.toString, Idle)
            queue(Vector(PathState(serviceAccountSource, source.path, documentPath, source.mimeType)))
          case _ =>
        }
      case Update(diff) => sender() ! applyDiff(diff)
    }

    override protected def queryBuilder = {
      case (queuedStates, demand) =>
        Future.successful {
          val (newStates, hits) = handleStates(queuedStates, demand * 4)
          if (newStates.isEmpty) {
            Result(scroll = None, hits = hits)
          } else {
            Result(scroll = Some(newStates), hits = hits)
          }
        }
    }

    @tailrec
    private final def handleStates(states: Vector[State], requestCount: Long, hits: Vector[Document] = Vector()): (Vector[State], Vector[Document]) = {
      if (requestCount == 0) {
        (states, hits)
      } else {
        states match {
          case (state +: queuedStates) =>
            val (nextStates, hit) =
              try {
                state match {
                  case documentIteration@DocumentIteration(task, iterator) =>
                    if (iterator.hasNext) {
                      (Vector(documentIteration), Some(iterator.next()))
                    } else {
                      supervisor ! task.copy(status = Done(startDate = task.status.startDate))
                      (Vector.empty, None)
                    }
                  case (convertibleFile: ConvertibleFile) =>
                    val task = ServiceAccountSourceTask[Working](source = convertibleFile.serviceAccountSource, convertibleFile.path.toString, status = Working())
                    supervisor ! task
                    val documentIterator = readConvertibleFile(convertibleFile)
                    (Vector(DocumentIteration(task, documentIterator)), None)
                  case PathState(serviceAccountSource, path, documentPath, mimeType) =>
                    try {
                      if (Files.isDirectory(path)) {
                        val directories = Vector.newBuilder[PathState]
                        val directoryStream = Files.newDirectoryStream(path)
                        val iterator = directoryStream.iterator().asScala.collect {
                          case pathInsideDirectory =>
                            val documentPathInsideDirectory = documentPath.resolve(pathInsideDirectory.getFileName)
                            try {
                              if (Files.isDirectory(pathInsideDirectory)) {
                                directories += PathState(serviceAccountSource, pathInsideDirectory, documentPathInsideDirectory)
                                None
                              } else {
                                retrieveFile(serviceAccountSource, pathInsideDirectory, None, documentPathInsideDirectory)
                              }
                            } catch {
                              case e: SecurityException =>
                                logger.error(s"Error checking if path is directory $pathInsideDirectory", e)
                                None
                            }
                        }.flatten
                        (Vector(DirectoryIteration(directoryStream, iterator, directories)), None)
                      } else {
                        (retrieveFile(serviceAccountSource, path, mimeType, documentPath).toVector, None)
                      }
                    } catch {
                      case e@(_: IOException | _: SecurityException) =>
                        logger.error(s"Error opening $path for document $documentPath", e)
                        (Vector.empty, None)
                    }
                  case (directoryIteration: DirectoryIteration) =>
                    if (directoryIteration.iterator.hasNext) {
                      (Vector(directoryIteration.iterator.next(), directoryIteration), None)
                    } else {
                      try {
                        directoryIteration.directoryStream.close()
                      } catch {
                        case e: IOException =>
                          logger.error("Error closing DirectoryStream", e)
                      }
                      (directoryIteration.directories.result, None)
                    }
                  case zipIteration: ZipIteration =>
                    if (zipIteration.iterator.hasNext) {
                      (Vector(zipIteration.iterator.next(), zipIteration), None)
                    } else {
                      try {
                        zipIteration.zipFile.close()
                      } catch {
                        case e: IOException =>
                          logger.error("Error closing ZipFile", e)
                      }
                      (Vector.empty, None)
                    }
                  case ZipFileState(serviceAccountSource, zipfile: ZipFile, documentPath: Path) =>
                    (Vector(ZipIteration(zipfile, zipfile.entries().asScala.collect {
                      case entry if !entry.isDirectory =>
                        retrieveFile(serviceAccountSource, documentPath.resolve(entry.getName), () => zipfile.getInputStream(entry))
                    }.flatten)), None)
                }
              } catch {
                case e: Exception =>
                  // This can happen if a DocumentIteration, ZipIteration or DirectoryIteration fails
                  logger.error(s"Cannot handle state $state. Skipping.")
                  (Vector.empty, None)
              }
            hit match {
              case Some(h) =>
                handleStates(nextStates ++ queuedStates, requestCount - 1, hits :+ h)
              case None =>
                handleStates(nextStates ++ queuedStates, requestCount, hits)
            }
          case _ =>
            // empty states
            (states, hits)
        }
      }
    }

    private def retrieveFile(serviceAccountSource: ServiceAccountSource, path: Path, mimeTypeOption: Option[String], documentPath: Path): Option[State] = {
      retrieveFile(serviceAccountSource, path, mimeTypeOption.getOrElse(mimeTypeFromPath(documentPath)), documentPath) match {
        case Some(state) => Some(state)
        case None =>
          mimeTypeOption match {
            case Some(mimeType) if mimeType != mimeTypeFromPath(documentPath) =>
              retrieveFile(serviceAccountSource, path, mimeTypeFromPath(documentPath), documentPath)
            case _ => None
          }
      }
    }

    private def retrieveFile(serviceAccountSource: ServiceAccountSource, path: Path, mimeType: String, documentPath: Path): Option[State] = {
      try {
        if (Files.exists(path)) {
          mimeType match {
            case "application/zip" | "application/x-zip-compressed" =>
              Some(ZipFileState(serviceAccountSource, new ZipFile(path.toFile), documentPath))
            case _ =>
              retrieveFile(serviceAccountSource, documentPath, mimeType, () => Files.newInputStream(path))
          }
        } else {
          logger.warn(s"File does not exist: $path")
          None
        }
      } catch {
        case e@(_: IOException | _: SecurityException) =>
          logger.error(s"Error retrieving file at $path for document $documentPath", e)
          None
      }
    }

    private def retrieveFile(serviceAccountSource: ServiceAccountSource, path: Path, streamClosure: () => InputStream): Option[ConvertibleFile] = {
      retrieveFile(serviceAccountSource, path, mimeTypeFromPath(path), streamClosure)
    }

    private def retrieveFile(serviceAccountSource: ServiceAccountSource, path: Path, mimeType: String, streamClosure: () => InputStream): Option[ConvertibleFile] = {
      converters.get(mimeType).map {
        converter => ConvertibleFile(serviceAccountSource, path, converter, streamClosure())
      }.orElse {
        logger.warn(s"Unsupported MIME type $mimeType for $path")
        None
      }
    }

    private def readConvertibleFile(convertibleFile: ConvertibleFile): Iterator[Document] = {
      val baseUri = convertibleFile.path.toUri.toString
      def context: Option[String] => IRI = {
        case Some(part) =>
          // We assume that path Uris do not have a query part nor a fragment
          // TODO: Is this right ?
          valueFactory.createIRI(s"$baseUri?part=${URLEncoder.encode(part, "UTF-8")}")
        case None => valueFactory.createIRI(baseUri)
      }

      def createSourceContext(statements: StatementSet, id: String): IRI = {
        val iri = valueFactory.createIRI(s"${convertibleFile.serviceAccountSource.iri.toString}?id=${URLEncoder.encode(id, "UTF-8")}")
        statements.add(iri, Personal.DOCUMENT_OF, convertibleFile.serviceAccountSource.iri, iri)
        iri
      }
      try {
        val documentIterator = convertibleFile.converter.convert(convertibleFile.inputStream, context, createSourceContext).map {
          case (documentIri, statements) =>
            statements.add(documentIri, Personal.DOCUMENT_OF, convertibleFile.serviceAccountSource.iri, documentIri)
            Document(documentIri, statements)
        }
        new Iterator[Document] {
          override def hasNext: Boolean = {
            if (documentIterator.hasNext) {
              true
            } else {
              try {
                convertibleFile.inputStream.close()
              } catch {
                case e: IOException =>
                  logger.error(s"Error closing file ${convertibleFile.path}", e)
              }
              false
            }
          }

          override def next(): Document = {
            documentIterator.next()
          }
        }
      } catch {
        case e: Exception =>
          logger.error(s"Error converting file ${convertibleFile.path} with converter ${convertibleFile.converter}", e)
          Iterator.empty
      } finally {
        try {
          convertibleFile.inputStream.close()
        } catch {
          case e: IOException =>
            logger.error(s"Error closing file ${convertibleFile.path}", e)
        }
      }
    }

    private def mimeTypeFromPath(path: Path): String = {
      registeredExtensions(FilenameUtils.getExtension(path.toString))
    }

    private def applyDiff(diff: ModelDiff): UpdateResults = {
      //We tag file:// contexts as failed
      UpdateResults.merge(
        diff.contexts()
          .filter(_.stringValue().startsWith("file://"))
          .map(context => UpdateResults.allFailed(
            diff.filter(_.getContext == context),
            new ConverterException("Files could not be modified")
          ))
      )
    }
  }
}
