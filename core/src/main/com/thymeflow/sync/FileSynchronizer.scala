package com.thymeflow.sync

import java.io.{File, InputStream}
import java.net.URLEncoder
import java.nio.file.{DirectoryStream, Files, Path, Paths}
import java.util.zip.ZipFile

import akka.actor.Props
import akka.stream.scaladsl.Source
import com.thymeflow.rdf.model.ModelDiff
import com.thymeflow.rdf.model.document.Document
import com.thymeflow.sync.Synchronizer.Update
import com.thymeflow.sync.converter._
import com.thymeflow.sync.publisher.ScrollDocumentPublisher
import com.thymeflow.update.UpdateResults
import org.apache.commons.io.FilenameUtils
import org.openrdf.model.{IRI, ValueFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.Future

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
object FileSynchronizer extends Synchronizer {

  private var registeredConverters: Map[String, ValueFactory => Converter] = Map(
    "message/rfc822" -> (new EmailMessageConverter(_)),
    "text/calendar" -> (new ICalConverter(_)),
    "text/vcard" -> (new VCardConverter(_))
  )
  private var registeredExtensions: Map[String, String] = Map(
    "eml" -> "message/rfc822",
    "ics" -> "text/calendar",
    "vcf" -> "text/vcard",
    "zip" -> "application/zip"
  ).withDefaultValue("application/octet-stream")

  def source(valueFactory: ValueFactory) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory)))

  def registerConverter(mimeType: String, converter: ValueFactory => Converter) = {
    registeredConverters += mimeType -> converter
  }

  def registerExtension(extension: String, mimeType: String) = {
    registeredExtensions += extension -> mimeType
  }

  case class Config(file: File, mimeType: Option[String] = None)

  private class Publisher(valueFactory: ValueFactory)
    extends ScrollDocumentPublisher[Document, (Vector[Any])] with BasePublisher {

    val converters = registeredConverters.map {
      case (k, converter) =>
        k -> converter(valueFactory)
    }

    override def receive: Receive = super.receive orElse {
      case config: Config => queue(Vector(config))
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
    private final def handleStates(states: Vector[Any], requestCount: Long, hits: Vector[Document] = Vector()): (Vector[Any], Vector[Document]) = {
      if (requestCount == 0) {
        (states, hits)
      } else {
        states match {
          case (state +: queuedStates) =>
            val (nextStates, hit) = state match {
              case documentIteration@DocumentIteration(iterator) =>
                if (iterator.hasNext) {
                  (Vector(documentIteration), Some(iterator.next()))
                } else {
                  (Vector.empty, None)
                }
              case (convertibleFile: ConvertibleFile) =>
                val documentIterator = convertibleFile.read()
                (Vector(DocumentIteration(documentIterator)), None)
              case path: Path =>
                if (Files.isDirectory(path)) {
                  val directories = Vector.newBuilder[Path]
                  val directoryStream = Files.newDirectoryStream(path)
                  val iterator = directoryStream.iterator().asScala.collect {
                    case pathInsideDirectory =>
                      if (Files.isDirectory(pathInsideDirectory)) {
                        directories += pathInsideDirectory
                        None
                      } else {
                        retrieveFile(pathInsideDirectory)
                      }
                  }.flatten
                  (Vector(DirectoryIteration(directoryStream, iterator, directories)), None)
                } else {
                  (retrieveFile(path).toVector, None)
                }
              case (directoryIteration: DirectoryIteration) =>
                if (directoryIteration.iterator.hasNext) {
                  (Vector(directoryIteration.iterator.next(), directoryIteration), None)
                } else {
                  directoryIteration.directoryStream.close()
                  (directoryIteration.directories.result, None)
                }
              case zipIteration: ZipIteration =>
                if (zipIteration.iterator.hasNext) {
                  (Vector(zipIteration.iterator.next(), zipIteration), None)
                } else {
                  zipIteration.zipFile.close()
                  (Vector.empty, None)
                }
              case (zipfile: ZipFile) =>
                (Vector(ZipIteration(zipfile, zipfile.entries().asScala.collect {
                  case entry if !entry.isDirectory =>
                    retrieveFile(Paths.get(zipfile.getName, entry.getName), () => zipfile.getInputStream(entry))
                }.flatten)), None)
              case config: Config =>
                (config.mimeType match {
                  case Some(mimeType) =>
                    retrieveFile(config.file.toPath, mimeType).toVector
                  case None =>
                    Vector(config.file.toPath)
                }, None)
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

    private def retrieveFile(path: Path): Option[Any] = {
      retrieveFile(path, mimeTypeFromFile(path))
    }

    private def retrieveFile(path: Path, mimeType: String): Option[Any] = {
      if (Files.exists(path)) {
        mimeType match {
          case "application/zip" | "application/x-zip-compressed" =>
            Some(new ZipFile(path.toFile))
          case _ =>
            retrieveFile(path, mimeType, () => Files.newInputStream(path))
        }
      } else {
        logger.warn(s"Path does not exist: $path")
        None
      }
    }

    private def retrieveFile(path: Path, streamClosure: () => InputStream): Option[ConvertibleFile] = {
      retrieveFile(path, mimeTypeFromFile(path), streamClosure)
    }

    private def retrieveFile(path: Path, mimeType: String, streamClosure: () => InputStream): Option[ConvertibleFile] = {
      converters.get(mimeType).map {
        case converter => ConvertibleFile(path, converter, streamClosure())
      }.orElse {
        logger.warn(s"Unsupported MIME type $mimeType for $path")
        None
      }
    }

    private def mimeTypeFromFile(path: Path): String = {
      registeredExtensions(FilenameUtils.getExtension(path.toString))
    }

    private case class ZipIteration(zipFile: ZipFile, iterator: Iterator[ConvertibleFile])

    private case class DocumentIteration(iterator: Iterator[Document])

    private case class DirectoryIteration(directoryStream: DirectoryStream[Path], iterator: Iterator[Any], directories: scala.collection.mutable.Builder[Path, Vector[Path]])

    private case class ConvertibleFile(path: Path, converter: Converter, inputStream: InputStream) {
      def read(): Iterator[Document] = {
        val baseUri = path.toUri.toString
        def context: Option[String] => IRI = {
          case Some(part) =>
            // We assume that path Uris do not have a query part nor a fragment
            // TODO: Is this right ?
            valueFactory.createIRI(s"$baseUri?part=${URLEncoder.encode(part, "UTF-8")}")
          case None => valueFactory.createIRI(baseUri)
        }
        val documentIterator = converter.convert(inputStream, context).map {
          case (documentIri, model) => Document(documentIri, model)
        }
        new Iterator[Document] {
          override def hasNext: Boolean = {
            if (documentIterator.hasNext) {
              true
            } else {
              inputStream.close()
              false
            }
          }

          override def next(): Document = {
            documentIterator.next()
          }
        }
      }
    }

    private def applyDiff(diff: ModelDiff): UpdateResults = {
      //We tag file:// contexts as failed
      UpdateResults.merge(
        diff.contexts().asScala
          .filter(_.stringValue().startsWith("file://"))
          .map(context => UpdateResults.allFailed(
            diff.filter(null, null, null, context),
            new ConverterException("Files could not be modified")
          ))
      )
    }
  }
}
