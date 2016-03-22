package pkb.sync

import java.io.{File, InputStream}
import java.nio.file.{DirectoryStream, Files, Path, Paths}
import java.util.zip.ZipFile

import akka.actor.Props
import akka.stream.scaladsl.Source
import org.apache.commons.io.FilenameUtils
import org.openrdf.model.ValueFactory
import pkb.rdf.model.document.Document
import pkb.sync.converter.{Converter, EmailMessageConverter, ICalConverter, VCardConverter}
import pkb.sync.publisher.ScrollDocumentPublisher

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * @author Thomas Pellissier Tanon
  */
object FileSynchronizer {

  def source(valueFactory: ValueFactory)(implicit executionContext: ExecutionContext) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory)))

  case class Config(file: File, mimeType: Option[String] = None) {
  }

  private class Publisher(valueFactory: ValueFactory)(implicit val executionContext: ExecutionContext)
    extends ScrollDocumentPublisher[Document, (Vector[Any]), Traversable[Document]] {

    private val emailMessageConverter = new EmailMessageConverter(valueFactory)
    private val iCalConverter = new ICalConverter(valueFactory)
    private val vCardConverter = new VCardConverter(valueFactory)

    override def receive: Receive = {
      case config: Config =>
        currentScrollOption = Some(currentScrollOption match {
          case Some(queuedConfigs) =>
            queuedConfigs :+ config
          case None =>
            Vector(config)
        })
        nextResults()
      case message =>
        super.receive(message)
    }

    override protected def queryBuilder = {
      case (state +: queuedStates) =>
        Future {
          val (nextStates, hits) = state match {
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
            case (convertibleFile: ConvertibleFile) =>
              (Vector.empty, Some(convertibleFile.read()))
            case (directoryIteration: DirectoryIteration) =>
              if (directoryIteration.iterator.hasNext) {
                (Vector(directoryIteration.iterator.next(), directoryIteration), None)
              } else {
                directoryIteration.directoryStream.close()
                (directoryIteration.directories.result, None)
              }
            case (zipfile: ZipFile) =>
              (Vector(ZipIteration(zipfile, zipfile.entries().asScala.collect {
                case entry if !entry.isDirectory =>
                  retrieveFile(Paths.get(entry.getName), zipfile.getInputStream(entry))
              }.flatten)), None)
            case zipIteration: ZipIteration =>
              if (zipIteration.iterator.hasNext) {
                (Vector(zipIteration.iterator.next(), zipIteration), None)
              } else {
                zipIteration.zipFile.close()
                (Vector.empty, None)
              }
            case config: Config =>
              (config.mimeType match {
                case Some(mimeType) =>
                  retrieveFile(config.file.toPath, mimeType).toVector
                case None =>
                  Vector(config.file.toPath)
              }, None)
          }
          Result(scroll = Some(nextStates ++ queuedStates), hits = hits)
        }
      case Vector() =>
        Future.successful(Result(scroll = None, hits = None))
    }

    private def retrieveFile(path: Path): Option[Any] = {
      retrieveFile(path, mimeTypeFromFile(path))
    }

    private def retrieveFile(path: Path, mimeType: String): Option[Any] = {
      mimeType match {
        case "application/zip" => Some(new ZipFile(path.toFile))
        case "message/rfc822" | "text/calendar" | "text/vcard" =>
          retrieveFile(path, Files.newInputStream(path))
        case _ =>
          logger.info("Unsupported MIME type " + mimeType + " for file " + path)
          None
      }
    }

    private def retrieveFile(path: Path, stream: InputStream): Option[ConvertibleFile] = {
      retrieveFile(path, mimeTypeFromFile(path), stream)
    }

    private def retrieveFile(path: Path, mimeType: String, stream: InputStream): Option[ConvertibleFile] = {
      mimeType match {
        case "message/rfc822" => Some(ConvertibleFile(path, emailMessageConverter, stream))
        case "text/calendar" => Some(ConvertibleFile(path, iCalConverter, stream))
        case "text/vcard" => Some(ConvertibleFile(path, vCardConverter, stream))
        case mimeType =>
          logger.info("Unsupported MIME type " + mimeType + " for file " + path)
          None
      }
    }

    private def mimeTypeFromFile(path: Path): String = {
      FilenameUtils.getExtension(path.toString) match {
        case "eml" => "message/rfc822"
        case "ics" => "text/calendar"
        case "vcf" => "text/vcard"
        case "zip" => "application/zip"
        case _ => "application/octet-stream"
      }
    }

    private case class ZipIteration(zipFile: ZipFile, iterator: Iterator[ConvertibleFile])

    private case class DirectoryIteration(directoryStream: DirectoryStream[Path], iterator: Iterator[Any], directories: scala.collection.mutable.Builder[Path, Vector[Path]])

    private case class ConvertibleFile(path: Path, converter: Converter, inputStream: InputStream) {
      def read(): Document = {
        val documentIri = valueFactory.createIRI(path.toUri.toString)
        val model = converter.convert(inputStream, documentIri)
        inputStream.close()
        Document(documentIri, model)
      }
    }
  }
}
