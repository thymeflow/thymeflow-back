package pkb.sync

import java.io.{File, FileInputStream, InputStream}
import java.util.zip.ZipFile

import akka.actor.Props
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FilenameUtils
import org.openrdf.model.ValueFactory
import pkb.rdf.model.document.Document
import pkb.sync.converter.{Converter, EmailMessageConverter, ICalConverter, VCardConverter}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * @author Thomas Pellissier Tanon
  */
object FileSynchronizer {

  def source(valueFactory: ValueFactory) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory)))

  case class Config(file: File, mimeType: Option[String] = None) {
  }

  private class Publisher(valueFactory: ValueFactory) extends ActorPublisher[Document] with StrictLogging {

    private val emailMessageConverter = new EmailMessageConverter(valueFactory)
    private val iCalConverter = new ICalConverter(valueFactory)
    private val vCardConverter = new VCardConverter(valueFactory)
    private val queue = new mutable.Queue[ConvertibleFile]

    override def receive: Receive = {
      case Request(_) =>
        deliverWaitingMessages()
      case config: Config =>
        config.mimeType match {
          case Some(mimeType) => retrieveFile(config.file, mimeType)
          case None => retrieveFiles(List(config.file))
        }
      case Cancel =>
        context.stop(self)
    }

    private def retrieveFiles(files: Traversable[File]): Unit = {
      files.foreach(file =>
        if (file.isDirectory) {
          retrieveFiles(file.listFiles())
        } else {
          retrieveFile(file, mimeTypeFromFile(file))
        }
      )
    }

    private def retrieveFile(file: File, mimeType: String): Unit = {
      mimeType match {
        case "application/zip" => retrieveFile(new ZipFile(file))
        case "message/rfc822" => addFile(ConvertibleFile(file, emailMessageConverter))
        case "text/calendar" => addFile(ConvertibleFile(file, iCalConverter))
        case "text/vcard" => addFile(ConvertibleFile(file, vCardConverter))
        case _ => logger.info("Unsupported MIME type " + mimeType + " for file " + file)
      }
    }

    private def retrieveFile(file: ZipFile): Unit = {
      file.entries().asScala.foreach(entry =>
        if (!entry.isDirectory) {
          retrieveFile(new File(entry.getName), file.getInputStream(entry))
        }
      )
    }

    private def retrieveFile(file: File, stream: InputStream): Unit = {
      mimeTypeFromFile(file) match {
        case "message/rfc822" => addFile(ConvertibleFile(file, emailMessageConverter, Some(stream)))
        case "text/calendar" => addFile(ConvertibleFile(file, iCalConverter, Some(stream)))
        case "text/vcard" => addFile(ConvertibleFile(file, vCardConverter, Some(stream)))
        case mimeType => logger.info("Unsupported MIME type " + mimeType + " for file " + file)
      }
    }

    private def addFile(file: ConvertibleFile): Unit = {
      if (waitingForData) {
        onNext(file)
      } else {
        queue.enqueue(file)
      }
    }

    private def onNext(file: ConvertibleFile): Unit = {
      onNext(file.read())
    }

    private def deliverWaitingMessages(): Unit = {
      while (waitingForData && queue.nonEmpty) {
        onNext(queue.dequeue())
      }
    }

    private def waitingForData: Boolean = {
      isActive && totalDemand > 0
    }

    private def mimeTypeFromFile(file: File): String = {
      FilenameUtils.getExtension(file.toString) match {
        case "eml" => "message/rfc822"
        case "ics" => "text/calendar"
        case "vcf" => "text/vcard"
        case "zip" => "application/zip"
        case _ => "application/octet-stream"
      }
    }

    private case class ConvertibleFile(path: File, converter: Converter, inputStream: Option[InputStream] = None) {
      def read(): Document = {
        val stream = inputStream.getOrElse(new FileInputStream(path))
        val documentIri = valueFactory.createIRI(path.toURI.toString)
        val model = converter.convert(stream, documentIri)
        stream.close()
        Document(documentIri, model)
      }
    }
  }
}
