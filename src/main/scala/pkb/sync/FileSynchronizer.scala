package pkb.sync

import java.io.{File, FileInputStream, InputStream}
import java.util.zip.ZipFile

import akka.actor.Props
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
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

  case class Config(files: Traversable[String]) {
  }

  private class Publisher(valueFactory: ValueFactory) extends ActorPublisher[Document] with StrictLogging {

    private val emailMessageConverter = new EmailMessageConverter(valueFactory)
    private val iCalConverter = new ICalConverter(valueFactory)
    private val vCardConverter = new VCardConverter(valueFactory)
    private val queue = new mutable.Queue[ConvertibleFile]

    override def receive: Receive = {
      case Request =>
        deliverWaitingMessages()
      case config: Config =>
        retrieveFiles(config.files.map(new File(_)))
    }

    private def retrieveFiles(files: Traversable[File]): Unit = {
      files.foreach(file =>
        if (file.isDirectory) {
          retrieveFiles(file.listFiles())
        } else {
          retrieveFile(file)
        }
      )
    }

    private def retrieveFile(file: File): Unit = {
      FilenameUtils.getExtension(file.toString) match {
        case "eml" => addFile(ConvertibleFile(file, emailMessageConverter))
        case "ics" => addFile(ConvertibleFile(file, iCalConverter))
        case "vcf" => addFile(ConvertibleFile(file, vCardConverter))
        case "zip" => retrieveFile(new ZipFile(file))
        case extension =>
          logger.info("Unsupported file extension " + extension + " for file " + file)
      }
    }

    private def retrieveFile(file: ZipFile): Unit = {
      file.entries().asScala.foreach(entry =>
        if (!entry.isDirectory) {
          retrieveFile(entry.getName, file.getInputStream(entry))
        }
      )
    }

    private def retrieveFile(fileName: String, stream: InputStream): Unit = {
      FilenameUtils.getExtension(fileName) match {
        case "eml" => addFile(ConvertibleFile(new File(fileName), emailMessageConverter, Some(stream)))
        case "ics" => addFile(ConvertibleFile(new File(fileName), iCalConverter, Some(stream)))
        case "vcf" => addFile(ConvertibleFile(new File(fileName), vCardConverter, Some(stream)))
        case extension =>
          logger.info("Unsupported file extension " + extension + " for file " + fileName)
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

    private case class ConvertibleFile(path: File, converter: Converter, inputStream: Option[InputStream] = None) {
      def read(): Document = {
        val stream = inputStream.getOrElse(new FileInputStream(path))
        val model = converter.convert(stream)
        stream.close()
        new Document(valueFactory.createIRI(path.toURI.toString), model)
      }
    }
  }
}
