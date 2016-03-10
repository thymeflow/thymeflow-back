package pkb.sync

import java.io.{File, InputStream}
import java.util.zip.ZipFile

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FilenameUtils
import org.openrdf.model.{IRI, ValueFactory}
import pkb.rdf.model.document.Document
import pkb.sync.converter.{Converter, EmailMessageConverter, ICalConverter, VCardConverter}

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
class FileSynchronizer(valueFactory: ValueFactory, files: Array[String]) extends Synchronizer with StrictLogging {

  private val emailMessageConverter = new EmailMessageConverter(valueFactory)
  private val iCalConverter = new ICalConverter(valueFactory)
  private val vCardConverter = new VCardConverter(valueFactory)

  def synchronize(): Traversable[Document] = {
    retrieve(files.map(file => new File(file)))
  }

  private def retrieve(files: Traversable[File]): Traversable[Document] = {
    files.flatMap(file =>
      if (file.isDirectory) {
        retrieve(file.listFiles())
      } else {
        retrieve(file)
      }
    )
  }

  private def retrieve(file: File): Traversable[Document] = {
    FilenameUtils.getExtension(file.toString) match {
      case "eml" => Some(convert(file, emailMessageConverter))
      case "ics" => Some(convert(file, iCalConverter))
      case "vcf" => Some(convert(file, vCardConverter))
      case "zip" => retrieve(new ZipFile(file))
      case extension =>
        logger.info("Unsupported file extension " + extension + " for file " + file)
        None
    }
  }

  private def retrieve(file: ZipFile): Traversable[Document] = {
    file.entries().asScala.flatMap(entry =>
      if (entry.isDirectory) {
        None
      } else {
        retrieve(entry.getName, file.getInputStream(entry))
      }
    ).toTraversable
  }

  private def retrieve(fileName: String, stream: InputStream): Traversable[Document] = {
    FilenameUtils.getExtension(fileName) match {
      case "eml" => Some(convert(fileName, stream, emailMessageConverter))
      case "ics" => Some(convert(fileName, stream, iCalConverter))
      case "vcf" => Some(convert(fileName, stream, vCardConverter))
      case extension =>
        logger.info("Unsupported file extension " + extension + " for file " + fileName)
        None
    }
  }

  private def convert(file: File, converter: Converter): Document = {
    new Document(iriForFile(file), converter.convert(file))
  }

  private def convert(fileName: String, stream: InputStream, converter: Converter): Document = {
    val model = converter.convert(stream)
    stream.close()
    new Document(iriForFile(fileName), model)
  }

  private def iriForFile(file: File): IRI = {
    valueFactory.createIRI(file.toURI.toString)
  }

  private def iriForFile(fileName: String): IRI = {
    iriForFile(new File(fileName))
  }
}
