package pkb.sync

import java.io.File

import org.apache.commons.io.FilenameUtils
import org.openrdf.model.{IRI, ValueFactory}
import org.slf4j.LoggerFactory
import pkb.rdf.model.document.Document
import pkb.sync.converter.{EmailMessageConverter, ICalConverter, VCardConverter}

/**
  * @author Thomas Pellissier Tanon
  */
class FileSynchronizer(valueFactory: ValueFactory, files: Array[String]) extends Synchronizer {

  private val logger = LoggerFactory.getLogger(classOf[FileSynchronizer])
  private val emailMessageConverter = new EmailMessageConverter(valueFactory)
  private val iCalConverter = new ICalConverter(valueFactory)
  private val vCardConverter = new VCardConverter(valueFactory)

  def synchronize(): Traversable[Document] = {
    retrieve(files.map(file => new File(file)))
  }

  private def retrieve(files: Array[File]): Traversable[Document] = {
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
      case "eml" => Some(new Document(iriForFile(file), emailMessageConverter.convert(file)))
      case "ics" => Some(new Document(iriForFile(file), iCalConverter.convert(file)))
      case "vcf" => Some(new Document(iriForFile(file), vCardConverter.convert(file)))
      case extension =>
        logger.info("Unsupported file extension " + extension + " for file " + file)
        None
    }
  }

  private def iriForFile(file: File): IRI = {
    valueFactory.createIRI(file.toURI.toString)
  }
}
