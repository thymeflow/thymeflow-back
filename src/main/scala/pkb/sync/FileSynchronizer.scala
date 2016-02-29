package pkb.sync

import java.io.File

import org.apache.commons.io.FilenameUtils
import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.{Model, ValueFactory}
import org.slf4j.LoggerFactory
import pkb.sync.converter.{EmailMessageConverter, ICalConverter, VCardConverter}

/**
  * @author Thomas Pellissier Tanon
  */
class FileSynchronizer(valueFactory: ValueFactory) {

  private val logger = LoggerFactory.getLogger(classOf[FileSynchronizer])
  private val emailMessageConverter = new EmailMessageConverter(valueFactory)
  private val iCalConverter = new ICalConverter(valueFactory)
  private val vCardConverter = new VCardConverter(valueFactory)

  def synchronize(files: Array[String]): Model = {
    synchronize(files.map(file => new File(file)))
  }

  def synchronize(files: Array[File]): Model = {
    val model = new LinkedHashModel
    synchronize(files, model)
    model
  }

  private def synchronize(files: Array[File], model: Model): Unit = {
    files.foreach(file =>
      if (file.isDirectory) {
        synchronize(file.listFiles(), model)
      } else {
        synchronize(file, model)
      }
    )
  }

  private def synchronize(file: File, model: Model): Unit = {
    FilenameUtils.getExtension(file.toString) match {
      case "eml" => model.addAll(emailMessageConverter.convert(file))
      case "ics" => model.addAll(iCalConverter.convert(file))
      case "vcf" => model.addAll(vCardConverter.convert(file))
      case extension => logger.info("Unsupported file extension " + extension + " for file " + file)
    }
  }
}
