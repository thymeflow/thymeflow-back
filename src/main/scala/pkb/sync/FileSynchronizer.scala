package pkb.sync

import java.io.{File, IOException}

import org.apache.commons.io.FilenameUtils
import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.{Model, ValueFactory}
import pkb.sync.converter.ICalConverter

/**
  * @author Thomas Pellissier Tanon
  */
class FileSynchronizer(valueFactory: ValueFactory) {

  private val iCalConverter = new ICalConverter(valueFactory)

  def synchronize(files: Array[String]): Model = {
    synchronize(files.map(file => new File(file)))
  }

  def synchronize(files: Array[File]): Model = {
    val model = new LinkedHashModel
    for (file <- files) {
      FilenameUtils.getExtension(file.toString) match {
        case "ics" => model.addAll(iCalConverter.convert(file))
        case extension => throw new IOException("Unsupported file extension: " + extension)
      }
    }
    model
  }
}
