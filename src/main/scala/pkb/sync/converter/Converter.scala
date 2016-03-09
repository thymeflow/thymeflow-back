package pkb.sync.converter

import java.io.{File, FileInputStream, InputStream}

import org.openrdf.model.Model

/**
  * @author Thomas Pellissier Tanon
  */
trait Converter {

  def convert(file: File): Model = {
    val stream = new FileInputStream(file)
    val model = convert(stream)
    stream.close()
    model
  }

  def convert(str: String): Model

  def convert(stream: InputStream): Model
}
