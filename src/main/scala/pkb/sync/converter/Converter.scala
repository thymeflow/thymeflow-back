package pkb.sync.converter

import java.io.InputStream

import org.openrdf.model.Model

/**
  * @author Thomas Pellissier Tanon
  */
trait Converter {

  def convert(str: String): Model

  def convert(stream: InputStream): Model
}
