package thymeflow.sync.converter

import java.io.InputStream

import org.openrdf.model.{Model, Resource}

/**
  * @author Thomas Pellissier Tanon
  */
trait Converter {

  def convert(str: String, context: Resource): Model

  def convert(stream: InputStream, context: Resource): Model
}
