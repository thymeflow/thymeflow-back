package thymeflow.sync.converter

import java.io.InputStream

import org.openrdf.model.{IRI, Model}

/**
  * @author Thomas Pellissier Tanon
  */
trait Converter {

  def convert(str: String, context: IRI): Model

  def convert(stream: InputStream, context: IRI): Model
}
