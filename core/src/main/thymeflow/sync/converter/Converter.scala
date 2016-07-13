package thymeflow.sync.converter

import java.io.InputStream

import org.openrdf.model.{Model, Resource}
import thymeflow.rdf.model.ModelDiff
import thymeflow.update.UpdateResults

/**
  * @author Thomas Pellissier Tanon
  */
trait Converter {

  def convert(str: String, context: Resource): Model

  def convert(stream: InputStream, context: Resource): Model

  def applyDiff(str: String, diff: ModelDiff): (String, UpdateResults) = {
    (str, UpdateResults.allFailed(diff,
      new ConverterException(s"Content converted by ${this.getClass.getSimpleName} could not be converted")
    ))
  }
}
