package com.thymeflow.sync.converter

import java.io.InputStream

import com.thymeflow.rdf.model.ModelDiff
import com.thymeflow.update.UpdateResults
import org.eclipse.rdf4j.model.{IRI, Model}

/**
  * @author Thomas Pellissier Tanon
  */
trait Converter {

  def convert(stream: InputStream, context: Option[String] => IRI, createSourceContext: (Model, String) => IRI): Iterator[(IRI, Model)]

  def applyDiff(str: String, diff: ModelDiff): (String, UpdateResults) = {
    (str, UpdateResults.allFailed(diff,
      new ConverterException(s"Content converted by ${this.getClass.getSimpleName} could not be converted")
    ))
  }
}
