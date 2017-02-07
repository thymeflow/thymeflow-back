package com.thymeflow.sync.converter

import java.io.InputStream

import com.thymeflow.rdf.model.{StatementSet, StatementSetDiff}
import com.thymeflow.update.UpdateResults
import org.eclipse.rdf4j.model.IRI

/**
  * @author Thomas Pellissier Tanon
  */
trait Converter {

  def convert(stream: InputStream, context: Option[String] => IRI, createSourceContext: (StatementSet, String) => IRI): Iterator[(IRI, StatementSet)]

  def applyDiff(str: String, diff: StatementSetDiff): (String, UpdateResults) = {
    (str, UpdateResults.allFailed(diff,
      new ConverterException(s"Content converted by ${this.getClass.getSimpleName} could not be converted")
    ))
  }
}
