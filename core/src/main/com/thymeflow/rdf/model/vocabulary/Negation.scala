package com.thymeflow.rdf.model.vocabulary

import java.net.URLEncoder

import org.openrdf.model.IRI
import org.openrdf.model.impl.SimpleValueFactory

/**
  * @author Thomas Pellissier Tanon
  */
object Negation {

  private val negationBaseNamespace = "http://thymeflow.com/negation#"
  private val valueFactory = SimpleValueFactory.getInstance()

  def not(property: IRI): IRI = {
    property match {
      case Personal.SAME_AS => Personal.DIFFERENT_FROM
      case Personal.DIFFERENT_FROM => Personal.SAME_AS
      case _ =>
        valueFactory.createIRI(negationBaseNamespace, URLEncoder.encode(property.stringValue(), "UTF-8"))
    }
  }
}
