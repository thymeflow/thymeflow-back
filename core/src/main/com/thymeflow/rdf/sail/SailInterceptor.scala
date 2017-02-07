package com.thymeflow.rdf.sail

import org.eclipse.rdf4j.model.{IRI, Resource, Value, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
trait SailInterceptor {
  /**
    * Method called when an insertion is tried on the sail
    *
    * @return boolean if the insertion should be done
    */
  def onSparqlAddStatement(subject: Resource, predicate: IRI, `object`: Value, contexts: Resource*)(implicit valueFactory: ValueFactory): Boolean = true

  /**
    * Method called when an deletion is tried on the sail
    *
    * @return boolean if the deletion should be done
    */
  def onSparqlRemoveStatement(subject: Resource, predicate: IRI, `object`: Value, contexts: Resource*)(implicit valueFactory: ValueFactory): Boolean = true
}