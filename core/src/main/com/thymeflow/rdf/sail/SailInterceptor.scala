package com.thymeflow.rdf.sail

import com.thymeflow.rdf.model.StatementSetDiff

/**
  * @author Thomas Pellissier Tanon
  */
trait SailInterceptor {
  /**
    * Method called when an SPARQL UPDATE is tried on the sail
    */
  def onSparqlUpdateDiff(statementSetDiff: StatementSetDiff)
}