package com.thymeflow.rdf.repository

import org.openrdf.model.ValueFactory
import org.openrdf.repository.RepositoryConnection

/**
  * @author David Montoya
  */
trait Repository {
  def newConnection(): RepositoryConnection

  def valueFactory: ValueFactory
}
