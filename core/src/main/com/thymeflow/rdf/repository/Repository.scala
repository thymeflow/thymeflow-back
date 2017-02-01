package com.thymeflow.rdf.repository

import org.eclipse.rdf4j.model.ValueFactory
import org.eclipse.rdf4j.repository.RepositoryConnection

/**
  * @author David Montoya
  */
trait Repository {
  def newConnection(): RepositoryConnection

  def valueFactory: ValueFactory
}
