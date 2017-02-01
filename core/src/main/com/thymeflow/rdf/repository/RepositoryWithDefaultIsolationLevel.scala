package com.thymeflow.rdf.repository

import org.eclipse.rdf4j.IsolationLevel
import org.eclipse.rdf4j.model.ValueFactory
import org.eclipse.rdf4j.repository.RepositoryConnection

/**
  * Encapsulates a Repository by providing new repository connection with a default isolation level
  *
  * @author David Montoya
  */
case class RepositoryWithDefaultIsolationLevel(repository: org.eclipse.rdf4j.repository.Repository,
                                               defaultIsolationLevel: IsolationLevel)
  extends Repository {

  def newConnection: RepositoryConnection = {
    val connection = repository.getConnection
    connection.setIsolationLevel(defaultIsolationLevel)
    connection
  }

  def valueFactory: ValueFactory = repository.getValueFactory
}
