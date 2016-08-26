package com.thymeflow.rdf.repository

import org.openrdf.IsolationLevel
import org.openrdf.model.ValueFactory
import org.openrdf.repository.RepositoryConnection

/**
  * Encapsulates a Repository by providing new repository connection with a default isolation level
  *
  * @author David Montoya
  */
case class RepositoryWithDefaultIsolationLevel(repository: org.openrdf.repository.Repository,
                                               defaultIsolationLevel: IsolationLevel)
  extends Repository {

  def newConnection: RepositoryConnection = {
    val connection = repository.getConnection
    connection.setIsolationLevel(defaultIsolationLevel)
    connection
  }

  def valueFactory: ValueFactory = repository.getValueFactory
}
