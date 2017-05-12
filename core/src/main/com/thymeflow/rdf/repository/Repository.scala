package com.thymeflow.rdf.repository

import org.eclipse.rdf4j.model.{Statement, ValueFactory}
import org.eclipse.rdf4j.repository.RepositoryConnection

/**
  * @author David Montoya
  */
trait Repository {
  def newConnection(): RepositoryConnection

  def valueFactory: ValueFactory

  def shutdown(): Unit
}

object Repository{
  def hasStatementWithContext(statement: Statement, includeInferred: Boolean)(implicit repositoryConnection: RepositoryConnection): Boolean = {
    if(statement.getContext == null) throw new IllegalArgumentException("Provided statement must have a context")
    repositoryConnection.hasStatement(statement, includeInferred, statement.getContext)
  }
}