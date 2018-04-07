package com.thymeflow.enricher

import com.thymeflow.rdf.model.StatementSetDiff
import com.thymeflow.rdf.model.vocabulary.{Negation, Personal}
import com.thymeflow.rdf.repository.Repository
import org.eclipse.rdf4j.model.{Resource, Statement}
import org.eclipse.rdf4j.query.QueryLanguage
import org.eclipse.rdf4j.repository.RepositoryConnection

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
abstract class AbstractEnricher(override val newRepositoryConnection: () => RepositoryConnection) extends Enricher {

  private val isDifferentFromQuery = repositoryConnection.prepareBooleanQuery(QueryLanguage.SPARQL,
    s"""ASK {
      ?facet1 <${Personal.SAME_AS}>*/<${Personal.DIFFERENT_FROM}>/<${Personal.SAME_AS}>* ?facet2
    }"""
  )

  /**
    * Add an inferred statements to the repository if it is not already existing
    */
  protected def addStatements(repositoryConnection: RepositoryConnection)(diff: StatementSetDiff, statements: Iterable[Statement]): Unit = {
    val newStatements = statements
      .filter(statement =>
        !Repository.hasStatementWithContext(statement, includeInferred = false)(repositoryConnection) &&
          !repositoryConnection.hasStatement(statement.getSubject, Negation.not(statement.getPredicate), statement.getObject, true)
      )
    diff.add(newStatements)
    repositoryConnection.add(newStatements.asJavaCollection)
  }

  /**
    * Add an inferred statement to the repository if it is not already existing
    */
  protected def addStatement(repositoryConnection: RepositoryConnection)(diff: StatementSetDiff, statement: Statement): Unit = {
    addStatements(repositoryConnection)(diff, Option(statement))
  }

  /**
    * Remove inferred statements from the repository if it is existing
    */
  protected def removeStatements(repositoryConnection: RepositoryConnection)(diff: StatementSetDiff, statements: Iterable[Statement]): Unit = {
    val existingStatements = statements
      .filter(statement => Repository.hasStatementWithContext(statement, includeInferred = false)(repositoryConnection))
    diff.remove(existingStatements)
    repositoryConnection.remove(existingStatements.asJavaCollection)
  }

  /**
    * Remove an inferred statement from the repository if it is existing
    */
  protected def removeStatement(repositoryConnection: RepositoryConnection)(diff: StatementSetDiff, statement: Statement): Unit = {
    removeStatements(repositoryConnection)(diff, Option(statement))
  }

  protected def addStatements(diff: StatementSetDiff, statements: Iterable[Statement]): Unit = {
    addStatements(repositoryConnection)(diff, statements)
  }

  protected def addStatement(diff: StatementSetDiff, statement: Statement): Unit = {
    addStatement(repositoryConnection)(diff, statement)
  }

  protected def removeStatements(diff: StatementSetDiff, statements: Iterable[Statement]): Unit = {
    removeStatements(repositoryConnection)(diff, statements)
  }

  protected def removeStatement(diff: StatementSetDiff, statement: Statement): Unit = {
    removeStatement(repositoryConnection)(diff, statement)
  }

  protected def isDifferentFrom(facet1: Resource, facet2: Resource): Boolean = {
    isDifferentFromQuery.setBinding("facet1", facet1)
    isDifferentFromQuery.setBinding("facet2", facet2)
    isDifferentFromQuery.evaluate()
  }
}
