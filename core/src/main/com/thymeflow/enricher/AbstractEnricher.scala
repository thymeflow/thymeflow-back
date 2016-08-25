package com.thymeflow.enricher

import java.util

import com.thymeflow.rdf.model.ModelDiff
import com.thymeflow.rdf.model.vocabulary.{Negation, Personal}
import org.openrdf.model.{Resource, Statement}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection

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
  protected def addStatements(diff: ModelDiff, statements: util.Collection[Statement]): Unit = {
    val newStatements = statements.asScala
      .filterNot(repositoryConnection.hasStatement(_, false))
      .filterNot(statement => repositoryConnection.hasStatement(
        statement.getSubject,
        Negation.not(statement.getPredicate),
        statement.getObject,
        true
      ))
      .asJavaCollection
    diff.add(newStatements)
    repositoryConnection.add(newStatements)
  }

  /**
    * Add an inferred statement to the repository if it is not already existing
    */
  protected def addStatement(diff: ModelDiff, statement: Statement): Unit = {
    if (
      !repositoryConnection.hasStatement(statement, false) &&
      !repositoryConnection.hasStatement(statement.getSubject, Negation.not(statement.getPredicate), statement.getObject, true)
    ) {
      diff.add(statement)
      repositoryConnection.add(statement)
    }
  }

  /**
    * Remove inferred statements from the repository if it is existing
    */
  protected def removeStatements(diff: ModelDiff, statements: util.Collection[Statement]): Unit = {
    val existingStatements = statements.asScala
      .filter(repositoryConnection.hasStatement(_, false))
      .asJavaCollection
    diff.remove(existingStatements)
    repositoryConnection.remove(existingStatements)
  }

  /**
    * Remove an inferred statement from the repository if it is existing
    */
  protected def removeStatement(diff: ModelDiff, statement: Statement): Unit = {
    if (repositoryConnection.hasStatement(statement, false)) {
      diff.remove(statement)
      repositoryConnection.remove(statement)
    }
  }

  protected def isDifferentFrom(facet1: Resource, facet2: Resource): Boolean = {
    isDifferentFromQuery.setBinding("facet1", facet1)
    isDifferentFromQuery.setBinding("facet2", facet2)
    isDifferentFromQuery.evaluate()
  }
}
