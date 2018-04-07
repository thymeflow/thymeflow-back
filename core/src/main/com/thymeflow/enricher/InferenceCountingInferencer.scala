package com.thymeflow.enricher

import com.thymeflow.rdf.model.StatementSetDiff
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.repository.RepositoryConnection

import scala.collection.mutable

/**
  * @author Thomas Pellissier Tanon
  */
abstract class InferenceCountingInferencer(newRepositoryConnection: () => RepositoryConnection) extends AbstractEnricher(newRepositoryConnection) {

  private val timesStatementIsInferred = mutable.Map[Int, Int]() //The statement is stored as its hash

  /**
    * Add an inferred statement to the repository if it is not already existing
    * In any case increment the counter of the number of inferences of this statement
    */
  override protected def addStatement(diff: StatementSetDiff, statement: Statement): Unit = {
    val key = statement.hashCode()
    val oldCount = timesStatementIsInferred.getOrElse(key, 0)
    timesStatementIsInferred.put(key, oldCount + 1)
    if (oldCount == 0) {
      super.addStatement(diff, statement)
    }
  }

  /**
    * Removes an inferred statement from the repository if the number of inferences from it was of 1
    * In any case decrement the counter of the number of inferences of this statement
    *
    * WARNING: you should add a specific context to triples guessed by your inferencer in order to avoid removal of statements
    * added from an other inferencer/data source
    */
  override protected def removeStatement(diff: StatementSetDiff, statement: Statement): Unit = {
    val key = statement.hashCode()
    timesStatementIsInferred.get(key).foreach(oldCount =>
      if (oldCount == 1) {
        timesStatementIsInferred.remove(key)
        super.removeStatement(diff, statement)
      } else {
        timesStatementIsInferred.put(key, oldCount - 1)
      }
    )
  }
}
