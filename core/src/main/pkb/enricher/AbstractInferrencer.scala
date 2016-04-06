package pkb.enricher

import org.openrdf.model.Statement
import org.openrdf.repository.RepositoryConnection
import pkb.rdf.model.ModelDiff

import scala.collection.mutable

/**
  * @author Thomas Pellissier Tanon
  */
abstract class AbstractInferrencer(repositoryConnection: RepositoryConnection) extends Enricher {

  private val timesStatementIsInferred = new mutable.HashMap[Int, Int]() //The statement is stored as its hash

  /**
    * Add an inferred statement to the repository if it is not already existing
    * In any case increment the counter of the number of inferences of this statement
    */
  protected def addInferredStatement(diff: ModelDiff, statement: Statement): Unit = {
    timesStatementIsInferred.put(statement.hashCode(), timesStatementIsInferred.getOrElse(statement.hashCode(), 0) + 1)

    if (!repositoryConnection.hasStatement(statement, true)) {
      diff.added.add(statement)
      repositoryConnection.add(statement)
    }
  }

  /**
    * Removes an inferred statement from the repository if the number of inferences from it was of 1
    * In any case decrement the counter of the number of inferences of this statement
    *
    * WARNING: you should add a specific context to triples guessed by your inferencer in order to avoid removal of statements
    * added from an other inferencer/data source
    */
  protected def removeInferredStatement(diff: ModelDiff, statement: Statement): Unit = {
    if (timesStatementIsInferred.put(statement.hashCode(), timesStatementIsInferred.getOrElse(statement.hashCode(), 0) - 1).contains(1)) {
      diff.removed.add(statement)
      repositoryConnection.remove(statement)
    }
  }
}
