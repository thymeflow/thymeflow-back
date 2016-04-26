package thymeflow.enricher

import org.openrdf.model.Statement
import org.openrdf.repository.RepositoryConnection
import thymeflow.rdf.model.ModelDiff

import scala.collection.mutable

/**
  * @author Thomas Pellissier Tanon
  */
abstract class InferenceCountingInferencer(repositoryConnection: RepositoryConnection) extends AbstractEnricher(repositoryConnection) {

  private val timesStatementIsInferred = new mutable.HashMap[Int, Int]() //The statement is stored as its hash

  /**
    * Add an inferred statement to the repository if it is not already existing
    * In any case increment the counter of the number of inferences of this statement
    */
  override protected def addStatement(diff: ModelDiff, statement: Statement): Unit = {
    timesStatementIsInferred.put(statement.hashCode(), timesStatementIsInferred.getOrElse(statement.hashCode(), 0) + 1)
    super.addStatement(diff, statement)
  }

  /**
    * Removes an inferred statement from the repository if the number of inferences from it was of 1
    * In any case decrement the counter of the number of inferences of this statement
    *
    * WARNING: you should add a specific context to triples guessed by your inferencer in order to avoid removal of statements
    * added from an other inferencer/data source
    */
  override protected def removeStatement(diff: ModelDiff, statement: Statement): Unit = {
    if (
      timesStatementIsInferred.put(statement.hashCode(), timesStatementIsInferred.getOrElse(statement.hashCode(), 0) - 1).contains(1)
    ) {
      super.addStatement(diff, statement)
    }
  }
}
