package com.thymeflow.update

import com.thymeflow.rdf.model.StatementSetDiff
import com.thymeflow.utilities.{Error, Ok}
import org.eclipse.rdf4j.model.Statement

/**
  * @author Thomas Pellissier Tanon
  */
case class UpdateResults(
                          added: Map[Statement, UpdateResult],
                          removed: Map[Statement, UpdateResult]
                        ) {
}

object UpdateResults {

  def apply(): UpdateResults = new UpdateResults(Map.empty, Map.empty)

  def allFailed(diff: StatementSetDiff, error: Exception): UpdateResults = new UpdateResults(
    diff.added.map(statement => statement -> Error(Seq(error)))(scala.collection.breakOut),
    diff.removed.map(statement => statement -> Error(Seq(error)))(scala.collection.breakOut)
  )

  /**
    * Merges update results. An insertion is considered as Ok it has been done at least once and a deletion is considered
    * as Error if it has failed at least once.
    */
  def merge(results: UpdateResults*): UpdateResults = merge(results)

  def merge(results: Traversable[UpdateResults]): UpdateResults = {
    UpdateResults(
      results.flatMap(_.added.keys).toVector.distinct.map(statement =>
        statement -> mergeAdded(results.map(_.added.getOrElse(statement, Error(Seq()))))
      )(scala.collection.breakOut),
      results.flatMap(_.removed.keys).toVector.distinct.map(statement =>
        statement -> mergeRemoved(results.map(_.removed.getOrElse(statement, Ok())))
      )(scala.collection.breakOut)
    )
  }

  def mergeAdded(results: Traversable[UpdateResult]): UpdateResult = {
    if (results.exists(_.isOk)) {
      Ok()
    } else {
      Error(results.flatMap {
        case Error(ex: Seq[Exception]) => ex
        case _ => Seq.empty
      }(scala.collection.breakOut))
    }
  }

  def mergeRemoved(results: Traversable[UpdateResult]): UpdateResult = {
    if (results.exists(_.isError)) {
      Error(results.flatMap {
        case Error(ex: Seq[Exception]) => ex
        case _ => Seq.empty
      }(scala.collection.breakOut))
    } else {
      Ok()
    }
  }
}
