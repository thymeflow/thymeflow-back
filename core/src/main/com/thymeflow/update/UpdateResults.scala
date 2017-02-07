package com.thymeflow.update

import com.thymeflow.rdf.model.ModelDiff
import com.thymeflow.utilities.{Error, Ok}
import org.eclipse.rdf4j.model.Statement

/**
  * @author Thomas Pellissier Tanon
  */
case class UpdateResults(
                          added: Map[Statement, UpdateResult],
                          removed: Map[Statement, UpdateResult]
                        ) {

  /**
    * Merges update results. An insertion is considered as Ok it has been done at least once and a deletion is considered
    * as Error if it has failed at least once.
    */
  def merge(results: UpdateResults): UpdateResults = {
    UpdateResults(
      (added.keys ++ results.added.keys).map(statement =>
        (added.getOrElse(statement, Error(List())), results.added.getOrElse(statement, Error(List()))) match {
          case (Ok(_), _) => statement -> Ok()
          case (_, Ok(_)) => statement -> Ok()
          case (Error(ex1: List[Exception]), Error(ex2: List[Exception])) => statement -> Error(ex1 ++ ex2)
        }).toMap,
      (removed.keys ++ results.removed.keys).map(statement =>
        (added.getOrElse(statement, Ok()), results.added.getOrElse(statement, Ok())) match {
          case (Error(ex1: List[Exception]), Error(ex2: List[Exception])) => statement -> Error(ex1 ++ ex2)
          case (Error(ex), _) => statement -> Error(ex)
          case (_, Error(ex)) => statement -> Error(ex)
          case (Ok(_), Ok(_)) => statement -> Ok()
        }).toMap
    )
  }
}

object UpdateResults {

  def apply(): UpdateResults = new UpdateResults(Map.empty, Map.empty)

  def allFailed(diff: ModelDiff): UpdateResults = new UpdateResults(
    diff.added.map(statement => statement -> Error(List()))(scala.collection.breakOut),
    diff.removed.map(statement => statement -> Error(List()))(scala.collection.breakOut)
  )

  def allFailed(diff: ModelDiff, error: Exception): UpdateResults = new UpdateResults(
    diff.added.map(statement => statement -> Error(List(error)))(scala.collection.breakOut),
    diff.removed.map(statement => statement -> Error(List(error)))(scala.collection.breakOut)
  )

  def merge(results: Traversable[UpdateResults]): UpdateResults = {
    if (results.isEmpty) {
      UpdateResults()
    } else {
      results.reduce(UpdateResults.merge)
    }
  }

  def merge(results: UpdateResults*): UpdateResults = {
    merge(results)
  }

  /**
    * Merges update results. An insertion is considered as Ok it has been done at least once and a deletion is considered
    * as Error if it has failed at least once.
    */
  def merge(results1: UpdateResults, results2: UpdateResults): UpdateResults = {
    UpdateResults(
      (results1.added.keys ++ results2.added.keys).map(statement =>
        statement -> mergeAdded(results1.added.getOrElse(statement, Error(List())), results2.added.getOrElse(statement, Error(List())))
      ).toMap,
      (results1.removed.keys ++ results2.removed.keys).map(statement =>
        statement -> mergeRemoved(results1.added.getOrElse(statement, Ok()), results2.added.getOrElse(statement, Ok()))
      ).toMap
    )
  }

  def mergeAdded(result1: UpdateResult, result2: UpdateResult): UpdateResult = {
    (result1, result2) match {
      case (Ok(_), _) => Ok()
      case (_, Ok(_)) => Ok()
      case (Error(ex1: List[Exception]), Error(ex2: List[Exception])) => Error(ex1 ++ ex2)
    }
  }

  def mergeRemoved(result1: UpdateResult, result2: UpdateResult): UpdateResult = {
    (result1, result2) match {
      case (Error(ex1: List[Exception]), Error(ex2: List[Exception])) => Error(ex1 ++ ex2)
      case (Error(ex), _) => Error(ex)
      case (_, Error(ex)) => Error(ex)
      case (Ok(_), Ok(_)) => Ok()
    }
  }
}
