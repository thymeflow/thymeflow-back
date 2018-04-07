package com.thymeflow.update

import com.thymeflow.Supervisor
import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.vocabulary.{Negation, Personal}
import com.thymeflow.rdf.model.{StatementSet, StatementSetDiff}
import com.thymeflow.rdf.repository.Repository
import com.thymeflow.sync.Synchronizer.Update
import com.thymeflow.utilities.{Error, Ok}
import com.typesafe.scalalogging.StrictLogging
import org.eclipse.rdf4j.model.{Resource, Statement, ValueFactory}
import org.eclipse.rdf4j.repository.RepositoryConnection

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * @author Thomas Pellissier Tanon
  */
class Updater(valueFactory: ValueFactory, newRepositoryConnection: () => RepositoryConnection, supervisor: Supervisor.Interactor) extends StrictLogging {

  private implicit val valueFactoryImplicit = valueFactory
  private val userDataContext = valueFactory.createIRI(Personal.NAMESPACE, "userData") //TODO: config

  def apply(diff: StatementSetDiff): Future[UpdateResults] = {
    val (diffWithoutContext, diffWithExplicitContext) = splitGraphFromDiff(diff, null)
    val (userGraphResult, sourceGraphDiff, diffWithPossibleContexts) = {
      implicit val repositoryConnection = newRepositoryConnection()
      repositoryConnection.begin()
      val diffWithPossibleContexts = addPossibleContexts(diffWithoutContext)
      val (userGraphDiff, sourceGraphDiff) = getUserAndSourceGraphDiff(diffWithPossibleContexts, diffWithExplicitContext)
      val userGraphResult = applyUserGraphDiff(userGraphDiff)
      diff.added.foreach(statement => {
        repositoryConnection.remove(statement.getSubject, Negation.not(statement.getPredicate), statement.getObject)
      })
      repositoryConnection.commit()
      repositoryConnection.close()
      (userGraphResult, sourceGraphDiff, diffWithPossibleContexts)
    }

    sendDiffToSource(sourceGraphDiff).map(UpdateResults.merge(_, userGraphResult)).map(updateResult => {
      implicit val repositoryConnection = newRepositoryConnection()
      repositoryConnection.begin()
      val endResult = UpdateResults.merge(
        // Without context
        UpdateResults(
          (diffWithoutContext.added.map(addedStatement => {
            addedStatement -> UpdateResults.mergeAdded(diffWithPossibleContexts.added.flatMap {
              case candidateStatement if filterWithoutContext(addedStatement)(candidateStatement) =>
                updateResult.added.get(candidateStatement)
              case _ => None
            }(scala.collection.breakOut))
          })(scala.collection.breakOut): Map[Statement, UpdateResult])
            .map {
              case (statement, Error(l)) if !repositoryConnection.hasStatement(statement, false) =>
                repositoryConnection.add(statement.getSubject, statement.getPredicate, statement.getObject, userDataContext)
                (statement, Error(l))
              case v =>
                v
            },
          (diffWithoutContext.removed.map(removedStatement => {
            removedStatement -> UpdateResults.mergeAdded(diffWithPossibleContexts.removed.flatMap {
              case candidateStatement if filterWithoutContext(removedStatement)(candidateStatement) =>
                updateResult.removed.get(candidateStatement)
              case _ => None
            }(scala.collection.breakOut))
          })(scala.collection.breakOut): Map[Statement, UpdateResult])
            .map {
              case (statement, Error(l)) if repositoryConnection.hasStatement(statement, true) =>
                repositoryConnection.add(statement.getSubject, Negation.not(statement.getPredicate), statement.getObject, userDataContext)
                repositoryConnection.remove(statement.getSubject, statement.getPredicate, statement.getObject)
                (statement, Error(l))
              case v => v
            }
        ),
        //With context
        UpdateResults(
          diffWithExplicitContext.added.map(statement =>
            statement -> updateResult.added.getOrElse(statement, noGraphEditorFound(statement.getContext))
          ).toMap,
          diffWithExplicitContext.removed.map(statement =>
            statement -> updateResult.removed.getOrElse(statement, noGraphEditorFound(statement.getContext))
          ).toMap
        )
      )
      repositoryConnection.commit()
      repositoryConnection.close()
      endResult
    })
  }

  /**
    * @return The diff for the graph, and the other diff
    */
  private def splitGraphFromDiff(diff: StatementSetDiff, context: Resource): (StatementSetDiff, StatementSetDiff) = {
    (
      new StatementSetDiff(
        diff.added.filter(_.getContext == context),
        diff.removed.filter(_.getContext == context)
      ),
      new StatementSetDiff(
        diff.added.filter(_.getContext != context),
        diff.removed.filter(_.getContext != context)
      )
    )
  }

  private def addPossibleContexts(diff: StatementSetDiff)(implicit repositoryConnection: RepositoryConnection): StatementSetDiff = {
    new StatementSetDiff(
      addPossibleContextsToAddedStatements(diff.added),
      addPossibleContextsToRemovedStatements(diff.removed)
    )
  }

  private def addPossibleContextsToAddedStatements(statements: StatementSet)(implicit repositoryConnection: RepositoryConnection): StatementSet = {
    statements.flatMap(statement =>
      findPossibleContextsForAddedStatement(statement)
        .map(statementWithContext(statement, _))
        .filterNot(statement => Repository.hasStatementWithContext(statement, includeInferred = false))
    )
  }

  private def findPossibleContextsForAddedStatement(statement: Statement)(implicit repositoryConnection: RepositoryConnection): Iterator[Resource] = {
    Option(statement.getContext)
      .map(Some(_).toIterator)
      .getOrElse(
        repositoryConnection.getStatements(statement.getSubject, null, null).flatMap(statement => Option(statement.getContext))
      )
  }

  private def addPossibleContextsToRemovedStatements(statements: StatementSet)(implicit repositoryConnection: RepositoryConnection): StatementSet = {
    statements.flatMap(statement =>
      findExistingStatementsFor(statement)
    )
  }

  private def findExistingStatementsFor(statement: Statement)(implicit repositoryConnection: RepositoryConnection): Iterator[Statement] = {
    if (statement.getContext == null) {
      repositoryConnection.getStatements(statement.getSubject, statement.getPredicate, statement.getObject)
    } else {
      repositoryConnection.getStatements(statement.getSubject, statement.getPredicate, statement.getObject, statement.getContext)
    }
  }

  private def statementWithContext(statement: Statement, context: Resource): Statement = {
    if (statement.getContext != null) {
      throw new IllegalArgumentException("Statement should not have a context")
    }
    valueFactory.createStatement(statement.getSubject, statement.getPredicate, statement.getObject, context)
  }

  private def filterWithoutContext(statementToFind: Statement)(statement: Statement) = {
    statement.getSubject == statementToFind.getSubject &&
      statement.getPredicate == statementToFind.getPredicate &&
      statement.getObject == statementToFind.getObject
  }

  private def getUserAndSourceGraphDiff(diffWithPossibleContexts: StatementSetDiff, diffWithExplicitContext: StatementSetDiff)(implicit repositoryConnection: RepositoryConnection): (StatementSetDiff, StatementSetDiff) = {
    val diff = StatementSetDiff.merge(diffWithPossibleContexts, diffWithExplicitContext)
    // filter out statements to add that are already in the repository
    val toAdd = diff.added.filter(!Repository.hasStatementWithContext(_, includeInferred = false))
    // and filter out statements to remove that are not.
    val toRemove = diff.removed.filter(Repository.hasStatementWithContext(_, includeInferred = true))
    val filteredDiff = new StatementSetDiff(toAdd, toRemove)
    // separate user graph statements
    splitGraphFromDiff(filteredDiff, userDataContext)
  }

  private def sendDiffToSource(diff: StatementSetDiff): Future[UpdateResults] = {
    if (diff.isEmpty) {
      Future.successful(UpdateResults())
    } else {
      implicit val timeout = com.thymeflow.actors.timeout
      supervisor.applyUpdate(Update(diff)).flatMap {
        case Success(s) => Future.successful(s)
        case Failure(t) => Future.failed(t)
      }
    }
  }

  private def applyUserGraphDiff(diff: StatementSetDiff)(implicit repositoryConnection: RepositoryConnection): UpdateResults = {
    UpdateResults(
      diff.added.map(statement => {
        repositoryConnection.add(statement)
        statement -> Ok()
      }).toMap,
      diff.removed.map(statement => {
        repositoryConnection.remove(statement)
        statement -> Ok()
      }).toMap
    )
  }

  private def noGraphEditorFound(context: Resource): UpdateResult =
    Error(List(new Exception(s"No controller for graph $context found.")))

}
