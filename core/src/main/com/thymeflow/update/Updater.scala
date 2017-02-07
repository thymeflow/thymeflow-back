package com.thymeflow.update

import com.thymeflow.Supervisor
import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.vocabulary.{Negation, Personal}
import com.thymeflow.rdf.model.{StatementSet, StatementSetDiff}
import com.thymeflow.sync.Synchronizer.Update
import com.thymeflow.utilities.{Error, Ok}
import com.typesafe.scalalogging.StrictLogging
import org.eclipse.rdf4j.model.{Resource, Statement}
import org.eclipse.rdf4j.repository.RepositoryConnection

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * @author Thomas Pellissier Tanon
  */
class Updater(repositoryConnection: RepositoryConnection, supervisor: Supervisor.Interactor) extends StrictLogging {

  private implicit val valueFactory = repositoryConnection.getValueFactory
  private val userDataContext = valueFactory.createIRI(Personal.NAMESPACE, "userData") //TODO: config

  def apply(diff: StatementSetDiff): Future[UpdateResults] = {
    val (diffWithoutContext, diffWithContext) = splitGraphFromDiff(diff, null)
    val guessedContextForWithoutContext = addPossibleContexts(diffWithoutContext)

    applyDiffWithContext(StatementSetDiff.merge(guessedContextForWithoutContext, diffWithContext)).map(updateResult => {
      repositoryConnection.begin()
      val endResult = UpdateResults.merge(
        //Without context
        UpdateResults(
          diffWithoutContext.added.map(statement =>
            statement -> findWithoutContext(guessedContextForWithoutContext.added, statement).flatMap(updateResult.added.get)
          ).toMap
            .mapValues(results => if (results.isEmpty) {
              Some(Error(None)).asInstanceOf[Traversable[UpdateResult]]
            } else {
              results
            })
            .mapValues(_.reduce(UpdateResults.mergeAdded))
            .map {
              case (statement, Error(l)) =>
                repositoryConnection.add(statement.getSubject, statement.getPredicate, statement.getObject, userDataContext)
                (statement, Error(l))
              case v => v
            },
          diffWithoutContext.removed.map(statement =>
            statement -> findWithoutContext(guessedContextForWithoutContext.removed, statement).flatMap(updateResult.removed.get)
          ).toMap
            .mapValues(results => if (results.isEmpty) {
              Some(Error(None)).asInstanceOf[Traversable[UpdateResult]]
            } else {
              results
            })
            .mapValues(_.reduce(UpdateResults.mergeRemoved))
            .map {
              case (statement, Error(l)) =>
                repositoryConnection.add(statement.getSubject, Negation.not(statement.getPredicate), statement.getObject, userDataContext)
                repositoryConnection.remove(statement.getSubject, statement.getPredicate, statement.getObject)
                (statement, Error(l))
              case v => v
            }
        ),
        //With context
        UpdateResults(
          diffWithContext.added.map(statement =>
            statement -> updateResult.added.getOrElse(statement, noGraphEditorFound(statement.getContext))
          ).toMap,
          diffWithContext.removed.map(statement =>
            statement -> updateResult.removed.getOrElse(statement, noGraphEditorFound(statement.getContext))
          ).toMap
        )
      )
      repositoryConnection.commit()
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

  private def addPossibleContexts(diff: StatementSetDiff): StatementSetDiff = {
    new StatementSetDiff(
      addPossibleContextsToAddedStatements(diff.added),
      addPossibleContextsToRemovedStatements(diff.removed)
    )
  }

  private def addPossibleContextsToAddedStatements(statements: StatementSet): StatementSet = {
    statements.flatMap(statement =>
      findPossibleContextsForAddedStatement(statement)
        .map(statementWithContext(statement, _))
        .filterNot(statement => repositoryConnection.hasStatement(statement, false, statement.getContext))
    )
  }

  private def findPossibleContextsForAddedStatement(statement: Statement): Iterator[Resource] = {
    Option(statement.getContext)
      .map(Some(_).toIterator)
      .getOrElse(
        repositoryConnection.getStatements(statement.getSubject, null, null).flatMap(statement => Option(statement.getContext))
      )
  }

  private def addPossibleContextsToRemovedStatements(statements: StatementSet): StatementSet = {
    statements.flatMap(statement =>
      findExistingStatementsFor(statement)
    )
  }

  private def findExistingStatementsFor(statement: Statement): Iterator[Statement] = {
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

  private def findWithoutContext(statements: StatementSet, statementToFind: Statement): StatementSet = {
    statements.filter(statement => statement.getSubject == statementToFind.getSubject &&
      statement.getPredicate == statementToFind.getPredicate &&
      statement.getObject == statementToFind.getObject &&
      statement.getContext == null)
  }

  private def applyDiffWithContext(diff: StatementSetDiff): Future[UpdateResults] = {
    val cleanedDiff = new StatementSetDiff(
      diff.added.filterNot(hasStatementWithContext(_, false)),
      diff.removed.filter(hasStatementWithContext(_, true))
    )
    val (userGraphDiff, sourceGraphDiff) = splitGraphFromDiff(cleanedDiff, userDataContext)
    val userGraphResult = applyDiffToRepository(userGraphDiff)
    sendDiffToSource(sourceGraphDiff).map(UpdateResults.merge(_, userGraphResult))
  }

  private def sendDiffToSource(diff: StatementSetDiff): Future[UpdateResults] = {
    if (diff.isEmpty) {
      Future.successful(UpdateResults())
    } else {
      implicit val timeout = com.thymeflow.actors.timeout
      supervisor.applyUpdate(Update(diff))
    }
  }

  private def applyDiffToRepository(diff: StatementSetDiff): UpdateResults = {
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

  private def hasStatementWithContext(statement: Statement, includeInferred: Boolean): Boolean = {
    repositoryConnection.hasStatement(statement, includeInferred, statement.getContext)
  }

  private def noGraphEditorFound(context: Resource): UpdateResult =
    Error(List(new Exception(s"No controller for graph $context found.")))

}
