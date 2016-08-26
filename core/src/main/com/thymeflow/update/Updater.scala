package com.thymeflow.update

import com.thymeflow.Pipeline
import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.vocabulary.{Negation, Personal}
import com.thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import com.thymeflow.sync.Synchronizer.Update
import com.thymeflow.utilities.{Error, Ok}
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.{Model, Resource, Statement}
import org.openrdf.repository.RepositoryConnection

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * @author Thomas Pellissier Tanon
  */
class Updater(repositoryConnection: RepositoryConnection, pipeline: Pipeline) extends StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val userDataContext = valueFactory.createIRI(Personal.NAMESPACE, "userData") //TODO: config

  def apply(diff: ModelDiff): Future[UpdateResults] = {
    val (diffWithoutContext, diffWithContext) = splitGraphFromDiff(diff, null)
    val guessedContextForWithoutContext = addPossibleContexts(diffWithoutContext)

    applyDiffWithContext(ModelDiff.merge(guessedContextForWithoutContext, diffWithContext)).map(updateResult => {
      repositoryConnection.begin()
      val endResult = UpdateResults.merge(
        //Without context
        UpdateResults(
          diffWithoutContext.added.asScala.map(statement =>
            statement -> findWithoutContext(guessedContextForWithoutContext.added, statement).asScala.flatMap(updateResult.added.get)
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
          diffWithoutContext.removed.asScala.map(statement =>
            statement -> findWithoutContext(guessedContextForWithoutContext.removed, statement).asScala.flatMap(updateResult.removed.get)
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
          diffWithContext.added.asScala.map(statement =>
            statement -> updateResult.added.getOrElse(statement, noGraphEditorFound(statement.getContext))
          ).toMap,
          diffWithContext.removed.asScala.map(statement =>
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
  private def splitGraphFromDiff(diff: ModelDiff, context: Resource): (ModelDiff, ModelDiff) = {
    (
      new ModelDiff(
        new SimpleHashModel(valueFactory, diff.added.asScala.filter(_.getContext == context).asJava),
        new SimpleHashModel(valueFactory, diff.removed.asScala.filter(_.getContext == context).asJava)
      ),
      new ModelDiff(
        new SimpleHashModel(valueFactory, diff.added.asScala.filter(_.getContext != context).asJava),
        new SimpleHashModel(valueFactory, diff.removed.asScala.filter(_.getContext != context).asJava)
      )
      )
  }

  private def addPossibleContexts(diff: ModelDiff): ModelDiff = {
    new ModelDiff(
      addPossibleContextsToAddedStatements(diff.added),
      addPossibleContextsToRemovedStatements(diff.removed)
    )
  }

  private def addPossibleContextsToAddedStatements(statements: Model): Model = {
    new SimpleHashModel(valueFactory, statements.asScala.flatMap(statement =>
      findPossibleContextsForAddedStatement(statement)
        .map(statementWithContext(statement, _))
        .filterNot(statement => repositoryConnection.hasStatement(statement, false, statement.getContext))
    ).asJava)
  }

  private def findPossibleContextsForAddedStatement(statement: Statement): Iterator[Resource] = {
    Option(statement.getContext)
      .map(Some(_).toIterator)
      .getOrElse(
        repositoryConnection.getStatements(statement.getSubject, null, null).flatMap(statement => Option(statement.getContext))
      )
  }

  private def addPossibleContextsToRemovedStatements(statements: Model): Model = {
    new SimpleHashModel(valueFactory, statements.asScala.flatMap(statement =>
      findExistingStatementsFor(statement)
    ).asJava)
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

  private def findWithoutContext(model: Model, statement: Statement): Model = {
    model.filter(statement.getSubject, statement.getPredicate, statement.getObject, null)
  }

  private def applyDiffWithContext(diff: ModelDiff): Future[UpdateResults] = {
    val cleanedDiff = new ModelDiff(
      new SimpleHashModel(valueFactory, diff.added.asScala.filterNot(hasStatementWithContext(_, false)).asJava),
      new SimpleHashModel(valueFactory, diff.removed.asScala.filter(hasStatementWithContext(_, true)).asJava)
    )
    val (userGraphDiff, sourceGraphDiff) = splitGraphFromDiff(cleanedDiff, userDataContext)
    val userGraphResult = applyDiffToRepository(userGraphDiff)
    sendDiffToSource(sourceGraphDiff).map(UpdateResults.merge(_, userGraphResult))
  }

  private def sendDiffToSource(diff: ModelDiff): Future[UpdateResults] = {
    if (diff.isEmpty) {
      Future {
        UpdateResults()
      }
    } else {
      pipeline.applyUpdate(Update(diff))
    }
  }

  private def applyDiffToRepository(diff: ModelDiff): UpdateResults = {
    UpdateResults(
      diff.added.asScala.map(statement => {
        repositoryConnection.add(statement)
        statement -> Ok()
      }).toMap,
      diff.removed.asScala.map(statement => {
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
