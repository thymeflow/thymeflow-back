package com.thymeflow

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorRefFactory}
import akka.pattern.ask
import akka.stream._
import akka.stream.scaladsl._
import akka.util.Timeout
import com.thymeflow.enricher.{DelayedBatch, Enricher}
import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.document.Document
import com.thymeflow.rdf.model.vocabulary.Negation
import com.thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import com.thymeflow.rdf.repository.Repository
import com.thymeflow.service.ServiceAccountSources
import com.thymeflow.sync.Synchronizer
import com.thymeflow.sync.Synchronizer.Update
import com.thymeflow.update.UpdateResults
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.repository.RepositoryConnection

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  */
class Pipeline private(repositoryConnection: RepositoryConnection,
                       source: Source[Document, Traversable[ActorRef]],
                       enrichers: Graph[FlowShape[ModelDiff, ModelDiff], _])
                      (implicit materializer: Materializer)
  extends StrictLogging {

  private val sourceRefs = source
    .via(buildRepositoryInsertion())
    .filter(!_.isEmpty)
    .via(enrichers)
    .to(Sink.ignore)
    .run()

  def addServiceAccount(serviceAccountSources: ServiceAccountSources)(implicit sender: ActorRef = Actor.noSender): Unit = {
    sourceRefs.foreach(_ ! serviceAccountSources)
  }

  def applyUpdate(update: Update)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[UpdateResults] = {
    //TODO: filter
    implicit val ec = this.materializer.executionContext
    Future.sequence(sourceRefs.map(_ ? update)).map(_.flatMap {
      case result: UpdateResults => Some(result)
      case _ => None
    }).map(UpdateResults.merge)
  }

  private def buildRepositoryInsertion(): Flow[Document, ModelDiff, NotUsed] = {
    Flow[Document].map(addDocumentToRepository)
  }

  private def addDocumentToRepository(document: Document): ModelDiff = {
    repositoryConnection.begin()
    //Removes the removed statements from the repository and the already existing statements from statements
    val documentStatements = new SimpleHashModel(repositoryConnection.getValueFactory, document.model)
    val statementsToRemove = new SimpleHashModel(repositoryConnection.getValueFactory)

    if (document.iri != null) {
      repositoryConnection.getStatements(null, null, null, document.iri).foreach(existingStatement =>
        if (document.model.contains(existingStatement)) {
          documentStatements.remove(existingStatement)
        } else {
          statementsToRemove.add(existingStatement)
        }
      )
    }

    //Do not add already existing statements or with already a negation
    val statementsToAdd = new SimpleHashModel(
      repositoryConnection.getValueFactory,
      documentStatements.asScala
        .filterNot(statement => repositoryConnection.hasStatement(statement, false))
        .filterNot(statement => repositoryConnection.hasStatement(
          statement.getSubject,
          Negation.not(statement.getPredicate),
          statement.getObject,
          true
        ))
        .asJava
    )

    repositoryConnection.add(statementsToAdd)
    repositoryConnection.remove(statementsToRemove)
    repositoryConnection.commit()

    new ModelDiff(statementsToAdd, statementsToRemove)
  }
}

object Pipeline {

  def apply(repositoryConnection: RepositoryConnection,
            source: Source[Document, Traversable[ActorRef]],
            enrichers: Graph[FlowShape[ModelDiff, ModelDiff], _])(implicit materializer: Materializer) = {
    new Pipeline(repositoryConnection, source, enrichers)
  }

  def create(repository: Repository,
             synchronizers: Seq[Synchronizer],
             enrichers: Graph[FlowShape[ModelDiff, ModelDiff], _])
            (implicit config: Config, actorFactory: ActorRefFactory, materializer: Materializer) = {
    Supervisor.interactor(actorFactory.actorOf(Supervisor.props(repository, synchronizers, enrichers), name = "ThymeflowSupervisor"))
  }

  def delayedBatchToFlow(delay: FiniteDuration) =
    Flow[ModelDiff].via(DelayedBatch[ModelDiff]((diff1, diff2) => {
      diff1.apply(diff2)
      diff1
    }, delay))

  def enricherToFlow(enricher: Enricher) = Flow[ModelDiff].map(diff => {
    enricher.enrich(diff)
    diff
  })
}
