package thymeflow

import akka.NotUsed
import akka.actor.ActorRef
import akka.stream._
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.repository.RepositoryConnection
import thymeflow.actors._
import thymeflow.enricher.{DelayedBatch, Enricher}
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.document.Document
import thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import thymeflow.sync.Synchronizer.Sync
import thymeflow.sync._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  */
class Pipeline(repositoryConnection: RepositoryConnection, enrichers: Flow[ModelDiff, ModelDiff, _])
  extends StrictLogging {

  private val actorRefs = buildSource()
    .via(buildRepositoryInsertion())
    .via(enrichers)
    .to(Sink.ignore)
    .run()

  actorRefs.foreach(system.scheduler.schedule(1 second, 1 second, _, Sync))

  def addSource[T](sourceConfig: T): Unit = {
    actorRefs.foreach(_ ! sourceConfig)
  }

  private def buildSource(): Source[Document, List[ActorRef]] = {
    //TODO: find a way to not hardcode synchronizers
    val valueFactory = repositoryConnection.getValueFactory

    val files = FileSynchronizer.source(valueFactory)
    val calDav = CalDavSynchronizer.source(valueFactory)
    val cardDav = CardDavSynchronizer.source(valueFactory)
    val emails = EmailSynchronizer.source(valueFactory)
    Source.fromGraph[Document, List[ActorRef]](GraphDSL.create(files, calDav, cardDav, emails)(List(_, _, _, _)) { implicit builder =>
      (files, calDav, cardDav, emails) =>
        val merge = builder.add(Merge[Document](4))
        files ~> merge
        calDav ~> merge
        cardDav ~> merge
        emails ~> merge

        SourceShape(merge.out)
    })
  }

  private def buildRepositoryInsertion(): Flow[Document, ModelDiff, NotUsed] = {
    Flow[Document].map(addDocumentToRepository)
  }

  private def addDocumentToRepository(document: Document): ModelDiff = {
    repositoryConnection.begin()
    //Removes the removed statements from the repository and the already existing statements from statements
    val statements = new SimpleHashModel(repositoryConnection.getValueFactory, document.model)
    val statementsToRemove = new SimpleHashModel(repositoryConnection.getValueFactory)

    if (document.iri != null) {
      repositoryConnection.getStatements(null, null, null, document.iri).foreach(existingStatement =>
        if (document.model.contains(existingStatement)) {
          statements.remove(existingStatement)
        } else {
          statementsToRemove.add(existingStatement)
        }
      )
    }

    //Do not add already existing statements
    val statementsToAdd = new SimpleHashModel(
      repositoryConnection.getValueFactory,
      statements.asScala.filterNot(statement => repositoryConnection.hasStatement(statement, false)).asJava
    )

    repositoryConnection.add(statementsToAdd)
    repositoryConnection.remove(statementsToRemove)
    repositoryConnection.commit()

    new ModelDiff(statements, statementsToRemove)
  }
}

object Pipeline {
  def delayedBatchToFlow(delay: FiniteDuration) = Flow[ModelDiff].via(DelayedBatch[ModelDiff]((diff1, diff2) => {
    diff1.apply(diff2)
    diff1
  }, delay))

  def enricherToFlow(enricher: Enricher) = Flow[ModelDiff].map(diff => {
    enricher.enrich(diff)
    diff
  })
}
