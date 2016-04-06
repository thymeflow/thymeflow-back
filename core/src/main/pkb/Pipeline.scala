package pkb

import akka.NotUsed
import akka.actor.ActorRef
import akka.stream.SourceShape
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.repository.RepositoryConnection
import pkb.actors._
import pkb.inferencer.Enricher
import pkb.rdf.Converters._
import pkb.rdf.model.document.Document
import pkb.rdf.model.{ModelDiff, SimpleHashModel}
import pkb.sync.Synchronizer.Sync
import pkb.sync._

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  */
class Pipeline(repositoryConnection: RepositoryConnection, enrichers: Iterable[Enricher])
  extends StrictLogging {

  private val actorRefs = buildSource()
    .via(buildRepositoryInsertion())
    .via(buildInferenceSystem())
    .to(Sink.ignore)
    .run()

  actorRefs.foreach(system.scheduler.schedule(1 minute, 1 minute, _, Sync))

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

    repositoryConnection.remove(statementsToRemove)
    repositoryConnection.add(statements)
    repositoryConnection.commit()

    new ModelDiff(statements, statementsToRemove)
  }

  private def buildInferenceSystem(): Flow[ModelDiff, ModelDiff, NotUsed] = {
    var flow = Flow[ModelDiff]
    enrichers.foreach(inferencer =>
      flow = flow.map(diff => {
        inferencer.enrich(diff)
        diff
      })
    )
    flow
  }
}
