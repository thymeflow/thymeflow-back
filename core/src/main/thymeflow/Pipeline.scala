package thymeflow

import akka.NotUsed
import akka.actor.ActorRef
import akka.pattern.ask
import akka.stream._
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.repository.RepositoryConnection
import thymeflow.actors._
import thymeflow.enricher.{DelayedBatch, Enricher}
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.document.Document
import thymeflow.rdf.model.vocabulary.Negation
import thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import thymeflow.sync.Synchronizer.Update
import thymeflow.update.UpdateResults
import thymeflow.utilities.VectorExtensions

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
  extends StrictLogging {

  private val sourceRefs = source
    .via(buildRepositoryInsertion())
    .via(enrichers)
    .to(Sink.ignore)
    .run()

  def addSourceConfig(sourceConfig: Any): Unit = {
    sourceRefs.foreach(_ ! sourceConfig)
  }

  def applyUpdate(update: Update): Future[UpdateResults] = {
    //TODO: filter
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

    //Do not add already existing statements or with already a negation
    val statementsToAdd = new SimpleHashModel(
      repositoryConnection.getValueFactory,
      statements.asScala
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

    new ModelDiff(statements, statementsToRemove)
  }
}

object Pipeline {

  def create(repositoryConnection: RepositoryConnection,
             sources: Traversable[Graph[SourceShape[Document], ActorRef]],
             enrichers: Graph[FlowShape[ModelDiff, ModelDiff], _]) = {
    val sourcesVector = sources.map(Source.fromGraph(_).mapMaterializedValue(List(_))).toVector
    if (sourcesVector.isEmpty) {
      throw new IllegalArgumentException("Pipeline requires at least one source.")
    }
    val source = VectorExtensions.reduceLeftTree(sourcesVector)(mergeSources)
    new Pipeline(repositoryConnection,
      source,
      enrichers)
  }

  private def mergeSources[Out, Mat](s1: Source[Out, List[Mat]], s2: Source[Out, List[Mat]]): Source[Out, List[Mat]] = {
    Source.fromGraph[Out, List[Mat]](GraphDSL.create(s1, s2)(_ ++ _) { implicit builder => (s1, s2) =>
      val merge = builder.add(Merge[Out](2))
      s1 ~> merge
      s2 ~> merge
      SourceShape(merge.out)
    })
  }

  def delayedBatchToFlow(delay: FiniteDuration) = Flow[ModelDiff].via(DelayedBatch[ModelDiff]((diff1, diff2) => {
    diff1.apply(diff2)
    diff1
  }, delay))

  def enricherToFlow(enricher: Enricher) = Flow[ModelDiff].map(diff => {
    enricher.enrich(diff)
    diff
  })
}
