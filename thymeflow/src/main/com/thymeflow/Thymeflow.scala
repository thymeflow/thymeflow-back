package com.thymeflow

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.thymeflow.enricher._
import com.thymeflow.enricher.entityresolution.AgentMatchEnricher
import com.thymeflow.rdf.repository.{Repository, RepositoryFactory}
import com.thymeflow.service.File
import com.thymeflow.spatial.geocoding.Geocoder
import com.thymeflow.sync._
import com.thymeflow.sync.converter.GoogleLocationHistoryConverter
import com.thymeflow.sync.facebook.FacebookSynchronizer
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
object Thymeflow extends StrictLogging {

  def main(args: Array[String]) {
    import com.thymeflow.actors._
    implicit val config = com.thymeflow.config.cli
    val repository = RepositoryFactory.initializeRepository()
    val pipeline = Thymeflow.initialize(repository)
    args.map(x => File.account(Paths.get(x))).foreach {
      accountFuture => accountFuture.foreach(pipeline.addServiceAccount)
    }
    sys.addShutdownHook {
      repository.shutdown()
    }
  }

  def initialize(repository: Repository)(implicit config: Config, actorSystem: ActorSystem, materializer: Materializer) = {
    val geocoder = Geocoder.google(Some(Paths.get(System.getProperty("java.io.tmpdir"), "thymeflow/google-api-cache")))

    setupSynchronizers()
    val newConnection = repository.newConnection _
    val pipelineStartTime = System.currentTimeMillis()
    Pipeline.create(
      repository,
      Vector(
        FileSynchronizer,
        CalDavSynchronizer,
        CardDavSynchronizer,
        EmailSynchronizer,
        FacebookSynchronizer
      ),
      Pipeline.enricherToFlow(new InverseFunctionalPropertyInferencer(newConnection))
        .via(Pipeline.enricherToFlow(new PlacesGeocoderEnricher(newConnection, geocoder)))
        .via(Pipeline.delayedBatchToFlow(10 seconds))
        .via(Pipeline.enricherToFlow(new LocationStayEnricher(newConnection)))
        .via(Pipeline.enricherToFlow(new LocationEventEnricher(newConnection)))
        .via(Pipeline.enricherToFlow(new EventsWithStaysGeocoderEnricher(newConnection, geocoder)))
        .via(Pipeline.enricherToFlow(new AgentMatchEnricher(newConnection)))
        .via(Pipeline.enricherToFlow(new PrimaryFacetEnricher(newConnection)))
        .map(diff => {
          val durationSinceStart = Duration(System.currentTimeMillis() - pipelineStartTime, TimeUnit.MILLISECONDS)
          logger.info(s"A diff went at the end of the pipeline with ${diff.added.size} additions and ${diff.removed.size} deletions at time $durationSinceStart")
          diff
        })
    )
  }

  def setupSynchronizers() = {
    FileSynchronizer.registerExtension("json", "application/json")
    FileSynchronizer.registerConverter("application/json", new GoogleLocationHistoryConverter(_)(_))
  }
}
