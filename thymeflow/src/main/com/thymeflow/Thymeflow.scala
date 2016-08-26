package com.thymeflow

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import com.thymeflow.enricher._
import com.thymeflow.enricher.entityresolution.AgentMatchEnricher
import com.thymeflow.rdf.RepositoryFactory
import com.thymeflow.rdf.repository.Repository
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
    implicit val config = com.thymeflow.config.cli
    val repository = RepositoryFactory.initializeRepository()
    val pipeline = initializePipeline(repository)
    args.map(x => FileSynchronizer.Config(Paths.get(x))).foreach {
      fileConfig => pipeline.addSourceConfig(fileConfig)
    }
  }

  def initializePipeline(repository: Repository)(implicit config: Config) = {
    val geocoder = Geocoder.cached(
      Geocoder.googleMaps(),
      Some(Paths.get(System.getProperty("java.io.tmpdir"), "thymeflow/geocoder-google-cache"))
    )

    setupSynchronizers()
    val pipelineStartTime = System.currentTimeMillis()
    Pipeline.create(
      repository.newConnection(),
      Vector(
        FileSynchronizer,
        CalDavSynchronizer,
        CardDavSynchronizer,
        EmailSynchronizer,
        FacebookSynchronizer
      ).map(_.source(repository.valueFactory)),
      Pipeline.enricherToFlow(new InverseFunctionalPropertyInferencer(repository.newConnection))
        .via(Pipeline.enricherToFlow(new PlacesGeocoderEnricher(repository.newConnection, geocoder)))
        .via(Pipeline.delayedBatchToFlow(10 seconds))
        .via(Pipeline.enricherToFlow(new LocationStayEnricher(repository.newConnection)))
        .via(Pipeline.enricherToFlow(new LocationEventEnricher(repository.newConnection)))
        .via(Pipeline.enricherToFlow(new EventsWithStaysGeocoderEnricher(repository.newConnection, geocoder)))
        .via(Pipeline.enricherToFlow(new AgentMatchEnricher(repository.newConnection)))
        .via(Pipeline.enricherToFlow(new PrimaryFacetEnricher(repository.newConnection)))
        .map(diff => {
          val durationSinceStart = Duration(System.currentTimeMillis() - pipelineStartTime, TimeUnit.MILLISECONDS)
          logger.info(s"A diff went at the end of the pipeline with ${diff.added.size()} additions and ${diff.removed.size()} deletions at time $durationSinceStart")
          diff
        })
    )
  }

  def setupSynchronizers() = {
    FileSynchronizer.registerExtension("json", "application/json")
    FileSynchronizer.registerConverter("application/json", new GoogleLocationHistoryConverter(_)(_))
  }
}
