package com.thymeflow

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import com.thymeflow.enricher._
import com.thymeflow.enricher.entityresolution.AgentMatchEnricher
import com.thymeflow.rdf.RepositoryFactory
import com.thymeflow.spatial.geocoding.Geocoder
import com.thymeflow.sync._
import com.thymeflow.sync.converter.GoogleLocationHistoryConverter
import com.thymeflow.sync.facebook.FacebookSynchronizer
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.repository.Repository

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
object Thymeflow extends StrictLogging {

  def main(args: Array[String]) {
    val config = com.thymeflow.config.default
    val repository = if (config.getBoolean("thymeflow.cli.repository.disk")) {
      RepositoryFactory.initializedDiskRepository(
        dataDirectory = new File(config.getString("thymeflow.cli.repository.data-directory")),
        fullTextSearch = config.getBoolean("thymeflow.cli.repository.full-text-search"),
        owlInference = config.getBoolean("thymeflow.cli.repository.owl-inference")
      )
    } else {
      RepositoryFactory.initializedMemoryRepository(
        dataDirectory = new File(config.getString("thymeflow.cli.repository.data-directory")),
        persistToDisk = config.getBoolean("thymeflow.cli.repository.persist-to-disk"),
        fullTextSearch = config.getBoolean("thymeflow.cli.repository.full-text-search"),
        snapshotCleanupStore = config.getBoolean("thymeflow.cli.repository.snapshot-cleanup-store"),
        owlInference = config.getBoolean("thymeflow.cli.repository.owl-inference")
      )
    }
    val pipeline = initializePipeline(repository)
    args.map(x => FileSynchronizer.Config(new File(x))).foreach {
      config => pipeline.addSourceConfig(config)
    }
  }

  def initializePipeline(repository: Repository) = {
    val geocoder = Geocoder.cached(
      Geocoder.googleMaps(),
      Some(Paths.get(System.getProperty("java.io.tmpdir"), "thymeflow/geocoder-google-cache"))
    )

    setupSynchronizers()
    val pipelineStartTime = System.currentTimeMillis()
    Pipeline.create(
      repository.getConnection,
      List(
        FileSynchronizer.source(repository.getValueFactory),
        CalDavSynchronizer.source(repository.getValueFactory),
        CardDavSynchronizer.source(repository.getValueFactory),
        EmailSynchronizer.source(repository.getValueFactory),
        FacebookSynchronizer.source(repository.getValueFactory)
      ),
      Pipeline.enricherToFlow(new InverseFunctionalPropertyInferencer(repository.getConnection))
        .via(Pipeline.enricherToFlow(new PlacesGeocoderEnricher(repository.getConnection, geocoder)))
        .via(Pipeline.delayedBatchToFlow(10 seconds))
        .via(Pipeline.enricherToFlow(new LocationStayEnricher(repository.getConnection)))
        .via(Pipeline.enricherToFlow(new LocationEventEnricher(repository.getConnection)))
        .via(Pipeline.enricherToFlow(new EventsWithStaysGeocoderEnricher(repository.getConnection, geocoder)))
        .via(Pipeline.enricherToFlow(new AgentMatchEnricher(repository.getConnection)))
        .via(Pipeline.enricherToFlow(new PrimaryFacetEnricher(repository.getConnection)))
        .map(diff => {
          val durationSinceStart = Duration(System.currentTimeMillis() - pipelineStartTime, TimeUnit.MILLISECONDS)
          logger.info(s"A diff went at the end of the pipeline with ${diff.added.size()} additions and ${diff.removed.size()} deletions at time $durationSinceStart")
          diff
        })
    )
  }

  def setupSynchronizers() = {
    FileSynchronizer.registerExtension("json", "application/json")
    FileSynchronizer.registerConverter("application/json", new GoogleLocationHistoryConverter(_))
  }
}
