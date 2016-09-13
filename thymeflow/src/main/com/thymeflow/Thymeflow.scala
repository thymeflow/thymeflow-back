package com.thymeflow

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import akka.stream.scaladsl.Source
import com.thymeflow.actors._
import com.thymeflow.enricher._
import com.thymeflow.enricher.entityresolution.{EntityResolution, ParisEnricher}
import com.thymeflow.rdf.model.ModelDiff
import com.thymeflow.rdf.repository.{Repository, RepositoryFactory}
import com.thymeflow.spatial.geocoding.Geocoder
import com.thymeflow.sync._
import com.thymeflow.sync.converter.GoogleLocationHistoryConverter
import com.thymeflow.sync.facebook.FacebookSynchronizer
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
object Thymeflow extends StrictLogging {

  def main(args: Array[String]) {
    implicit val config = com.thymeflow.config.cli
    System.getProperties.setProperty("line.separator", "\r\n")
    val repository = RepositoryFactory.initializeRepository()
    val pipeline = initializePipeline(repository)
    args.map(x => FileSynchronizer.Config(Paths.get(x))).foreach {
      fileConfig => pipeline.addSourceConfig(fileConfig)
    }
  }

  def initializePipeline(repository: Repository)(implicit config: Config) = {
    val geocoder = Geocoder.google(Some(Paths.get(System.getProperty("java.io.tmpdir"), "thymeflow/google-api-cache")))

    setupSynchronizers()
    val pipelineStartTime = System.currentTimeMillis()
    val crws = Vector(Some(0.25), Some(0.5), Some(0.75), None)
    val idfs = Vector(true, false)
    val similarities = Vector(EntityResolution.LevensteinSimilarity, EntityResolution.JaroWinklerSimilarity)
    val smps = Vector(60)
    val mdts = Vector(1.0)
    val idMatches = Vector(true, false)
    val evaluationFiles = Files.newDirectoryStream(Paths.get("data/barack_evaluations")).iterator().asScala.toVector
    /*val agentMatchEnrichers = for (crw <- crws;
                          idf <- idfs;
                          similarity <- similarities;
                          smp <- smps;
                          mdt <- mdts) yield {
      new AgentMatchEnricher(repository.newConnection,
        solveMode = AgentMatchEnricher.Vanilla,
        baseStringSimilarity = similarity,
        searchSize = 10000,
        searchMatchPercent = smp,
        matchDistanceThreshold = mdt,
        contactRelativeWeight = crw,
        evaluationThreshold = BigDecimal("0.5"),
        useIDF = idf,
        evaluationSamplesFiles = evaluationFiles,
        persistenceThreshold = 2.0
      )
    }*/
    val parisEnrichers = for (idf <- idfs;
                              similarity <- similarities;
                              smp <- smps;
                              mdt <- mdts) yield {
      new ParisEnricher(repository.newConnection,
        baseStringSimilarity = similarity,
        searchMatchPercent = smp,
        searchSize = 10000,
        matchDistanceThreshold = mdt,
        useIDF = idf,
        evaluationSamplesFiles = evaluationFiles,
        persistenceThreshold = 2.0,
        maxIterations = 3
      )
    }
    val parallelism = 1
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
        .mapAsync(1) {
          baseDiff =>
            val h = Source.fromIterator(() => parisEnrichers.iterator).mapAsyncUnordered(parallelism) {
              enricher =>
                logger.info(s"Running enricher $enricher")
                val f = Future {
                  val diff = ModelDiff.merge(baseDiff)
                  enricher.enrich(diff)
                  diff
                }
                f.onFailure {
                  case e => logger.error("Parallel enricher failure", e)
                }
                f
            }.runFold(Vector.empty[ModelDiff]) {
              case (x, f) => x :+ f
            }.map {
              case diffs => ModelDiff.merge(diffs: _*)
            }
            h.onFailure {
              case e => logger.error("Parallel enricher pipeline failure", e)
            }
            h
        }
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
