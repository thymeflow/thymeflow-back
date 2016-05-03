package thymeflow

import java.io.File

import com.typesafe.scalalogging.StrictLogging
import thymeflow.enricher._
import thymeflow.rdf.RepositoryFactory
import thymeflow.spatial.geocoding.Geocoder
import thymeflow.sync.FileSynchronizer
import thymeflow.sync.converter.GoogleLocationHistoryConverter

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
object Thymeflow extends StrictLogging {

  def main(args: Array[String]) {
    val repository = RepositoryFactory.initializedMemoryRepository(snapshotCleanupStore = false, owlInference = false, lucene = false)
    setupSynchronizers()
    val pipeline = new Pipeline(
      repository.getConnection,
      Pipeline.enricherToFlow(new InverseFunctionalPropertyInferencer(repository.getConnection))
        .via(Pipeline.enricherToFlow(new GeocoderEnricher(repository.getConnection, Geocoder.cached(Geocoder.googleMaps()))))
        .via(Pipeline.delayedBatchToFlow(10 seconds))
        .via(Pipeline.enricherToFlow(new LocationStayEnricher(repository.getConnection)))
        .via(Pipeline.enricherToFlow(new LocationEventEnricher(repository.getConnection)))
        .via(Pipeline.enricherToFlow(new AgentIdentityResolutionEnricher(repository.getConnection)))
    )

    args.map(x => FileSynchronizer.Config(new File(x))).foreach {
      config => pipeline.addSource(config)
    }
  }

  def setupSynchronizers() = {
    FileSynchronizer.registerExtension("json", "application/json")
    FileSynchronizer.registerConverter("application/json", new GoogleLocationHistoryConverter(_))
  }
}
