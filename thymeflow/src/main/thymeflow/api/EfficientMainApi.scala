package thymeflow.api


import java.io.File

import org.openrdf.IsolationLevels
import thymeflow.enricher._
import thymeflow.rdf.model.vocabulary.Personal
import thymeflow.rdf.{FileSynchronization, RepositoryFactory}
import thymeflow.spatial.geocoding.Geocoder
import thymeflow.{Pipeline, Thymeflow}

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author David Montoya
  */
object EfficientMainApi extends Api {

  override protected val repository = RepositoryFactory.initializedMemoryRepository(
    persistenceDirectory = Some(new File(System.getProperty("java.io.tmpdir") + "/thymeflow/efficient-main-api-memory")),
    snapshotCleanupStore = false,
    owlInference = false,
    lucene = true,
    isolationLevel = IsolationLevels.NONE
    //persistenceDirectory = Some(new File(System.getProperty("java.io.tmpdir") + "/thymeflow/sesame-memory"))
  )

  override protected val pipeline = {
    val geocoder = Geocoder.cached(
      Geocoder.googleMaps(),
      Some(new File(System.getProperty("java.io.tmpdir") + "/thymeflow/geocoder-google-cache"))
    )

    Thymeflow.setupSynchronizers()
    new Pipeline(
      repository.getConnection,
      Pipeline.enricherToFlow(new InverseFunctionalPropertyInferencer(repository.getConnection))
        .via(Pipeline.enricherToFlow(new PlacesGeocoderEnricher(repository.getConnection, geocoder)))
        .via(Pipeline.delayedBatchToFlow(10 seconds))
        .via(Pipeline.enricherToFlow(new LocationStayEnricher(repository.getConnection)))
        .via(Pipeline.enricherToFlow(new LocationEventEnricher(repository.getConnection)))
        .via(Pipeline.enricherToFlow(new EventsWithStaysGeocoderEnricher(repository.getConnection, geocoder)))
        .via(Pipeline.enricherToFlow(new AgentAttributeIdentityResolutionEnricher(repository.getConnection)))
        .via(Pipeline.enricherToFlow(new PrimaryFacetEnricher(repository.getConnection)))
        .map(diff => {
          logger.info(s"A diff went at the end of the pipeline with ${diff.added.size()} additions and ${diff.removed.size()} deletions at time $durationSinceStart")
          diff
        })
    )
  }

  if (args.length < 1) {
    logger.info("No file for user graph provided")
  } else {
    val file = new File(args(0))
    if (file.exists() && !file.isFile) {
      logger.warn(s"$file is not a valid file")
    } else {
      val fileSync = FileSynchronization(
        repository.getConnection,
        new File(args(0)),
        repository.getValueFactory.createIRI(Personal.NAMESPACE, "userData")
      )
      if (file.exists()) {
        logger.info(s"Loading user graph from file $file")
        fileSync.load()
      }
      logger.info(s"The user graph will be saved on close to file $file")
      fileSync.saveOnJvmClose()
    }
  }
}
