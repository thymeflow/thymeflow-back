package thymeflow.api


import java.io.File

import org.openrdf.IsolationLevels
import thymeflow.enricher.{AgentAttributeIdentityResolutionEnricher, InverseFunctionalPropertyInferencer, LocationEventEnricher, LocationStayEnricher}
import thymeflow.rdf.RepositoryFactory
import thymeflow.{Pipeline, Thymeflow}

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author David Montoya
  */
object EfficientMainApi extends thymeflow.api.Api {

  override protected val repository = RepositoryFactory.initializedMemoryRepository(
    persistenceDirectory = Some(new File(System.getProperty("java.io.tmpdir") + "/thymeflow/efficient-main-api-memory")),
    snapshotCleanupStore = false,
    owlInference = false,
    lucene = false,
    isolationLevel = IsolationLevels.NONE)

  override protected val pipeline = {
    Thymeflow.setupSynchronizers()
    new Pipeline(
      repository.getConnection,
      Pipeline.enricherToFlow(new InverseFunctionalPropertyInferencer(repository.getConnection))
        .via(Pipeline.delayedBatchToFlow(10 seconds))
        .via(Pipeline.enricherToFlow(new LocationStayEnricher(repository.getConnection)))
        .via(Pipeline.enricherToFlow(new LocationEventEnricher(repository.getConnection)))
        .via(Pipeline.enricherToFlow(new AgentAttributeIdentityResolutionEnricher(repository.getConnection)))
    )
  }
}
