package thymeflow.api


import org.openrdf.IsolationLevels
import thymeflow.enricher.{AgentAttributeIdentityResolutionEnricher, LocationStayEnricher}
import thymeflow.rdf.RepositoryFactory
import thymeflow.{Pipeline, Thymeflow}

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author David Montoya
  */
object MainApi extends thymeflow.api.Api {

  override protected val repository = RepositoryFactory.initializedMemoryRepository(
    snapshotCleanupStore = false,
    owlInference = false,
    lucene = false,
    isolationLevel = IsolationLevels.NONE)

  override protected val pipeline = {
    Thymeflow.setupSynchronizers()
    new Pipeline(repository.getConnection, List(
      new LocationStayEnricher(repository.getConnection, 10 seconds),
      new AgentAttributeIdentityResolutionEnricher(repository.getConnection, 10 seconds)))
  }

}
