package thymeflow.api


import org.openrdf.IsolationLevels
import pkb.Pipeline
import pkb.rdf.RepositoryFactory
import thymeflow.Thymeflow
import thymeflow.enricher.{AgentIdentityResolutionEnricher, LocationStayEnricher}

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author David Montoya
  */
object MainApi extends pkb.api.Api {

  override protected val repository = RepositoryFactory.initializedMemoryRepository(
    snapshotCleanupStore = false,
    owlInference = false,
    lucene = false,
    isolationLevel = IsolationLevels.NONE)

  override protected val pipeline = {
    Thymeflow.setupSynchronizers()
    new Pipeline(repository.getConnection, List(
      new LocationStayEnricher(repository.getConnection, 10 seconds),
      new AgentIdentityResolutionEnricher(repository.getConnection, 10 seconds)))
  }

}
