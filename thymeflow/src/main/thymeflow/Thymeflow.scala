package thymeflow

import java.io.File

import com.typesafe.scalalogging.StrictLogging
import thymeflow.enricher.{AgentAttributeIdentityResolutionEnricher, InverseFunctionalPropertyInferencer, LocationStayEnricher, PrimaryFacetEnricher}
import thymeflow.rdf.RepositoryFactory
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
    val pipeline = new Pipeline(repository.getConnection, List(
      new InverseFunctionalPropertyInferencer(repository.getConnection),
      new LocationStayEnricher(repository.getConnection, 10 seconds),
      new AgentAttributeIdentityResolutionEnricher(repository.getConnection, 10 seconds),
      new PrimaryFacetEnricher(repository.getConnection)
    ))
    args.map(x => FileSynchronizer.Config(new File(x))).foreach {
      config => pipeline.addSource(config)
    }

  }

  def setupSynchronizers() = {
    FileSynchronizer.registerExtension("json", "application/json")
    FileSynchronizer.registerConverter("application/json", new GoogleLocationHistoryConverter(_))
  }
}
