package thymeflow

import java.io.File

import com.typesafe.scalalogging.StrictLogging
import pkb.Pipeline
import pkb.rdf.RepositoryFactory
import pkb.sync.FileSynchronizer
import thymeflow.enricher.AgentIdentityResolutionEnricher

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  */
object Thymeflow extends StrictLogging {
  def main(args: Array[String]) {
    val repository = RepositoryFactory.initializedMemoryRepository(snapshotCleanupStore = false, owlInference = false, lucene = false)
    val pipeline = new Pipeline(repository.getConnection, List(new AgentIdentityResolutionEnricher(repository.getConnection, 10 seconds)))
    args.map(x => FileSynchronizer.Config(new File(x))).foreach {
      config => pipeline.addSource(config)
    }

  }
}
