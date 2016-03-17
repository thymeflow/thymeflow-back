package thymeflow

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.sail.memory.model.MemValueFactory
import pkb.Pipeline
import pkb.rdf.RepositoryFactory
import pkb.sync.FileSynchronizer
import thymeflow.enricher.AgentIdentityResolutionEnricher

/**
  * @author Thomas Pellissier Tanon
  */
object Thymeflow extends StrictLogging {
  def main(args: Array[String]) {
    val repositoryConnection = RepositoryFactory.initializedMemoryRepository.getConnection

    val pipeline = new Pipeline(repositoryConnection)
    val memValueFactory = new MemValueFactory
    pipeline.addSynchronizer(new FileSynchronizer(memValueFactory, args))
    pipeline.run(1)

    new AgentIdentityResolutionEnricher(repositoryConnection).enrich()
  }
}
