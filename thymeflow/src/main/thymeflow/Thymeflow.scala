package thymeflow

import java.io.File

import com.typesafe.scalalogging.StrictLogging
import pkb.Pipeline
import pkb.rdf.RepositoryFactory
import pkb.sync.FileSynchronizer

/**
  * @author Thomas Pellissier Tanon
  */
object Thymeflow extends StrictLogging {
  def main(args: Array[String]) {
    val repositoryConnection = RepositoryFactory.initializedMemoryRepository.getConnection
    val pipeline = new Pipeline(repositoryConnection, List())
    //List(new AgentIdentityResolutionEnricher(repositoryConnection, Duration(10, TimeUnit.SECONDS)))
    args.map(x => FileSynchronizer.Config(new File(x))).foreach {
      config => pipeline.addSource(config)
    }

  }
}
