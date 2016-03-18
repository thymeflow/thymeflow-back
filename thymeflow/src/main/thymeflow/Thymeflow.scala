package thymeflow

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.StrictLogging
import pkb.Pipeline
import pkb.rdf.RepositoryFactory
import pkb.sync.FileSynchronizer
import thymeflow.enricher.AgentIdentityResolutionEnricher

import scala.concurrent.duration._

/**
  * @author Thomas Pellissier Tanon
  */
object Thymeflow extends StrictLogging {
  def main(args: Array[String]) {
    val repositoryConnection = RepositoryFactory.initializedMemoryRepository.getConnection

    import scala.concurrent.ExecutionContext.Implicits.global
    val pipeline = new Pipeline(repositoryConnection, List(new AgentIdentityResolutionEnricher(repositoryConnection, Duration(10, TimeUnit.SECONDS))))
    pipeline.addSource(FileSynchronizer.Config(args))


  }
}
