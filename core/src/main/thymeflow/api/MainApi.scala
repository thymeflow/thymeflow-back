package thymeflow.api

import java.io.File

import org.openrdf.IsolationLevels
import thymeflow.Pipeline
import thymeflow.enricher.{InverseFunctionalPropertyInferencer, PrimaryFacetEnricher}
import thymeflow.rdf.RepositoryFactory

/**
  * @author David Montoya
  */
object MainApi extends Api {

  override protected val repository = RepositoryFactory.initializedMemoryRepository(
    persistenceDirectory = Some(new File(System.getProperty("java.io.tmpdir") + "/thymeflow/sesame-memory")),
    isolationLevel = IsolationLevels.NONE
  )

  //TODO: should be in configuration
  override protected val pipeline = new Pipeline(
    repository.getConnection,
    Pipeline.enricherToFlow(new InverseFunctionalPropertyInferencer(repository.getConnection))
      .via(Pipeline.enricherToFlow(new PrimaryFacetEnricher(repository.getConnection)))
  )

}
