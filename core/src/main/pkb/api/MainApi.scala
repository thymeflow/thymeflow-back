package pkb.api

import java.io.File

import org.openrdf.IsolationLevels
import pkb.Pipeline
import pkb.enricher.{InverseFunctionalPropertyInferencer, PrimaryFacetEnricher}
import pkb.rdf.RepositoryFactory

/**
  * @author David Montoya
  */
object MainApi extends Api {

  override protected val repository = RepositoryFactory.initializedMemoryRepository(
    persistenceDirectory = Some(new File(System.getProperty("java.io.tmpdir") + "/pkb/sesame-memory")),
    isolationLevel = IsolationLevels.NONE
  )

  //TODO: should be in configuration
  override protected val pipeline = new Pipeline(
    repository.getConnection,
    List(new InverseFunctionalPropertyInferencer(repository.getConnection), new PrimaryFacetEnricher(repository.getConnection))
  )

}
