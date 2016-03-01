package pkb

import java.io.FileOutputStream

import org.openrdf.rio.{RDFFormat, Rio}
import pkb.rdf.RepositoryFactory
import pkb.sync.FileSynchronizer

/**
  * @author Thomas Pellissier Tanon
  */
object PKB {
  def main(args: Array[String]) {
    val repositoryConnection = RepositoryFactory.initializedMemoryRepository.getConnection

    val syncronizer = new FileSynchronizer(repositoryConnection.getValueFactory)
    repositoryConnection.add(syncronizer.synchronize(args))

    Rio.write(repositoryConnection.getStatements(null, null, null).asList(), new FileOutputStream("test.ttl"), RDFFormat.TURTLE)
  }
}
