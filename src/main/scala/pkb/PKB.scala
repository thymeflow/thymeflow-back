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

    val pipeline = new Pipeline(repositoryConnection)
    pipeline.addSynchronizer(new FileSynchronizer(repositoryConnection.getValueFactory, args))
    pipeline.run(1)

    Rio.write(repositoryConnection.getStatements(null, null, null).asList(), new FileOutputStream("test.ttl"), RDFFormat.TRIG)
  }
}
