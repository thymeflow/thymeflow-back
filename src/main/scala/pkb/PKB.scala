package pkb

import java.io.FileOutputStream

import org.openrdf.rio.{RDFFormat, Rio}
import org.openrdf.sail.memory.model.MemValueFactory
import pkb.rdf.RepositoryFactory
import pkb.sync.FileSynchronizer

/**
  * @author Thomas Pellissier Tanon
  */
object PKB {
  def main(args: Array[String]) {
    val repositoryConnection = RepositoryFactory.initializedMemoryRepository.getConnection

    val pipeline = new Pipeline(repositoryConnection)
    val memValueFactory = new MemValueFactory
    pipeline.addSynchronizer(new FileSynchronizer(memValueFactory, args))
    pipeline.run(1)

    Rio.write(repositoryConnection.getStatements(null, null, null).asList(), new FileOutputStream("test.ttl"), RDFFormat.TRIG)
  }
}
