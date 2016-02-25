package pkb

import java.io.FileOutputStream

import org.openrdf.repository.sail.SailRepository
import org.openrdf.rio.{RDFFormat, Rio}
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer
import org.openrdf.sail.memory.MemoryStore
import pkb.sync.FileSynchronizer

object PKB {
  def main(args: Array[String]) {
    val repository = new SailRepository(new ForwardChainingRDFSInferencer(new MemoryStore()))
    repository.initialize()
    val repositoryConnection = repository.getConnection
    repositoryConnection.add(getClass.getClassLoader.getResource("rdfs-ontology.ttl"), "", RDFFormat.TURTLE)

    val syncronizer = new FileSynchronizer(repositoryConnection.getValueFactory)
    repositoryConnection.add(syncronizer.synchronize(args))

    Rio.write(repositoryConnection.getStatements(null, null, null).asList(), new FileOutputStream("test.ttl"), RDFFormat.TURTLE)
  }
}
