package pkb

import java.io.FileOutputStream

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.rio.{RDFFormat, Rio}
import org.openrdf.sail.memory.model.MemValueFactory
import pkb.rdf.RepositoryFactory
import pkb.sync.FileSynchronizer

/**
  * @author Thomas Pellissier Tanon
  */
object PKB extends StrictLogging {
  def main(args: Array[String]) {
    val repositoryConnection = RepositoryFactory.initializedMemoryRepository.getConnection

    val pipeline = new Pipeline(repositoryConnection)
    val memValueFactory = new MemValueFactory
    pipeline.addSynchronizer(new FileSynchronizer(memValueFactory, args))
    pipeline.run(1)

    val outputFile = "test.ttl"
    logger.info(s"Writing output to file $outputFile...")
    Rio.write(repositoryConnection.getStatements(null, null, null).asList(), new FileOutputStream(outputFile), RDFFormat.TRIG)
    logger.info(s"Done writing output to file $outputFile.")
  }
}
