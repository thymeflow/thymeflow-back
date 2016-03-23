package pkb

import java.io.File

import com.typesafe.scalalogging.StrictLogging
import pkb.rdf.RepositoryFactory
import pkb.sync.FileSynchronizer

/**
  * @author Thomas Pellissier Tanon
  */
object PKB extends StrictLogging {
  def main(args: Array[String]) {
    val repositoryConnection = RepositoryFactory.initializedMemoryRepository().getConnection

    val pipeline = new Pipeline(repositoryConnection, List())
    args.foreach(fileName => pipeline.addSource(FileSynchronizer.Config(new File(fileName))))
  }
}
