package pkb

import java.util

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.Statement
import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.repository.RepositoryConnection
import pkb.inferencer.Inferencer
import pkb.rdf.Converters._
import pkb.rdf.model.ModelDiff
import pkb.rdf.model.document.Document
import pkb.sync.Synchronizer

import scala.collection.mutable.ArrayBuffer

/**
  * @author Thomas Pellissier Tanon
  */
class Pipeline(repositoryConnection: RepositoryConnection) extends StrictLogging {

  private val synchronizers = new ArrayBuffer[Synchronizer]

  private val inferencers = new ArrayBuffer[Inferencer]

  def addSynchronizer(synchronizer: Synchronizer): Unit = {
    synchronizers += synchronizer
  }

  def addInferencer(inferencer: Inferencer): Unit = {
    inferencers += inferencer
  }

  def run(numberOfIterations: Int = -1): Unit = {
    for (i <- 1 to numberOfIterations) {
      logger.info(s"Executing pipeline iteration {iteration=$i}")
      logger.info(s"Synchronizing repository {iteration=$i}")
      val diff = synchronizeRepository
      logger.info(s"Performing inference {iteration=$i}")
      doInference(diff)
      logger.info(s"Pipeline iteration done {iteration=$i}")
      if (i < numberOfIterations) {
        Thread.sleep(60000) //1 mn
      }
    }
  }

  private def synchronizeRepository: ModelDiff = {
    val diff = new ModelDiff(new LinkedHashModel(), new LinkedHashModel())

    newDocuments.foreach(document => {
      addDocumentToRepository(document, diff)
    })

    diff
  }

  private def newDocuments: Traversable[Document] = {
    logger.info("Getting new documents...")
    val result = synchronizers.flatMap(_.synchronize())
    logger.info("Done getting new documents.")
    result
  }

  private def addDocumentToRepository(document: Document, diff: ModelDiff): Unit = {
    repositoryConnection.begin()
    //Removes the removed statements from the repository and the already existing statements from statements
    val statements = new util.HashSet[Statement](document.model)
    val statementsToRemove = new util.HashSet[Statement]()

    repositoryConnection.getStatements(null, null, null, document.iri).foreach(existingStatement =>
      if (document.model.contains(existingStatement)) {
        statements.remove(existingStatement)
      } else {
        statementsToRemove.add(existingStatement)
      }
    )

    diff.removed.addAll(statementsToRemove)
    diff.added.addAll(statements)
    repositoryConnection.remove(statementsToRemove)
    repositoryConnection.add(statements)
    repositoryConnection.commit()
  }

  private def doInference(diff: ModelDiff): Unit = {
    inferencers.foreach(_.infer(diff))
  }
}
