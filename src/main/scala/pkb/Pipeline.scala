package pkb

import java.util

import org.openrdf.model.Statement
import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.repository.RepositoryConnection
import pkb.rdf.model.ModelDiff
import pkb.rdf.model.document.Document
import pkb.sync.Synchronizer

import scala.collection.mutable.ArrayBuffer

/**
  * @author Thomas Pellissier Tanon
  */
class Pipeline(repositoryConnection: RepositoryConnection) {

  private val synchronizers = new ArrayBuffer[Synchronizer]

  def addSynchronizer(synchronizer: Synchronizer): Unit = {
    synchronizers += synchronizer
  }

  def run(numberOfIterations: Int = -1): Unit = {
    for (_ <- 1 to numberOfIterations) {
      val diff = synchronizeRepository
      Thread.sleep(60000) //1 mn
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
    synchronizers.flatMap(synchronizer => synchronizer.synchronize())
  }

  private def addDocumentToRepository(document: Document, diff: ModelDiff): Unit = {
    repositoryConnection.begin()

    val statements = new util.HashSet[Statement](document.model)

    //Removes the removed statements from the repository and the already existing statements from statements
    val cursor = repositoryConnection.getStatements(null, null, null, document.iri)
    while (cursor.hasNext) {
      val existingStatement = cursor.next()
      if (document.model.contains(existingStatement)) {
        statements.remove(existingStatement)
      } else {
        repositoryConnection.remove(existingStatement)
        diff.removed.add(existingStatement)
      }
    }
    cursor.close()

    //Add the new statements
    repositoryConnection.add(statements)
    diff.added.addAll(statements)

    repositoryConnection.commit()
  }
}
