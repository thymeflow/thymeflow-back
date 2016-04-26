package thymeflow.enricher

import org.openrdf.model.Statement
import org.openrdf.repository.RepositoryConnection
import thymeflow.rdf.model.ModelDiff

/**
  * @author Thomas Pellissier Tanon
  */
abstract class AbstractEnricher(repositoryConnection: RepositoryConnection) extends Enricher {

  /**
    * Add an inferred statement to the repository if it is not already existing
    */
  protected def addStatement(diff: ModelDiff, statement: Statement): Unit = {
    if (!repositoryConnection.hasStatement(statement, false)) {
      diff.added.add(statement)
      repositoryConnection.add(statement)
    }
  }

  /**
    * Removes an inferred statement from the repository if it is existing
    */
  protected def removeStatement(diff: ModelDiff, statement: Statement): Unit = {
    if (repositoryConnection.hasStatement(statement, false)) {
      diff.removed.add(statement)
      repositoryConnection.remove(statement)
    }
  }
}
