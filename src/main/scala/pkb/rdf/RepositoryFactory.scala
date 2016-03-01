package pkb.rdf

import org.openrdf.model.vocabulary.{RDF, RDFS}
import org.openrdf.repository.sail.SailRepository
import org.openrdf.repository.{Repository, RepositoryConnection}
import org.openrdf.sail.memory.MemoryStore
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}

/**
  * @author Thomas Pellissier Tanon
  */
object RepositoryFactory {

  def initializedMemoryRepository: Repository = {
    val repository = new SailRepository(new MemoryStore())
    repository.initialize()
    addNamespacesToRepository(repository.getConnection)
    repository
  }

  private def addNamespacesToRepository(repositoryConnection: RepositoryConnection): Unit = {
    repositoryConnection.begin()
    repositoryConnection.setNamespace(RDF.PREFIX, RDF.NAMESPACE)
    repositoryConnection.setNamespace(RDFS.PREFIX, RDFS.NAMESPACE)
    repositoryConnection.setNamespace(SchemaOrg.PREFIX, SchemaOrg.NAMESPACE)
    repositoryConnection.setNamespace(Personal.PREFIX, Personal.NAMESPACE)
    repositoryConnection.commit()
  }
}
