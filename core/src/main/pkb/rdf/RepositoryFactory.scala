package pkb.rdf

import org.openrdf.IsolationLevels
import org.openrdf.model.vocabulary.{RDF, RDFS}
import org.openrdf.repository.sail.SailRepository
import org.openrdf.repository.{Repository, RepositoryConnection}
import org.openrdf.rio.RDFFormat
import org.openrdf.sail.NotifyingSail
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer
import org.openrdf.sail.memory.{MemoryStore, SimpleMemoryStore}
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}
import pkb.rdf.sail.inferencer.ForwardChainingSimpleOWLInferencer

/**
  * @author Thomas Pellissier Tanon
  */
object RepositoryFactory {

  def initializedMemoryRepository(snapshotCleanupStore: Boolean = true, owlInference: Boolean = true): Repository = {
    val store = if (snapshotCleanupStore) {
      new MemoryStore()
    } else {
      new SimpleMemoryStore()
    }
    store.setDefaultIsolationLevel(IsolationLevels.NONE)

    val repository = new SailRepository(addInferencer(store, owlInference))

    repository.initialize()

    val repositoryConnection = repository.getConnection
    addNamespacesToRepository(repositoryConnection)
    loadOntology(repositoryConnection)
    repositoryConnection.close()

    repository
  }

  private def addInferencer(store: NotifyingSail, withOwl: Boolean): NotifyingSail = {
    if (withOwl) {
      new ForwardChainingSimpleOWLInferencer(new ForwardChainingRDFSInferencer(store))
    } else {
      store
    }
  }

  private def addNamespacesToRepository(repositoryConnection: RepositoryConnection): Unit = {
    repositoryConnection.begin()
    repositoryConnection.setNamespace(RDF.PREFIX, RDF.NAMESPACE)
    repositoryConnection.setNamespace(RDFS.PREFIX, RDFS.NAMESPACE)
    repositoryConnection.setNamespace(SchemaOrg.PREFIX, SchemaOrg.NAMESPACE)
    repositoryConnection.setNamespace(Personal.PREFIX, Personal.NAMESPACE)
    repositoryConnection.commit()
  }

  private def loadOntology(repositoryConnection: RepositoryConnection): Unit = {
    repositoryConnection.add(
      getClass.getClassLoader.getResourceAsStream("rdfs-ontology.ttl"),
      Personal.NAMESPACE,
      RDFFormat.TURTLE
    )
  }
}
