package thymeflow.rdf

import java.io.File
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.{RDF, RDFS}
import org.openrdf.repository.sail.SailRepository
import org.openrdf.repository.{Repository, RepositoryConnection}
import org.openrdf.rio.RDFFormat
import org.openrdf.sail.NotifyingSail
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer
import org.openrdf.sail.lucene.LuceneSail
import org.openrdf.sail.lucene4.LuceneIndex
import org.openrdf.sail.memory.{MemoryStore, SimpleMemoryStore}
import org.openrdf.{IsolationLevel, IsolationLevels}
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.rdf.sail.inferencer.ForwardChainingSimpleOWLInferencer

import scala.concurrent.duration.Duration

/**
  * @author Thomas Pellissier Tanon
  */
object RepositoryFactory extends StrictLogging {

  private val storePersistenceSyncDelay = 1000

  def initializedMemoryRepository(
                                   snapshotCleanupStore: Boolean = true,
                                   owlInference: Boolean = true,
                                   lucene: Boolean = true,
                                   persistenceDirectory: Option[File] = None,
                                   isolationLevel: IsolationLevel = IsolationLevels.NONE
                                 ): Repository = {
    val initializationStart = System.currentTimeMillis()
    logger.info("Start initializing memory store")

    val store = if (snapshotCleanupStore) {
      val store = new MemoryStore()
      store.setPersist(persistenceDirectory.isDefined)
      store.setSyncDelay(storePersistenceSyncDelay)
      store
    } else {
      val store = new SimpleMemoryStore()
      store.setPersist(persistenceDirectory.isDefined)
      store.setSyncDelay(storePersistenceSyncDelay)
      store
    }
    store.setDefaultIsolationLevel(isolationLevel)

    val repository = new SailRepository(addLucene(addInferencer(store, owlInference), lucene))
    persistenceDirectory.foreach(repository.setDataDir)
    repository.initialize()

    val repositoryConnection = repository.getConnection
    addNamespacesToRepository(repositoryConnection)
    loadOntology(repositoryConnection)
    repositoryConnection.close()

    logger.info(s"Memory store initialization done in ${Duration(System.currentTimeMillis() - initializationStart, TimeUnit.MILLISECONDS)}")

    repository
  }

  private def addLucene(store: NotifyingSail, withElasticSearch: Boolean): NotifyingSail = {
    if (withElasticSearch) {
      val luceneSail = new LuceneSail()
      luceneSail.setParameter(LuceneSail.INDEX_CLASS_KEY, classOf[LuceneIndex].getName)
      luceneSail.setParameter(LuceneSail.LUCENE_RAMDIR_KEY, "true")
      luceneSail.setBaseSail(store)
      luceneSail
    } else {
      store
    }
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
