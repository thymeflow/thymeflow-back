package thymeflow.rdf

import java.io.File
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.{RDF, RDFS}
import org.openrdf.query.algebra.evaluation.function.FunctionRegistry
import org.openrdf.repository.sail.SailRepository
import org.openrdf.repository.{Repository, RepositoryConnection}
import org.openrdf.rio.RDFFormat
import org.openrdf.sail.NotifyingSail
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer
import org.openrdf.sail.lucene.LuceneSail
import org.openrdf.sail.lucene4.LuceneIndex
import org.openrdf.sail.memory.{MemoryStore, SimpleMemoryStore}
import org.openrdf.sail.nativerdf.NativeStore
import org.openrdf.{IsolationLevel, IsolationLevels}
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.rdf.query.algebra.evaluation.function
import thymeflow.rdf.sail.inferencer.ForwardChainingSimpleOWLInferencer
import thymeflow.rdf.sail.{InterceptingSail, SailInterceptor}

import scala.concurrent.duration.Duration

/**
  * @author Thomas Pellissier Tanon
  */
object RepositoryFactory extends StrictLogging {

  def initializedMemoryRepository(snapshotCleanupStore: Boolean = true,
                                  owlInference: Boolean = true,
                                  fullTextSearch: Boolean = true,
                                  dataDirectory: File,
                                  persistToDisk: Boolean = false,
                                  persistenceSyncDelay: Long = 1000,
                                  isolationLevel: IsolationLevel = IsolationLevels.NONE,
                                  sailInterceptor: Option[SailInterceptor] = None): Repository = {
    val initializationStart = System.currentTimeMillis()
    logger.info("Start initializing memory store")

    val store = if (snapshotCleanupStore) {
      val store = new MemoryStore()
      store.setPersist(persistToDisk)
      store.setSyncDelay(persistenceSyncDelay)
      store
    } else {
      val store = new SimpleMemoryStore()
      store.setPersist(persistToDisk)
      store.setSyncDelay(persistenceSyncDelay)
      store
    }
    store.setDefaultIsolationLevel(isolationLevel)

    val repository = initializeRepository(store, owlInference, fullTextSearch, dataDirectory, sailInterceptor)

    logger.info(s"Memory store initialization done in ${Duration(System.currentTimeMillis() - initializationStart, TimeUnit.MILLISECONDS)}")

    repository
  }

  def initializedDiskRepository(owlInference: Boolean = true,
                                fullTextSearch: Boolean = true,
                                dataDirectory: File,
                                isolationLevel: IsolationLevel = IsolationLevels.NONE,
                                sailInterceptor: Option[SailInterceptor] = None): Repository = {
    val initializationStart = System.currentTimeMillis()
    logger.info("Start initializing memory store")

    val store = new NativeStore()
    store.setDefaultIsolationLevel(isolationLevel)

    val repository = initializeRepository(store, owlInference, fullTextSearch, dataDirectory, sailInterceptor)

    logger.info(s"Disk store initialization done in ${Duration(System.currentTimeMillis() - initializationStart, TimeUnit.MILLISECONDS)}")

    repository
  }

  private def initializeRepository(store: NotifyingSail,
                                   owlInference: Boolean = true,
                                   fullTextSearch: Boolean = true,
                                   dataDirectory: File,
                                   sailInterceptor: Option[SailInterceptor] = None): Repository = {

    val repository = new SailRepository(addSailInterceptor(addFullTextSearch(addInferencer(store, owlInference), fullTextSearch), sailInterceptor))
    repository.setDataDir(dataDirectory)
    repository.initialize()
    addBasicsToRepository(repository)
    repository
  }

  private def addFullTextSearch(store: NotifyingSail, withFullTextSearch: Boolean): NotifyingSail = {
    if (withFullTextSearch) {
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

  private def addSailInterceptor(store: NotifyingSail, sailInterceptor: Option[SailInterceptor] = None): NotifyingSail = {
    sailInterceptor.map(sailInterceptor => new InterceptingSail(store, sailInterceptor)).getOrElse(store)
  }


  private def addBasicsToRepository(repository: Repository): Unit = {
    val repositoryConnection = repository.getConnection
    addNamespacesToRepository(repositoryConnection)
    loadOntology(repositoryConnection)
    repositoryConnection.close()
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

  //Register extra SPARQL functions
  {
    FunctionRegistry.getInstance().add(new function.Duration)
    FunctionRegistry.getInstance().add(new function.DurationInMillis)
  }
}
