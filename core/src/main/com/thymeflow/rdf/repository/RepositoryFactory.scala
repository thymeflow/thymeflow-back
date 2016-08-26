package com.thymeflow.rdf.repository

import java.nio.file.{Path, Paths}

import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.SimpleHashModel
import com.thymeflow.rdf.model.vocabulary.Personal
import com.thymeflow.rdf.query.algebra.evaluation.function
import com.thymeflow.rdf.sail.inferencer.ForwardChainingSimpleOWLInferencer
import com.thymeflow.rdf.sail.{InterceptingSail, SailInterceptor}
import com.thymeflow.utilities.TimeExecution
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.Statement
import org.openrdf.query.algebra.evaluation.function.FunctionRegistry
import org.openrdf.repository.RepositoryConnection
import org.openrdf.repository.sail.SailRepository
import org.openrdf.repository.util.RDFLoader
import org.openrdf.rio.{RDFFormat, RDFHandler}
import org.openrdf.sail.helpers.AbstractNotifyingSail
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer
import org.openrdf.sail.lucene.LuceneSail
import org.openrdf.sail.lucene4.LuceneIndex
import org.openrdf.sail.memory.{MemoryStore, SimpleMemoryStore}
import org.openrdf.sail.nativerdf.NativeStore
import org.openrdf.sail.{NotifyingSail, Sail}
import org.openrdf.{IsolationLevel, IsolationLevels}

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
object RepositoryFactory extends StrictLogging {

  private sealed trait RepositoryConfig {
    def dataDirectory: Path

    def isolationLevel: IsolationLevel
  }

  private case class SailRepositoryConfig(sailConfig: SailConfig,
                                          dataDirectory: Path,
                                          isolationLevel: IsolationLevel) extends RepositoryConfig

  private case class SailConfig(baseNotifyingSail: NotifyingSailConfig,
                                owlInference: Boolean = true,
                                fullTextSearch: Boolean = true)

  sealed trait NotifyingSailConfig

  sealed trait StoreConfig extends NotifyingSailConfig

  private case class MemoryStoreConfig(persistToDisk: Boolean = false,
                                       persistenceSyncDelay: Long = 1000,
                                       snapshotCleanupStore: Boolean = true) extends StoreConfig

  private case class DiskStoreConfig() extends StoreConfig

  private def initializeMemoryStore(config: MemoryStoreConfig): AbstractNotifyingSail = {
    val store = if (config.snapshotCleanupStore) {
      val store = new MemoryStore()
      store.setPersist(config.persistToDisk)
      store.setSyncDelay(config.persistenceSyncDelay)
      store
    } else {
      val store = new SimpleMemoryStore()
      store.setPersist(config.persistToDisk)
      store.setSyncDelay(config.persistenceSyncDelay)
      store
    }
    store
  }

  private def initializeDiskStore(config: DiskStoreConfig): AbstractNotifyingSail = {
    new NativeStore()
  }

  private def initializeStore(config: StoreConfig): NotifyingSail = {
    val store = config match {
      case storeConfig: MemoryStoreConfig => initializeMemoryStore(storeConfig)
      case storeConfig: DiskStoreConfig => initializeDiskStore(storeConfig)
    }
    store
  }

  private def initializeNotifyingSail(config: NotifyingSailConfig): NotifyingSail = {
    config match {
      case storeConfig: StoreConfig => initializeStore(storeConfig)
    }
  }

  private def initializeSail(sailConfig: SailConfig, sailInterceptor: Option[SailInterceptor] = None): Sail = {
    val baseNotifyingSail = initializeNotifyingSail(sailConfig.baseNotifyingSail)
    addSailInterceptor(addFullTextSearch(addInferencer(baseNotifyingSail, sailConfig.owlInference), sailConfig.fullTextSearch), sailInterceptor)
  }

  private def getRepositoryConfig(config: Config): RepositoryConfig = {
    def configKey(attributeName: String) = {
      s"thymeflow.repository.$attributeName"
    }
    val repositoryType = config.getString(configKey("type"))
    val baseNotifyingSailConfig = repositoryType match {
      case "disk" =>
        DiskStoreConfig()
      case "memory" =>
        MemoryStoreConfig(persistToDisk = config.getBoolean(configKey("persist-to-disk")),
          persistenceSyncDelay = config.getLong(configKey("persistence-sync-delay")),
          snapshotCleanupStore = config.getBoolean(configKey("snapshot-cleanup-store"))
        )
      case _ => throw new IllegalArgumentException(s"Unknown repository type: $repositoryType")
    }
    val sailConfig = SailConfig(baseNotifyingSail = baseNotifyingSailConfig,
      owlInference = config.getBoolean(configKey("owl-inference")),
      fullTextSearch = config.getBoolean(configKey("full-text-search"))
    )
    val dataDirectoryPath = if (config.hasPath(configKey("data-directory"))) {
      logger.warn("Data directory using `thymeflow.repository.data-directory` is DEPRECATED. Refer to reference.conf for the new definition and migration instructions.")
      Paths.get(config.getString(configKey("data-directory")))
    } else {
      Paths.get(config.getString("thymeflow.data-directory"), repositoryType)
    }
    SailRepositoryConfig(sailConfig = sailConfig,
      dataDirectory = dataDirectoryPath,
      isolationLevel = IsolationLevels.valueOf(config.getString(configKey("isolation-level")))
    )
  }

  /**
    * Initializes the Repository
    *
    * @param sailInterceptor an interceptor of SPARQL INSERT/DELETE commands
    * @param config          the application config
    * @return
    */
  // TODO: Improve the inclusion of the sailInterceptor
  def initializeRepository(sailInterceptor: Option[SailInterceptor] = None)(implicit config: Config): Repository = {
    val repositoryConfig = getRepositoryConfig(config)
    TimeExecution.timeInfo("repository-initialization", logger, {
      repositoryConfig match {
        case sailRepositoryConfig: SailRepositoryConfig =>
          val sail = initializeSail(sailRepositoryConfig.sailConfig, sailInterceptor)
          val sailRepository = new SailRepository(sail)
          sailRepository.setDataDir(sailRepositoryConfig.dataDirectory.toFile)
          sailRepository.initialize()
          val repository = RepositoryWithDefaultIsolationLevel(sailRepository, sailRepositoryConfig.isolationLevel)
          addBasicsToRepository(repository)
          repository
      }
    }, s"Config=$repositoryConfig")
  }

  private def addFullTextSearch(notifyingSail: NotifyingSail, withFullTextSearch: Boolean): NotifyingSail = {
    if (withFullTextSearch) {
      val luceneSail = new LuceneSail()
      luceneSail.setParameter(LuceneSail.INDEX_CLASS_KEY, classOf[LuceneIndex].getName)
      luceneSail.setParameter(LuceneSail.LUCENE_RAMDIR_KEY, "true")
      luceneSail.setBaseSail(notifyingSail)
      luceneSail
    } else {
      notifyingSail
    }
  }

  private def addInferencer(notifyingSail: NotifyingSail, withOwl: Boolean): NotifyingSail = {
    if (withOwl) {
      new ForwardChainingSimpleOWLInferencer(new ForwardChainingRDFSInferencer(notifyingSail))
    } else {
      notifyingSail
    }
  }

  private def addSailInterceptor(notifyingSail: NotifyingSail, sailInterceptor: Option[SailInterceptor] = None): NotifyingSail = {
    sailInterceptor.map(sailInterceptor => new InterceptingSail(notifyingSail, sailInterceptor)).getOrElse(notifyingSail)
  }

  private def addBasicsToRepository(repository: Repository): Unit = {
    val repositoryConnection = repository.newConnection()
    repositoryConnection.begin()
    if (loadOntology(repositoryConnection)) {
      // only commit if changes happened
      repositoryConnection.commit()
    } else {
      repositoryConnection.rollback()
    }
    repositoryConnection.close()
  }

  private def setNamespaceIfChanged(repositoryConnection: RepositoryConnection, prefix: String, name: String): Boolean = {
    val currentName = repositoryConnection.getNamespace(prefix)
    if (currentName != name) {
      repositoryConnection.setNamespace(prefix, name)
      true
    } else {
      false
    }
  }

  private def loadOntology(repositoryConnection: RepositoryConnection): Boolean = {
    val context = Personal.ONTOLOGY_DEFINITION
    val statementsToAdd = new SimpleHashModel(repositoryConnection.getValueFactory)
    val namespacesBuilder = Vector.newBuilder[(String, String)]

    val rdfHandler = new RDFHandler {
      def handleStatement(st: Statement): Unit = {
        statementsToAdd.add(st.getSubject, st.getPredicate, st.getObject, context)
      }

      def handleNamespace(prefix: String, uri: String): Unit = {
        namespacesBuilder += prefix -> uri
      }

      def handleComment(comment: String) = {}

      def startRDF() = {}

      def endRDF() = {}
    }

    val loader = new RDFLoader(repositoryConnection.getParserConfig, repositoryConnection.getValueFactory)
    loader.load(getClass.getClassLoader.getResourceAsStream("rdfs-ontology.ttl"),
      Personal.NAMESPACE,
      RDFFormat.TURTLE,
      rdfHandler)

    val statementsToRemove = new SimpleHashModel(repositoryConnection.getValueFactory)
    repositoryConnection.getStatements(null, null, null, Personal.ONTOLOGY_DEFINITION).foreach(existingStatement =>
      if (statementsToAdd.contains(existingStatement)) {
        statementsToAdd.remove(existingStatement)
      } else {
        statementsToRemove.add(existingStatement)
      }
    )

    val namespacesChanged = namespacesBuilder.result().map {
      case (prefix, name) => setNamespaceIfChanged(repositoryConnection, prefix, name)
    }.count(identity)
    repositoryConnection.add(statementsToAdd)
    repositoryConnection.remove(statementsToRemove)

    logger.info(s"Ontology ${Personal.ONTOLOGY_DEFINITION} loaded: $namespacesChanged namespaces changed, ${statementsToAdd.size()} statement(s) added, ${statementsToRemove.size()} statement(s) removed.")
    !(statementsToAdd.isEmpty && statementsToRemove.isEmpty && namespacesChanged == 0)
  }

  //Register extra SPARQL functions
  {
    FunctionRegistry.getInstance().add(new function.Duration)
    FunctionRegistry.getInstance().add(new function.DurationInMillis)
  }
}
