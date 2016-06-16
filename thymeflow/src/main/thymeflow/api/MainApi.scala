package thymeflow.api


import java.io.File

import thymeflow.Thymeflow
import thymeflow.rdf.model.vocabulary.Personal
import thymeflow.rdf.{FileSynchronization, RepositoryFactory}

import scala.language.postfixOps

/**
  * @author David Montoya
  */
object MainApi extends Api {

  override protected lazy val repository = if (config.getBoolean("thymeflow.api.repository.disk")) {
    RepositoryFactory.initializedDiskRepository(
      dataDirectory = new File(config.getString("thymeflow.api.repository.data-directory")),
      fullTextSearch = config.getBoolean("thymeflow.api.repository.full-text-search"),
      owlInference = config.getBoolean("thymeflow.api.repository.owl-inference")
    )
  } else {
    RepositoryFactory.initializedMemoryRepository(
      dataDirectory = new File(config.getString("thymeflow.api.repository.data-directory")),
      persistToDisk = config.getBoolean("thymeflow.api.repository.persist-to-disk"),
      fullTextSearch = config.getBoolean("thymeflow.api.repository.full-text-search"),
      snapshotCleanupStore = config.getBoolean("thymeflow.api.repository.snapshot-cleanup-store"),
      owlInference = config.getBoolean("thymeflow.api.repository.owl-inference")
    )
  }
  override protected lazy val pipeline = Thymeflow.initializePipeline(repository)
  protected val config = thymeflow.config.default

  if (args.length < 1) {
    logger.info("No file for user graph provided")
  } else {
    val file = new File(args(0))
    if (file.exists() && !file.isFile) {
      logger.warn(s"$file is not a valid file")
    } else {
      val fileSync = FileSynchronization(
        repository.getConnection,
        new File(args(0)),
        repository.getValueFactory.createIRI(Personal.NAMESPACE, "userData")
      )
      if (file.exists()) {
        logger.info(s"Loading user graph from file $file")
        fileSync.load()
      }
      logger.info(s"The user graph will be saved on close to file $file")
      fileSync.saveOnJvmClose()
    }
  }
}
