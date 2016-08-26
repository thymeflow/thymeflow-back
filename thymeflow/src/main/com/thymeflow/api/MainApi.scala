package com.thymeflow.api

import java.io.File

import com.thymeflow.Thymeflow
import com.thymeflow.rdf.model.vocabulary.Personal
import com.thymeflow.rdf.{FileSynchronization, RepositoryFactory}
import com.thymeflow.update.{UpdateSailInterceptor, Updater}

import scala.language.postfixOps

/**
  * @author David Montoya
  */
object MainApi extends Api {
  override protected implicit val config = com.thymeflow.config.application

  private val sailInterceptor = new UpdateSailInterceptor
  override protected val repository = RepositoryFactory.initializeRepository(Some(sailInterceptor))

  override protected val pipeline = Thymeflow.initializePipeline(repository)
  sailInterceptor.setUpdater(new Updater(repository.newConnection(), pipeline))

  if (args.length < 1) {
    logger.info("No file for user graph provided")
  } else {
    val file = new File(args(0))
    if (file.exists() && !file.isFile) {
      logger.warn(s"$file is not a valid file")
    } else {
      val fileSync = FileSynchronization(
        repository.newConnection(),
        new File(args(0)),
        repository.valueFactory.createIRI(Personal.NAMESPACE, "userData")
      )
      if (file.exists()) {
        logger.info(s"Loading user graph from file $file")
        fileSync.load()
      }
      logger.info(s"The user graph will be saved on close to file $file")
      fileSync.saveOnJvmClose()
    }
  }

  start()
}
