package com.thymeflow.api

import java.io.File

import com.thymeflow.Thymeflow
import com.thymeflow.actors._
import com.thymeflow.rdf.FileSynchronization
import com.thymeflow.rdf.model.vocabulary.Personal
import com.thymeflow.rdf.repository.RepositoryFactory
import com.thymeflow.service.{Email, Facebook, Google, Microsoft, File => FileService}
import com.thymeflow.update.{UpdateSailInterceptor, Updater}

import scala.language.postfixOps

/**
  * @author David Montoya
  */
trait MainApiDef extends Api {
  private val sailInterceptor = new UpdateSailInterceptor
  override protected val repository = RepositoryFactory.initializeRepository(Some(sailInterceptor))

  override protected val supervisor = Thymeflow.initialize(repository)
  override protected val services = Vector(Google, Microsoft, Facebook, Email, FileService)

  supervisor.addServices(services)
  sailInterceptor.setUpdater(new Updater(repository.valueFactory, () => repository.newConnection(), supervisor))

  def main(args: Array[String]): Unit = {
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
}

object MainApi extends {
  protected val actorSystemContext = com.thymeflow.actors.actorContext
  protected val config = com.thymeflow.config.application
} with MainApiDef