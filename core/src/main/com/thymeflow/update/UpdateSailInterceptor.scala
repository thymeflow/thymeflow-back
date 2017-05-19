package com.thymeflow.update

import com.thymeflow.rdf.model.{StatementSet, StatementSetDiff}
import com.thymeflow.rdf.sail.SailInterceptor
import com.thymeflow.utilities.Error
import com.typesafe.scalalogging.StrictLogging
import org.eclipse.rdf4j.model._

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * @author Thomas Pellissier Tanon
  */
class UpdateSailInterceptor extends SailInterceptor with StrictLogging {
  var updater: Updater = null

  //TODO: better way to inject the Updater?
  def setUpdater(updater: Updater): Unit = {
    this.updater = updater
  }

  /**
    * Method called when an SPARQL UPDATE is tried on the sail
    */
  override def onSparqlUpdateDiff(diff: StatementSetDiff) {
    val updateResults = updater.apply(diff)
    updateResults.foreach(results => {
      results.added.foreach {
        case (statement, Error(l)) if l.nonEmpty && statement.getContext == null => logger.info(s"Error(s) encountered when propagating $statement. The statement was added to the User Graph. (${l.mkString}) ")
        case (statement, Error(l)) if l.nonEmpty && statement.getContext != null => logger.info(s"Error(s) encountered when propagating $statement: ${l.mkString}")
        case _ =>
      }
      results.removed.foreach {
        case (statement, Error(l)) if l.nonEmpty && statement.getContext == null => logger.info(s"Error(s) encountered when propagating the removal of $statement. The statement was however removed and a negated version was added to the User Graph. (${l.mkString}) ")
        case (statement, Error(l)) if l.nonEmpty && statement.getContext != null => logger.info(s"Error(s) encountered when propagating the removal of $statement: ${l.mkString}")
        case _ =>
      }
    })
    updateResults.onFailure {
      case t => logger.error(s"Error applying update ($diff): ${t.getMessage}", t)
    }
  }
}
