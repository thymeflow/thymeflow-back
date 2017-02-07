package com.thymeflow.update

import com.thymeflow.rdf.model.{ModelDiff, StatementSet}
import com.thymeflow.rdf.sail.SailInterceptor
import com.thymeflow.utilities.{Error, Ok}
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

  override def onSparqlAddStatement(subject: Resource, predicate: IRI, `object`: Value, contexts: Resource*)(implicit valueFactory: ValueFactory): Boolean = {
    applyUpdate(new ModelDiff(
      modelFromStatement(subject, predicate, `object`, contexts),
      StatementSet.empty
    ))
  }

  override def onSparqlRemoveStatement(subject: Resource, predicate: IRI, `object`: Value, contexts: Resource*)(implicit valueFactory: ValueFactory): Boolean = {
    applyUpdate(new ModelDiff(
      StatementSet.empty,
      modelFromStatement(subject, predicate, `object`, contexts)
    ))
  }

  private def applyUpdate(diff: ModelDiff): Boolean = {
    updater.apply(diff).foreach(results => {
      results.added.foreach {
        case (statement, Ok(_)) => logger.info(s"statement $statement has been successfully added")
        case (statement, Error(l)) => logger.info(s"statement $statement failed to be added with errors: ${l.mkString}")
      }
      results.removed.foreach {
        case (statement, Ok(_)) => logger.info(s"statement $statement has been successfully removed")
        case (statement, Error(l)) => logger.info(s"statement $statement failed to be removed with errors: ${l.mkString}")
      }
    })
    false
  }

  private def modelFromStatement(subject: Resource, predicate: IRI, `object`: Value, contexts: Seq[Resource])(implicit valueFactory: ValueFactory): StatementSet = {
    val statements = StatementSet.empty
    if (contexts.isEmpty) {
      statements.add(subject, predicate, `object`)
    } else {
      contexts.map(context =>
        statements.add(subject, predicate, `object`, context)
      )
    }
    statements
  }
}
