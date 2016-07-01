package thymeflow.update

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.{IRI, Model, Resource, Value}
import thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import thymeflow.rdf.sail.SailInterceptor
import thymeflow.utilities.{Error, Ok}

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

  override def onSparqlAddStatement(subject: Resource, predicate: IRI, `object`: Value, contexts: Resource*): Boolean = {
    applyUpdate(new ModelDiff(
      modelFromStatement(subject, predicate, `object`, contexts),
      SimpleHashModel.empty
    ))
  }

  override def onSparqlRemoveStatement(subject: Resource, predicate: IRI, `object`: Value, contexts: Resource*): Boolean = {
    applyUpdate(new ModelDiff(
      SimpleHashModel.empty,
      modelFromStatement(subject, predicate, `object`, contexts)
    ))
  }

  private def applyUpdate(diff: ModelDiff): Boolean = {
    updater.apply(diff).foreach(results => {
      results.added.foreach {
        case (statement, Ok(_)) => logger.info(s"statement $statement have been successefully added")
        case (statement, Error(l)) => logger.info(s"statement $statement failed ot be added with errors: ${l.mkString}")
      }
      results.removed.foreach {
        case (statement, Ok(_)) => logger.info(s"statement $statement have been successefully removed")
        case (statement, Error(l)) => logger.info(s"statement $statement failed ot be removed with errors: ${l.mkString}")
      }
    })
    false
  }

  private def modelFromStatement(subject: Resource, predicate: IRI, `object`: Value, contexts: Seq[Resource]): Model = {
    val model = new SimpleHashModel()
    if (contexts.isEmpty) {
      model.add(subject, predicate, `object`)
    } else {
      contexts.map(context =>
        model.add(subject, predicate, `object`, context)
      )
    }
    model
  }
}
