package thymeflow.sync

import akka.stream.actor.ActorPublisher
import thymeflow.rdf.model.ModelDiff
import thymeflow.rdf.model.document.Document

/**
  * @author Thomas Pellissier Tanon
  */
trait Synchronizer {

  protected trait BasePublisher extends ActorPublisher[Document]

}

object Synchronizer {

  case class Update(diff: ModelDiff)

}