package pkb.sync

import akka.stream.actor.ActorPublisher
import pkb.rdf.model.document.Document

/**
  * @author Thomas Pellissier Tanon
  */
trait Synchronizer {

  protected trait BasePublisher extends ActorPublisher[Document]

}

object Synchronizer {

  case class Sync()

}
