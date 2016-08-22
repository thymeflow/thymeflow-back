package com.thymeflow.sync

import akka.stream.actor.ActorPublisher
import com.thymeflow.rdf.model.ModelDiff
import com.thymeflow.rdf.model.document.Document

/**
  * @author Thomas Pellissier Tanon
  */
trait Synchronizer {

  protected trait BasePublisher extends ActorPublisher[Document]

}

object Synchronizer {

  case class Update(diff: ModelDiff)

}