package com.thymeflow.sync

import akka.actor.ActorRef
import akka.stream.actor.ActorPublisher
import akka.stream.{Graph, SourceShape}
import com.thymeflow.rdf.model.StatementSetDiff
import com.thymeflow.rdf.model.document.Document
import com.typesafe.config.Config
import org.eclipse.rdf4j.model.ValueFactory

/**
  * @author Thomas Pellissier Tanon
  */
trait Synchronizer {

  def source(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config): Graph[SourceShape[Document], ActorRef]

  protected trait BasePublisher extends ActorPublisher[Document]

}

object Synchronizer {

  case class Update(diff: StatementSetDiff)

}