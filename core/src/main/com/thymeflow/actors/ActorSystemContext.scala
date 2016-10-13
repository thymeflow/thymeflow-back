package com.thymeflow.actors

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.ExecutionContext

/**
  * @author David Montoya
  */
case class ActorSystemContext(executionContext: ExecutionContext,
                              system: ActorSystem,
                              materializer: ActorMaterializer) {

  object Implicits {
    implicit val executionContext = ActorSystemContext.this.executionContext
    implicit val system = ActorSystemContext.this.system
    implicit val materializer = ActorSystemContext.this.materializer
  }

}