package com.thymeflow.actors

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.ExecutionContext

/**
  * @author David Montoya
  */
case class ActorSystemContext(implicit executionContext: ExecutionContext,
                              system: ActorSystem,
                              materializer: ActorMaterializer)