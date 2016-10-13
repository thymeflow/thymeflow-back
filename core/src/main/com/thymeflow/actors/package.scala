package com.thymeflow

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.Timeout
import com.thymeflow.utilities.ExceptionUtils
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
package object actors extends StrictLogging {

  implicit val system = ActorSystem("thymeflow")

  val decider: Supervision.Decider = throwable => {
    logger.error(ExceptionUtils.getUnrolledStackTrace(throwable))
    Supervision.Stop
  }

  implicit val executor = global

  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  )

  implicit val timeout = Timeout(30 seconds)

  implicit val actorContext = ActorSystemContext(executor, system, materializer)
}
