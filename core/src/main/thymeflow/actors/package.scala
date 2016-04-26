package thymeflow

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.StrictLogging
import thymeflow.utilities.ExceptionUtils

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
package object actors extends StrictLogging {

  implicit lazy val system = ActorSystem("thymeflow")

  lazy val decider: Supervision.Decider = throwable => {
    logger.error(ExceptionUtils.getUnrolledStackTrace(throwable))
    Supervision.Stop
  }

  implicit lazy val executor = global

  implicit lazy val materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  )

}
