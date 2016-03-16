package pkb

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.scalalogging.StrictLogging
import pkb.utilities.ExceptionUtils

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
package object actors extends StrictLogging {

  implicit lazy val system = ActorSystem("pkb")

  lazy val decider: Supervision.Decider = {
    case throwable =>
      logger.error(ExceptionUtils.getUnrolledStackTrace(throwable))
      Supervision.Stop
  }

  implicit lazy val materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(decider))

}
