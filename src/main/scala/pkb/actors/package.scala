package pkb

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
package object actors {

  implicit lazy val system = ActorSystem("pkb")

  lazy val decider: Supervision.Decider = {
    case throwable =>
      throwable.printStackTrace()
      Supervision.Stop
  }

  implicit lazy val materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(decider))

}
