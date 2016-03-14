package pkb

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
package object actors {
  implicit lazy val system = ActorSystem("pkb")
  implicit lazy val materializer = ActorMaterializer()
}
