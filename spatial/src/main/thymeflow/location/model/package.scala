package thymeflow.location

import java.time.{Duration, Instant}

import thymeflow.spatial.geographic.Point
import thymeflow.utilities.time.Implicits._

/**
  * @author David Montoya
  */
package object model {

  trait Stay {
    def weight = Duration.between(from, to).toSecondsDouble

    def variance = accuracy * accuracy

    def from: Instant

    def to: Instant

    def accuracy: Double

    def point: Point
  }

  case class Observation(index: Int, time: Instant, accuracy: Double, point: Point) extends thymeflow.location.treillis.Observation

  case class ClusterObservation(index: Int, from: Instant, to: Instant, accuracy: Double, point: Point) extends thymeflow.location.treillis.ClusterObservation with Stay

  case class StayCluster[T <: Stay](duration: Duration, accuracy: Double, point: Point, stays: IndexedSeq[T]) {
    def weight = duration.toSecondsDouble

    def variance = accuracy * accuracy
  }

}
