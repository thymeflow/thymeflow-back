package thymeflow.location.treillis

import java.time.Duration

import com.typesafe.scalalogging.StrictLogging
import thymeflow.spatial.geographic.Point
import thymeflow.utilities.time.Implicits._

/**
  * @author David Montoya
  */
object StateGenerator extends StrictLogging {

  def generator(fromState: State,
                previousClusterOption: Option[ClusterObservation],
                observation: Observation,
                clusterOption: Option[ClusterObservation],
                distancePoint: (Point, Point) => Double,
                lookupDuration: Duration): Traversable[(Unit, Double, State)] = {
    def distance(from: Observation, to: Observation) = {
      distancePoint(from.point, to.point)
    }
    def distanceCluster(from: ClusterObservation, to: Observation) = {
      distancePoint(from.point, to.point)
    }
    def allowedStep(from: Observation, to: Observation) = {
      (Duration.between(from.time, to.time) <= lookupDuration) || from.index == to.index - 1
    }
    def allowedClusterStep(cluster: ClusterObservation, to: Observation) = {
      cluster.to >= to.time
    }
    fromState match {
      case MovingPosition(moving, cluster) if allowedStep(moving, observation) =>
        new Traversable[(Unit, Double, State)] {
          override def foreach[U](f: ((Unit, Double, State)) => U): Unit = {
            clusterOption.collect {
              case observationCluster if observationCluster == cluster =>
                val toState = SamePosition(observation)
                f((), distanceCluster(cluster, toState.observation) + distance(moving, toState.observation), toState)
            }
            if (allowedClusterStep(cluster, observation)) {
              val toState = MovingPosition(moving = observation, cluster = cluster)
              f(((), distance(moving, toState.moving), toState))
            }
            clusterOption.collect {
              case observationCluster if observationCluster == cluster && allowedClusterStep(observationCluster, observation) =>
                val toState = StationaryPosition(moving = moving, stationary = observation, cluster = cluster)
                f(((), distanceCluster(cluster, toState.observation), toState))
            }
          }
        }
      case StationaryPosition(moving, stationary, cluster) if allowedStep(moving, observation) =>
        new Traversable[(Unit, Double, State)] {
          override def foreach[U](f: ((Unit, Double, State)) => U): Unit = {
            clusterOption.collect {
              case observationCluster if observationCluster == cluster =>
                val toState = SamePosition(observation)
                f(((), distanceCluster(cluster, toState.observation) + distance(moving, toState.observation), toState))
            }
            if (allowedClusterStep(cluster, observation)) {
              val toState = MovingPosition(moving = observation, cluster = cluster)
              f(((), distance(moving, toState.moving), toState))
            }
            clusterOption.collect {
              case observationCluster if observationCluster == cluster && allowedClusterStep(observationCluster, observation) =>
                val toState = StationaryPosition(moving = moving, stationary = observation, cluster = cluster)
                f(((), distanceCluster(cluster, toState.observation), toState))
            }
          }
        }
      case SamePosition(fromObservation: Observation) =>
        new Traversable[(Unit, Double, State)] {
          override def foreach[U](f: ((Unit, Double, State)) => U): Unit = {
            val toState = SamePosition(observation)
            f(((), distance(fromObservation, toState.observation), toState))
            previousClusterOption.collect {
              case previousCluster if allowedClusterStep(previousCluster, observation) && previousClusterOption != clusterOption =>
                val toState = MovingPosition(moving = observation, cluster = previousCluster)
                f(((), distance(fromObservation, toState.moving) + distanceCluster(previousCluster, fromObservation), toState))
            }
          }
        }
      case _ => Vector.empty
    }
  }
}
