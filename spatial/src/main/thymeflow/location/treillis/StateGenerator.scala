package thymeflow.location.treillis

import java.time.Duration

import com.typesafe.scalalogging.StrictLogging
import thymeflow.spatial.geographic.Point
import thymeflow.utilities.time.Implicits._

/**
  * @author David Montoya
  */
object StateGenerator extends StrictLogging {

  def generator[OBSERVATION <: Observation, CLUSTER_OBSERVATION <: ClusterObservation](fromState: State[OBSERVATION, CLUSTER_OBSERVATION],
                                                                                       previousClusterOption: Option[CLUSTER_OBSERVATION],
                                                                                       observationIndex: Int,
                                                                                       observation: OBSERVATION,
                                                                                       clusterOption: Option[CLUSTER_OBSERVATION],
                                                                                       distancePoint: (Point, Point) => Double,
                                                                                       lookupDuration: Duration): Traversable[(Unit, Double, State[OBSERVATION, CLUSTER_OBSERVATION])] = {
    def distance(from: OBSERVATION, to: OBSERVATION) = {
      distancePoint(from.point, to.point)
    }
    def distanceCluster(from: CLUSTER_OBSERVATION, to: OBSERVATION) = {
      distancePoint(from.point, to.point)
    }
    def allowedStep(fromIndex: Int, from: OBSERVATION, toIndex: Int, to: OBSERVATION) = {
      (Duration.between(from.time, to.time) <= lookupDuration) || fromIndex == toIndex - 1
    }
    def allowedClusterStep(cluster: CLUSTER_OBSERVATION, to: OBSERVATION) = {
      cluster.to >= to.time
    }
    fromState match {
      case MovingPosition(movingIndex, moving, cluster) if allowedStep(movingIndex, moving, observationIndex, observation) =>
        new Traversable[(Unit, Double, State[OBSERVATION, CLUSTER_OBSERVATION])] {
          override def foreach[U](f: ((Unit, Double, State[OBSERVATION, CLUSTER_OBSERVATION])) => U): Unit = {
            clusterOption.collect {
              case observationCluster if observationCluster == cluster =>
                val toState = SamePosition[OBSERVATION, CLUSTER_OBSERVATION](observationIndex = observationIndex, observation = observation)
                f((), distanceCluster(cluster, toState.observation) + distance(moving, toState.observation), toState)
            }
            if (allowedClusterStep(cluster, observation)) {
              val toState = MovingPosition(observationIndex = observationIndex, observation = observation, cluster = cluster)
              f(((), distance(moving, toState.observation), toState))
            }
            clusterOption.collect {
              case observationCluster if observationCluster == cluster && allowedClusterStep(observationCluster, observation) =>
                val toState = StationaryPosition(observationIndex = observationIndex, observation = observation, movingIndex = movingIndex, moving = moving, cluster = cluster)
                f(((), distanceCluster(cluster, toState.observation), toState))
            }
          }
        }
      case StationaryPosition(stationaryIndex, stationary, movingIndex, moving, cluster) if allowedStep(movingIndex, moving, observationIndex, observation) =>
        new Traversable[(Unit, Double, State[OBSERVATION, CLUSTER_OBSERVATION])] {
          override def foreach[U](f: ((Unit, Double, State[OBSERVATION, CLUSTER_OBSERVATION])) => U): Unit = {
            clusterOption.collect {
              case observationCluster if observationCluster == cluster =>
                val toState = SamePosition[OBSERVATION, CLUSTER_OBSERVATION](observationIndex = observationIndex, observation = observation)
                f(((), distanceCluster(cluster, toState.observation) + distance(moving, toState.observation), toState))
            }
            if (allowedClusterStep(cluster, observation)) {
              val toState = MovingPosition(observationIndex = observationIndex, observation = observation, cluster = cluster)
              f(((), distance(moving, toState.observation), toState))
            }
            clusterOption.collect {
              case observationCluster if observationCluster == cluster && allowedClusterStep(observationCluster, observation) =>
                val toState = StationaryPosition(observationIndex = observationIndex, observation = observation, movingIndex = movingIndex, moving = moving, cluster = cluster)
                f(((), distanceCluster(cluster, toState.observation), toState))
            }
          }
        }
      case SamePosition(index, fromObservation) =>
        new Traversable[(Unit, Double, State[OBSERVATION, CLUSTER_OBSERVATION])] {
          override def foreach[U](f: ((Unit, Double, State[OBSERVATION, CLUSTER_OBSERVATION])) => U): Unit = {
            val toState = SamePosition[OBSERVATION, CLUSTER_OBSERVATION](observationIndex = observationIndex, observation = observation)
            f(((), distance(fromObservation, toState.observation), toState))
            previousClusterOption.collect {
              case previousCluster if allowedClusterStep(previousCluster, observation) && previousClusterOption != clusterOption =>
                val toState = MovingPosition(observationIndex = observationIndex, observation = observation, cluster = previousCluster)
                f(((), distance(fromObservation, toState.observation) + distanceCluster(previousCluster, fromObservation), toState))
            }
          }
        }
      case _ => Vector.empty
    }
  }
}
