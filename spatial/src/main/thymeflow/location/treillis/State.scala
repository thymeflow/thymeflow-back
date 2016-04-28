package thymeflow.location.treillis

import java.nio.ByteBuffer
import java.time.Instant

import thymeflow.spatial.geographic.Point

/**
  * @author David Montoya
  */

trait Observation {
  def point: Point

  def time: Instant

  def accuracy: Double
}

trait ClusterObservation {
  def point: Point

  def accuracy: Double

  def from: Instant

  def to: Instant
}

sealed trait State[OBSERVATION <: Observation, CLUSTER_OBSERVATION <: ClusterObservation] {
  def observationIndex: Int

  def observation: Observation

  def serialize(implicit clusterObservationSerializer: CLUSTER_OBSERVATION => Int) = State.serialize(this)
}

object State {

  def observationIndex(value: Long) = {
    val b = ByteBuffer.allocate(java.lang.Long.BYTES)
    b.putLong(value)
    b.flip()
    b.get()
    b.getInt
  }

  def serialize[OBSERVATION <: Observation, CLUSTER_OBSERVATION <: ClusterObservation]
  (state: State[OBSERVATION, CLUSTER_OBSERVATION])(implicit clusterObservationToIndex: CLUSTER_OBSERVATION => Int) = {
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    state match {
      case SamePosition(_, _) =>
        buffer.putInt(-1)
        buffer.putInt(-1)
      case MovingPosition(_, _, cluster) =>
        buffer.putInt(clusterObservationToIndex(cluster))
        buffer.putInt(-1)
      case StationaryPosition(_, _, movingIndex, _, cluster) =>
        buffer.putInt(clusterObservationToIndex(cluster))
        buffer.putInt(movingIndex)
    }
    buffer.position(0)
    (state.observationIndex, buffer.getLong)
  }

  def deserialize[OBSERVATION <: Observation, CLUSTER_OBSERVATION <: ClusterObservation](value: (Int, Long))
                                                                                        (implicit indexToObservation: Int => OBSERVATION,
                                                                                         indexToClusterObservation: Int => CLUSTER_OBSERVATION): State[OBSERVATION, CLUSTER_OBSERVATION] = {
    val observationIndex = value._1
    val observation = indexToObservation(observationIndex)
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.putLong(value._2)
    buffer.flip()
    val clusterIndex = buffer.getInt
    val movingIndex = buffer.getInt
    if (clusterIndex >= 0) {
      if (movingIndex >= 0) {
        val cluster = indexToClusterObservation(clusterIndex)
        val moving = indexToObservation(movingIndex)
        StationaryPosition(observationIndex = observationIndex, observation = observation, movingIndex = movingIndex, moving = moving, cluster = cluster)
      } else {
        val cluster = indexToClusterObservation(clusterIndex)
        MovingPosition(observationIndex = observationIndex, observation = observation, cluster = cluster)
      }
    } else {
      SamePosition(observationIndex = observationIndex, observation)
    }
  }
}

/**
  * SamePosition refers to the state where both all measuring devices are travelling together.
  *
  */
case class SamePosition[OBSERVATION <: Observation, CLUSTER_OBSERVATION <: ClusterObservation](observationIndex: Int, observation: OBSERVATION) extends State[OBSERVATION, CLUSTER_OBSERVATION]

/**
  * NotSamePosition refers to those states where the traveler is not traveling with all of her devices.
  */
sealed trait NotSamePosition[OBSERVATION <: Observation, CLUSTER_OBSERVATION <: ClusterObservation] extends State[OBSERVATION, CLUSTER_OBSERVATION]

/**
  * MovingPosition is a NotSamePosition where an observation is triggered by a device carried by the traveler.
  *
  */
case class MovingPosition[OBSERVATION <: Observation, CLUSTER_OBSERVATION <: ClusterObservation](observationIndex: Int, observation: OBSERVATION, cluster: CLUSTER_OBSERVATION) extends NotSamePosition[OBSERVATION, CLUSTER_OBSERVATION]

/**
  * StationaryPosition is a NotSamePosition state where an observation is triggered by a device that is not in the possession of the traveler.
  *
  */
case class StationaryPosition[OBSERVATION <: Observation, CLUSTER_OBSERVATION <: ClusterObservation](observationIndex: Int, observation: OBSERVATION, movingIndex: Int, moving: OBSERVATION, cluster: CLUSTER_OBSERVATION) extends NotSamePosition[OBSERVATION, CLUSTER_OBSERVATION]
