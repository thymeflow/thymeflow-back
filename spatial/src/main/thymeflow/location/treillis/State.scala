package thymeflow.location.treillis

import java.nio.ByteBuffer
import java.time.Instant

import thymeflow.spatial.geographic.Point

/**
  * @author David Montoya
  */

trait Observation {
  def index: Int

  def point: Point

  def time: Instant

  def accuracy: Double
}

trait ClusterObservation {
  def index: Int

  def point: Point

  def accuracy: Double

  def from: Instant

  def to: Instant
}

sealed trait State {
  def observation: Observation

  def serialize(implicit observationSerializer: Observation => Int,
                clusterObservationSerializer: ClusterObservation => Int) = State.serialize(this)
}

object State {

  def observationIndex(value: Long) = {
    val b = ByteBuffer.allocate(java.lang.Long.BYTES)
    b.putLong(value)
    b.flip()
    b.get()
    b.getInt
  }

  def serialize(state: State)(implicit observationSerializer: Observation => Int, clusterObservationSerializer: ClusterObservation => Int) = {
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    state match {
      case samePosition: SamePosition =>
        buffer.putInt(-1)
        buffer.putInt(-1)
      case movingPosition: MovingPosition =>
        buffer.putInt(clusterObservationSerializer(movingPosition.cluster))
        buffer.putInt(-1)
      case stationaryPosition: StationaryPosition =>
        buffer.putInt(clusterObservationSerializer(stationaryPosition.cluster))
        buffer.putInt(observationSerializer(stationaryPosition.moving))
    }
    buffer.position(0)
    (observationSerializer(state.observation), buffer.getLong)
  }

  def deserialize(value: (Int, Long))
                 (implicit observationDeserializer: Int => Observation,
                  clusterObservationDeserializer: Int => ClusterObservation): State = {
    val observation = observationDeserializer(value._1)
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.putLong(value._2)
    buffer.flip()
    val clusterIndex = buffer.getInt
    val movingIndex = buffer.getInt
    if (clusterIndex >= 0) {
      if (movingIndex >= 0) {
        val stationary = observation
        val cluster = clusterObservationDeserializer(clusterIndex)
        val moving = observationDeserializer(movingIndex)
        StationaryPosition(moving = moving, stationary = stationary, cluster = cluster)
      } else {
        val moving = observation
        val cluster = clusterObservationDeserializer(clusterIndex)
        MovingPosition(moving = moving, cluster = cluster)
      }
    } else {
      SamePosition(observation)
    }
  }
}

/**
  * SamePosition refers to the state where both all measuring devices are travelling together.
  *
  * @param observation
  */
case class SamePosition(observation: Observation) extends State

/**
  * NotSamePosition refers to those states where the traveler is not traveling with all of her devices.
  */
sealed trait NotSamePosition extends State

/**
  * MovingPosition is a NotSamePosition where an observation is triggered by a device carried by the traveler.
  *
  * @param moving
  * @param cluster
  */
case class MovingPosition(moving: Observation, cluster: ClusterObservation) extends NotSamePosition {
  def observation = moving
}

/**
  * StationaryPosition is a NotSamePosition state where an observation is triggered by a device that is not in the possession of the traveler.
  *
  * @param moving
  * @param stationary
  * @param cluster
  */
case class StationaryPosition(moving: Observation, stationary: Observation, cluster: ClusterObservation) extends NotSamePosition {
  def observation = stationary
}
