package com.thymeflow.spatial.geographic

import com.thymeflow.spatial.cartesian.Envelope
import com.thymeflow.spatial.mapping.threedimensional.Envelope3DCartesianCalculator

/**
  * @author David Montoya
  */
trait Geography {
  lazy val envelope: Envelope = Envelope3DCartesianCalculator.geographyEnvelope(this)

  def coordinates: IndexedSeq[Coordinate]

  def head: Point = Geography.point(coordinates.head)

  def last: Point = Geography.point(coordinates.last)

  def apply(i: Int) = Geography.point(coordinates(i))

  def asPoint: Point = {
    this match {
      case point: Point => point
      case _ => throw new IllegalArgumentException(s"$this is not a Point.")
    }
  }

  def asTrail: Trail = {
    this match {
      case trail: Trail => trail
      case _ => throw new IllegalArgumentException(s"$this is not a Trail")
    }
  }
}

trait Linear extends Geography {
  // coordinates.size >= 2
  def reverse: Linear
}

trait Trail extends Linear {
  override def reverse: Trail
}

trait Point
  extends Linear {
  // coordinates.size == 1
  def longitude: Double

  def latitude: Double

  def coordinate: Coordinate

  override def reverse: Point
}

object Geography {
  def point(longitude: Double, latitude: Double): Point = {
    impl.Point(longitude, latitude)
  }

  def point(coordinate: Coordinate): Point = {
    impl.Point(coordinate.longitude, coordinate.latitude)
  }

  def trail(coordinates: Seq[Coordinate]): Trail = {
    if (coordinates.size >= 2) {
      impl.Trail(coordinates.toIndexedSeq)
    } else {
      throw new IllegalArgumentException(s"trail must have at least 2 coordinates (received ${coordinates.size}).")
    }
  }

  def pointTrail(points: Seq[Point]): Trail = {
    if (points.size >= 2) {
      impl.Trail(points.toIndexedSeq.map(_.coordinate))
    } else {
      throw new IllegalArgumentException(s"trail must have at least 2 coordinates (received ${points.size}).")
    }
  }

}

object Point {
  def unapply(point: Point) = {
    Some((point.longitude, point.latitude))
  }
}

object Trail {
  def unapply(trail: Trail) = {
    Some(trail.coordinates)
  }
}