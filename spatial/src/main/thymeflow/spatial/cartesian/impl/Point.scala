package thymeflow.spatial.cartesian.impl

import thymeflow.spatial
import thymeflow.spatial.cartesian
import thymeflow.spatial.cartesian.{Coordinate => CoordinateTrait}

/**
  * @author David Montoya
  */
case class Point(x: Double, y: Double, z: Double = 0.0) extends spatial.cartesian.Point {

  def coordinates: IndexedSeq[CoordinateTrait] = Array(coordinate)

  def coordinate = CoordinateTrait(x, y, z)

  def reverse = this

  override protected def build(x: Double, y: Double, z: Double): cartesian.Point = Point(x, y, z)

  override protected def repr: cartesian.Point = this
}
