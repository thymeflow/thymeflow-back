package thymeflow.spatial.cartesian

import thymeflow.spatial

/**
  * @author David Montoya
  */
trait Geometry {
  def coordinates: IndexedSeq[Coordinate]

  def head: Point = Geometry.point(coordinates.head)

  def last: Point = Geometry.point(coordinates.last)

  def apply(i: Int) = Geometry.point(coordinates(i))
}

trait Linear extends Geometry {
  def reverse: Linear
}

trait Point extends Linear with CoordinateOperationsLike[Point] {
  // coordinates.size == 1
  def coordinate: Coordinate

  override def reverse: Point

  def projectOn(geometry: Geometry): Point = {
    geometry match {
      case p: Point => p
      case t: Trail =>
        val c = coordinate
        Geometry.point(t.coordinates.sliding(2).map {
          case Seq(c1, c2) =>
            val p = c.project(c1, c2)
            (c.distance(p), p)
        }.minBy(_._1)._2)
    }
  }
}

trait Trail extends Linear {
  // coordinates.size >= 2
  override def reverse: Trail
}

object Geometry {

  def trail(coordinates: IndexedSeq[Coordinate]): Trail = {
    if (coordinates.size >= 2) {
      impl.Trail(coordinates)
    } else {
      throw new IllegalArgumentException(s"trail must have at least 2 coordinates (received ${coordinates.size}).")
    }
  }

  def pointTrail(coordinates: IndexedSeq[Point]): Trail = {
    if (coordinates.size >= 2) {
      impl.Trail(coordinates.map {
        _.coordinate
      })
    } else {
      throw new IllegalArgumentException(s"trail must have at least 2 coordinates (received ${coordinates.size}).")
    }
  }

  def point(x: Double, y: Double, z: Double = 0.0): Point = {
    spatial.cartesian.impl.Point(x, y, z)
  }

  def coordinate(x: Double, y: Double, z: Double): Coordinate = {
    spatial.cartesian.impl.Coordinate(x, y, z)
  }

  def point(c: Coordinate): Point = {
    spatial.cartesian.impl.Point(c.x, c.y, c.z)
  }
}