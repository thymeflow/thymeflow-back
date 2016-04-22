package thymeflow.spatial.geographic.impl

import thymeflow.spatial.geographic._

/**
  * @author David Montoya
  */
case class Point(longitude: Double, latitude: Double)
  extends Coordinate with thymeflow.spatial.geographic.Point {

  def coordinate = this

  def coordinates: IndexedSeq[Coordinate] = Array(this)

  def reverse = this
}
