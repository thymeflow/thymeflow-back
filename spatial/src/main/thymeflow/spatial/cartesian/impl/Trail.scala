package thymeflow.spatial.cartesian.impl

import thymeflow.spatial

/**
  * @author David Montoya
  */
case class Trail(coordinates: IndexedSeq[spatial.cartesian.Coordinate])
  extends spatial.cartesian.Trail {
  require(coordinates.size >= 2)

  def reverse: Trail = Trail(coordinates.reverse)
}
