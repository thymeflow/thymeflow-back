package com.thymeflow.spatial.geographic.impl

import com.thymeflow.spatial.geographic.Coordinate

/**
  * @author David Montoya
  */
case class Trail(coordinates: IndexedSeq[Coordinate])
  extends com.thymeflow.spatial.geographic.Trail {
  require(coordinates.size >= 2)

  def reverse: Trail = Trail(coordinates.reverse)

}
