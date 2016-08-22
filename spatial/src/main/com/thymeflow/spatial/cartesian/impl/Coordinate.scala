package com.thymeflow.spatial.cartesian.impl

import com.thymeflow.spatial.cartesian

/**
  * @author David Montoya
  */
case class Coordinate(x: Double, y: Double, z: Double = 0.0) extends cartesian.Coordinate {
  override protected def build(x: Double, y: Double, z: Double): cartesian.Coordinate = Coordinate(x, y, z)

  override protected def repr: cartesian.Coordinate = this
}
