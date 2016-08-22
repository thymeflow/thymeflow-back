package com.thymeflow.spatial.geographic.impl

import com.thymeflow.spatial.geographic._

/**
  * @author David Montoya
  */
case class Point(longitude: Double, latitude: Double)
  extends Coordinate with com.thymeflow.spatial.geographic.Point {

  def coordinate = this

  def coordinates: IndexedSeq[Coordinate] = Array(this)

  def reverse = this
}
