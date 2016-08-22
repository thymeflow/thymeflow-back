package com.thymeflow.spatial.mapping.surfaceprojection

import com.thymeflow.spatial

/**
  * @author David Montoya
  */
trait SurfaceProjection {
  def map(c: spatial.geographic.Coordinate): spatial.cartesian.Coordinate

  def map(p: spatial.geographic.Point): spatial.cartesian.Point = {
    spatial.cartesian.Geometry.point(map(p.coordinate))
  }

  def map(t: spatial.geographic.Trail): spatial.cartesian.Trail = {
    val mappedCoordinates = t.coordinates.map { c => map(c) }
    spatial.cartesian.Geometry.trail(mappedCoordinates)
  }

  def inverse(p: spatial.cartesian.Coordinate): spatial.geographic.Coordinate

  def inverse(p: spatial.cartesian.Point): spatial.geographic.Point = {
    spatial.geographic.Geography.point(inverse(p.coordinate))
  }

  def inverse(t: spatial.cartesian.Trail): spatial.geographic.Trail = {
    spatial.geographic.Geography.trail(t.coordinates.map { c => inverse(c) })
  }
}
