package com.thymeflow.spatial.mapping.threedimensional

import com.thymeflow.spatial.cartesian.{Coordinate => CartesianCoordinate}
import com.thymeflow.spatial.geographic.{Coordinate => GeographicCoordinate}

/**
  * @author David Montoya
  */
trait GeographicTo3DCartesianMapping {

  def geographicToCartesian(coordinate: GeographicCoordinate): CartesianCoordinate

  def cartesianToGeographic(coordinate: CartesianCoordinate): GeographicCoordinate

}
