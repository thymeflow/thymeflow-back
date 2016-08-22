package com.thymeflow.spatial.mapping.threedimensional

import com.thymeflow.spatial.cartesian.{Coordinate => CartesianCoordinate}
import com.thymeflow.spatial.geographic.{Coordinate => GeographicCoordinate}

/**
  * @author David Montoya
  */
trait UnitSphereGeographyTo3DCartesianMapping extends GeographicTo3DCartesianMapping {

  override def geographicToCartesian(coordinate: GeographicCoordinate) = {
    val latitudeRadians = coordinate.latitudeRadians
    val longitudeRadians = coordinate.longitudeRadians
    CartesianCoordinate(
      x = Math.cos(latitudeRadians) * Math.cos(longitudeRadians),
      y = Math.cos(latitudeRadians) * Math.sin(longitudeRadians),
      z = Math.sin(latitudeRadians)
    )
  }

  override def cartesianToGeographic(coordinate: CartesianCoordinate) = {
    GeographicCoordinate(
      longitude = Math.atan2(coordinate.y, coordinate.x).toDegrees,
      latitude = Math.asin(coordinate.z).toDegrees
    )
  }

}

object UnitSphereGeographyTo3DCartesianMapping extends UnitSphereGeographyTo3DCartesianMapping
