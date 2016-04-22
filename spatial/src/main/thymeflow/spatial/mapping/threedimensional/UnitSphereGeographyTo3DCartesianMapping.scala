package thymeflow.spatial.mapping.threedimensional

import thymeflow.spatial

/**
  * @author David Montoya
  */
trait UnitSphereGeographyTo3DCartesianMapping extends GeographicTo3DCartesianMapping {

  override def geographicToCartesian(coordinate: spatial.geographic.Coordinate) = {
    val latitudeRadians = coordinate.latitudeRadians
    val longitudeRadians = coordinate.longitudeRadians
    spatial.cartesian.Coordinate(
      x = Math.cos(latitudeRadians) * Math.cos(longitudeRadians),
      y = Math.cos(latitudeRadians) * Math.sin(longitudeRadians),
      z = Math.sin(latitudeRadians)
    )
  }

  override def cartesianToGeographic(coordinate: spatial.cartesian.Coordinate) = {
    spatial.geographic.Coordinate(
      longitude = Math.atan2(coordinate.y, coordinate.x).toDegrees,
      latitude = Math.asin(coordinate.z).toDegrees
    )
  }

}

object UnitSphereGeographyTo3DCartesianMapping extends UnitSphereGeographyTo3DCartesianMapping
