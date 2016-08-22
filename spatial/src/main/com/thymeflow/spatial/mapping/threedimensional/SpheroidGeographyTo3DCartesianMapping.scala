package com.thymeflow.spatial.mapping.threedimensional

import com.thymeflow.spatial
import com.thymeflow.spatial.cartesian
import com.thymeflow.spatial.geographic.Coordinate
import com.thymeflow.spatial.geographic.datum.Ellipsoid


/**
  * @author David Montoya
  */
trait SpheroidGeographyTo3DCartesianMapping extends GeographicTo3DCartesianMapping {
  val ellipsoid: Ellipsoid

  protected val a = ellipsoid.semiMajorAxis
  protected val c = ellipsoid.semiMinorAxis
  protected val aSquared = a * a
  protected val cSquared = c * c
  protected val firstEccentricitySquared = 1 - (c / a) * (c / a)

  /*
    Computes cartesian (x,y,z) coordinates from a pair of (longitude,latitude) geodetic coordinates
     Uses formula by Marcin Ligas, Piotr Banasik in
     "Conversion between Cartesian and geodetic coordinates on a rotational ellipsoid by solving a system of nonlinear equations"
     GEODESY AND CARTOGRAPHY, Vol.60 No.2, 2011, page 146
   */
  override def geographicToCartesian(coordinate: Coordinate): spatial.cartesian.Coordinate = {
    val latitudeRadians = coordinate.latitudeRadians
    val longitudeRadians = coordinate.longitudeRadians
    val cosLatitude = math.cos(latitudeRadians)
    val sinLatitude = math.sin(latitudeRadians)
    // radius of curvature in the prime vertical
    val N = a / math.sqrt(1 - firstEccentricitySquared * sinLatitude * sinLatitude)
    spatial.cartesian.Coordinate(
      x = N * cosLatitude * math.cos(longitudeRadians),
      y = N * cosLatitude * math.sin(longitudeRadians),
      z = ((1 - firstEccentricitySquared) * N) * sinLatitude
    )
  }

  override def cartesianToGeographic(coordinate: cartesian.Coordinate): Coordinate = {
    throw new NotImplementedError()
  }
}

object WGS84SpheroidGeographyTo3DCartesianMapping extends {
  override val ellipsoid = spatial.geographic.datum.WGS84Ellipsoid
} with SpheroidGeographyTo3DCartesianMapping
