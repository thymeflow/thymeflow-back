package com.thymeflow.spatial.geographic.geodesics.calculator

import com.thymeflow.spatial.geographic.Coordinate
import com.thymeflow.spatial.geographic.datum.Ellipsoid
import net.sf.geographiclib
import net.sf.geographiclib.GeodesicMask

/**
  * @author David Montoya
  */
class KarneyGeodeticCalculator(ellipsoid: Ellipsoid) extends GeodeticCalculator {

  private val geodesic = new geographiclib.Geodesic(ellipsoid.semiMajorAxis, 1 / ellipsoid.inverseFlattening)

  override def direct(from: Coordinate, distance: Double, azimuth: Double): Coordinate = {
    val d = geodesic.Direct(from.latitude, from.longitude, azimuth, false, distance, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE | GeodesicMask.AZIMUTH)
    Coordinate(d.lon2, d.lat2)
  }

  override def inverse(from: Coordinate, to: Coordinate): (Double, Double) = {
    val d = geodesic.Inverse(from.latitude, from.longitude, to.latitude, to.longitude)
    (d.s12, d.azi1)
  }

  override def inverseDistance(from: Coordinate, to: Coordinate): Double = {
    val d = geodesic.Inverse(from.latitude, from.longitude, to.latitude, to.longitude, GeodesicMask.DISTANCE)
    d.s12
  }

  override def inverseAzimuth(from: Coordinate, to: Coordinate): (Double, Double) = {
    val d = geodesic.Inverse(from.latitude, from.longitude, to.latitude, to.longitude, GeodesicMask.AZIMUTH)
    (d.azi1, d.azi2)
  }
}
