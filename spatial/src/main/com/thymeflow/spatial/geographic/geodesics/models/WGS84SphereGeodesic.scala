package com.thymeflow.spatial.geographic.geodesics.models

import com.thymeflow.spatial.geographic.Coordinate
import com.thymeflow.spatial.geographic.datum.WGS84Ellipsoid
import com.thymeflow.spatial.geographic.geodesics.{Geodesic, UnitSphereGeodesic}

/**
  * @author David Montoya
  */
trait WGS84SphereGeodesic extends Geodesic {

  private val ellipsoid = WGS84Ellipsoid
  private val radius = (ellipsoid.semiMajorAxis * 2.0 + ellipsoid.semiMinorAxis) / 3.0

  override def inverse(from: Coordinate, to: Coordinate): (Double, Double) = {
    UnitSphereGeodesic.inverse(from, to) match {
      case (d, azimuth) => (d * radius, azimuth)
    }
  }

  override def project(point: Coordinate, segment: (Coordinate, Coordinate)): (Coordinate, Double) = {
    UnitSphereGeodesic.project(point, segment) match {
      case (projected, distance) => (projected, distance * radius)
    }
  }

  override def direct(from: Coordinate, distance: Double, azimuth: Double): Coordinate = {
    UnitSphereGeodesic.direct(from, distance / radius, azimuth)
  }

  override def distance(from: Coordinate, to: Coordinate): Double = radius * UnitSphereGeodesic.distance(from, to)
}

object WGS84SphereGeodesic extends WGS84SphereGeodesic
