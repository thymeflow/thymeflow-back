package com.thymeflow.spatial.geographic.geodesics

import com.thymeflow.spatial.cartesian
import com.thymeflow.spatial.geographic.Coordinate
import com.thymeflow.spatial.geographic.geodesics.calculator.VincentyUnitSphereDistanceCalculator
import com.thymeflow.spatial.mapping.threedimensional.UnitSphereGeographyTo3DCartesianMapping
import com.typesafe.scalalogging.StrictLogging

/**
  * @author David Montoya
  */
trait UnitSphereGeodesic extends Geodesic with StrictLogging {

  protected val tolerance = 1.0E-12

  def project(point: Coordinate, segment: (Coordinate, Coordinate)): (Coordinate, Double) = {
    segment match {
      case (u, v) =>
        if (coordinateFixedPrecisionEquals(u, v)) {
          (u, distance(point, u))
        } else {
          val n = geographicToCartesianCrossProduct(u, v).normalize
          val p = UnitSphereGeographyTo3DCartesianMapping.geographicToCartesian(point)
          val k = p.minus(n.scale(n.dotProduct(p))).normalize
          val gk = UnitSphereGeographyTo3DCartesianMapping.cartesianToGeographic(k)
          val candidates = if (pointOnSegment(segment, gk)) {
            List((gk, distance(point, gk)))
          } else {
            List()
          }
          val nearest = ((v, distance(point, v)) ::(u, distance(point, u)) :: candidates).minBy(_._2)
          nearest
        }
    }
  }

  protected def coordinateFixedPrecisionEquals(coordinate1: Coordinate, coordinate2: Coordinate) = {
    Math.abs(coordinate1.latitude - coordinate2.latitude) <= tolerance &&
      Math.abs(coordinate1.longitude - coordinate2.longitude) <= tolerance
  }

  protected def pointOnSegment(edge: (Coordinate, Coordinate), point: Coordinate) = {
    edgePointInCone(edge, point) && edgePointOnPlane(edge, point)
  }

  /**
    * Returns true if the point is inside the cone defined by the
    * two ends of the edge.
    */
  protected def edgePointInCone(edge: (Coordinate, Coordinate), point: Coordinate) = {
    val vs = UnitSphereGeographyTo3DCartesianMapping.geographicToCartesian(edge._1)
    val ve = UnitSphereGeographyTo3DCartesianMapping.geographicToCartesian(edge._2)
    if (vs.x == -1.0 * ve.x && vs.y == -1.0 * ve.y && vs.z == -1.0 * ve.z)
      true
    else {
      val vp = UnitSphereGeographyTo3DCartesianMapping.geographicToCartesian(point)
      val vcp = vs.plus(ve).normalize
      val vsDotVcp = vs.dotProduct(vcp)
      val vpDotVcp = vp.dotProduct(vcp)
      vpDotVcp > vsDotVcp || Math.abs(vpDotVcp - vsDotVcp) < 2E-16
    }
  }

  /**
    * Returns true if the point is on the great circle plane.
    * Forms the scalar triple product of A,B,point and if the volume of the
    * resulting parallelepiped is near zero the point is on the
    * great circle plane.
    */
  protected def edgePointOnPlane(edge: (Coordinate, Coordinate), point: Coordinate) = {
    edgePointSide(edge, point) == 0
  }

  /**
    * Returns -1 if the point is to the left of the plane formed
    * by the edge, 1 if the point is to the right, and 0 if the
    * point is on the plane.
    */
  protected def edgePointSide(edge: (Coordinate, Coordinate), point: Coordinate): Int = {
    val normal = geographicToCartesianCrossProduct(edge._1, edge._2).normalize
    val pt = UnitSphereGeographyTo3DCartesianMapping.geographicToCartesian(point)
    val w = normal.dotProduct(pt)
    if (Math.abs(w) <= tolerance) {
      0
    } else {
      if (w < 0) {
        -1
      } else {
        1
      }
    }
  }

  /**
    * Computes the cross product of two vectors using their lat, lng representations.
    * Good even for small distances between p and q.
    */
  protected def geographicToCartesianCrossProduct(p: Coordinate, q: Coordinate): cartesian.Coordinate = {
    val pLatitudeRadians = p.latitudeRadians
    val qLatitudeRadians = q.latitudeRadians
    val pLongitudeRadians = p.longitudeRadians
    val qLongitudeRadians = q.longitudeRadians
    val lonQpp = (qLongitudeRadians + pLongitudeRadians) / -2.0
    val lonQmp = (qLongitudeRadians - pLongitudeRadians) / 2.0
    val sinPLatMinusQLat = Math.sin(pLatitudeRadians - qLatitudeRadians)
    val sinPLatPlusQLat = Math.sin(pLatitudeRadians + qLatitudeRadians)
    val sinLonQpp = Math.sin(lonQpp)
    val sinLonQmp = Math.sin(lonQmp)
    val cosLonQpp = Math.cos(lonQpp)
    val cosLonQmp = Math.cos(lonQmp)
    cartesian.Geometry.coordinate(
      x = sinPLatMinusQLat * sinLonQpp * cosLonQmp - sinPLatPlusQLat * cosLonQpp * sinLonQmp,
      y = sinPLatMinusQLat * cosLonQpp * cosLonQmp + sinPLatPlusQLat * sinLonQpp * sinLonQmp,
      z = Math.cos(pLatitudeRadians) * Math.cos(qLatitudeRadians) * Math.sin(qLongitudeRadians - pLongitudeRadians)
    )
  }

  def direct(from: Coordinate, distance: Double, azimuth: Double): Coordinate = {
    val lat1 = from.latitudeRadians
    val lon1 = from.longitudeRadians
    val azimuthRadians = azimuth.toRadians

    val lat2 = Math.asin(Math.sin(lat1) * Math.cos(distance) + Math.cos(lat1) * Math.sin(distance) * Math.cos(azimuthRadians))

    /* If we're going straight up or straight down, we don't need to calculate the longitude */
    /* TODO: this isn't quite true, what if we're going over the pole? */
    val lon2 = if (fixedPrecisionEquals(azimuthRadians, Math.PI) || fixedPrecisionEquals(azimuthRadians, 0.0)) {
      lon1
    }
    else {
      lon1 + Math.atan2(Math.sin(azimuthRadians) * Math.sin(distance) * Math.cos(lat1), Math.cos(distance) - Math.sin(lat1) * Math.sin(lat2))
    }

    if (lat2.isNaN || lon2.isNaN) {
      throw new IllegalArgumentException(s"{from=$from,d=$distance,azimuth=$azimuthRadians}")
    }
    Coordinate(lon2.toDegrees, lat2.toDegrees)
  }

  protected def fixedPrecisionEquals(d1: Double, d2: Double) = {
    Math.abs(d1 - d2) <= tolerance
  }

  override def inverse(from: Coordinate, to: Coordinate): (Double, Double) = {
    val d = distance(from, to)
    val azimuth = direction(from, to, d)
    (d, azimuth.toDegrees)
  }

  /**
    * Given two points on a unit sphere, calculate the direction
    */
  protected def direction(from: Coordinate, to: Coordinate, d: Double): Double = {
    var heading = 0.0d
    val fromLatitudeRadians = from.latitudeRadians

    /* Starting from the poles? Special case. */
    if (fixedPrecisionEquals(Math.cos(fromLatitudeRadians), 0.0d)) {
      if (fromLatitudeRadians > 0.0) Math.PI else 0.0d
    } else {
      val toLatitudeRadians = to.latitudeRadians
      val f = (Math.sin(toLatitudeRadians) - Math.sin(fromLatitudeRadians) * Math.cos(d)) / (Math.sin(d) * Math.cos(fromLatitudeRadians))
      if (fixedPrecisionEquals(f, 1.0))
        heading = 0.0
      else if (f.abs > 1.0) {
        heading = Math.acos(f)
      }
      else
        heading = Math.acos(f)

      if (Math.sin(to.longitudeRadians - from.longitudeRadians) < 0.0)
        heading = -1 * heading

      heading
    }
  }
}

object UnitSphereGeodesic extends UnitSphereGeodesic with VincentyUnitSphereDistanceCalculator