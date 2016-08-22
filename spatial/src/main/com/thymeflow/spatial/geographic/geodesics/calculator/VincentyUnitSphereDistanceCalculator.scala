package com.thymeflow.spatial.geographic.geodesics.calculator

import com.thymeflow.spatial.geographic.Coordinate
import com.thymeflow.spatial.metric.Metric

/**
  * @author David Montoya
  */
trait VincentyUnitSphereDistanceCalculator extends Metric[Coordinate, Double] {

  /**
    * Given two points on a unit sphere, calculate their distance apart in radians.
    *
    * @param from first point
    * @param to   second point
    * @return distance between points, in radians
    */
  override def distance(from: Coordinate, to: Coordinate): Double = {
    val toLatitudeRadians = to.latitudeRadians
    val fromLatitudeRadians = from.latitudeRadians
    val dLon = to.longitudeRadians - from.longitudeRadians
    val cosDLon = Math.cos(dLon)
    val cosLatTo = Math.cos(toLatitudeRadians)
    val sinLatTo = Math.sin(toLatitudeRadians)
    val cosLatFrom = Math.cos(fromLatitudeRadians)
    val sinLatFrom = Math.sin(fromLatitudeRadians)
    def pow2(x: Double) = x * x
    val a1 = pow2(cosLatTo * Math.sin(dLon))
    val a2 = pow2(cosLatFrom * sinLatTo - sinLatFrom * cosLatTo * cosDLon)
    val a = Math.sqrt(a1 + a2)
    val b = sinLatFrom * sinLatTo + cosLatFrom * cosLatTo * cosDLon
    Math.atan2(a, b)
  }

}

object VincentyUnitSphereDistanceCalculator extends VincentyUnitSphereDistanceCalculator


