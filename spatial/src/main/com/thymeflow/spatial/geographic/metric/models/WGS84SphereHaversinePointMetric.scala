package com.thymeflow.spatial.geographic.metric.models

import com.thymeflow.spatial.geographic.Point
import com.thymeflow.spatial.geographic.datum.WGS84Ellipsoid
import com.thymeflow.spatial.geographic.geodesics.calculator.HaversineUnitSphereDistanceCalculator
import com.thymeflow.spatial.metric.Metric

/**
  * @author David Montoya
  */
object WGS84SphereHaversinePointMetric extends Metric[Point, Double] {

  private val ellipsoid = WGS84Ellipsoid
  private val radius = (ellipsoid.semiMajorAxis * 2.0 + ellipsoid.semiMinorAxis) / 3.0

  override def distance(from: Point, to: Point): Double = {
    HaversineUnitSphereDistanceCalculator.distance(from.coordinate, to.coordinate) * radius
  }
}
