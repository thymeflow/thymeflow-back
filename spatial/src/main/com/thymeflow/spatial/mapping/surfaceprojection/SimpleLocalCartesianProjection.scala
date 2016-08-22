package com.thymeflow.spatial.mapping.surfaceprojection

import com.thymeflow.spatial.geographic.geodesics.Geodesic
import com.thymeflow.spatial.geographic.{Geography, Point}
import com.thymeflow.spatial.metric.Metric
import com.thymeflow.spatial.{cartesian, geographic}

/**
  * @author David Montoya
  */
class SimpleLocalCartesianProjection(centerPoint: Point, metric: Metric[Point, Double]) extends SurfaceProjection {

  protected val delta: Double = 1e-2
  protected val (longitudeDistortion, latitudeDistortion) = {
    val deltaLatitudePoint = Geography.point(centerPoint.coordinate.longitude, centerPoint.coordinate.latitude + delta)
    val deltaLongitudePoint = Geography.point(centerPoint.coordinate.longitude + delta, centerPoint.coordinate.latitude)
    val deltaLongitude = metric.distance(centerPoint, deltaLongitudePoint)
    val deltaLatitude = metric.distance(centerPoint, deltaLatitudePoint)
    (deltaLongitude / delta, deltaLatitude / delta)
  }

  def map(c: geographic.Coordinate): cartesian.Coordinate = {
    cartesian.Coordinate(
      (c.longitude - centerPoint.longitude) * longitudeDistortion,
      (c.latitude - centerPoint.latitude) * latitudeDistortion
    )
  }


  def inverse(p: cartesian.Coordinate): geographic.Coordinate = {
    // TODO: check resulting coordinates are within bounds
    val result = geographic.Coordinate(
      p.x / longitudeDistortion + centerPoint.longitude,
      p.y / latitudeDistortion + centerPoint.latitude
    )
    if (result.latitude.isInfinite || result.longitude.isInfinite) {
      throw new IllegalStateException("infinity")
    }
    result
  }

}

object SimpleLocalCartesianProjection {
  def apply(center: Point, metric: Metric[Point, Double]) = {
    new SimpleLocalCartesianProjection(center, metric)
  }

  def turnOffset(from: geographic.Coordinate,
                 fromAzimuth: Double,
                 geodesic: Geodesic,
                 metric: Metric[Point, Double],
                 maxIntersectionDistance: Double,
                 distance: Double = 10.0) = {

    val from2 = geodesic.direct(from, distance, fromAzimuth)
    val mapping = SimpleLocalCartesianProjection(from, metric)
    val projectedFrom = mapping.map(from)
    val projectedFrom2 = mapping.map(from2)
    val lFrom = projectedFrom.copy(z = 1.0).crossProduct(projectedFrom2.copy(z = 1.0))

    (to: geographic.Coordinate, toAzimuth: Double) => {

      val to2 = geodesic.direct(to, distance, toAzimuth)
      // use "from" as cartesian map center.

      val projectedTo = mapping.map(to)
      val projectedTo2 = mapping.map(to2)

      // see Hartley and Zisserman, Multiple View Geometry, Sec. 2.2.1
      val lTo = projectedTo.copy(z = 1.0).crossProduct(projectedTo2.copy(z = 1.0))

      val p = lFrom.crossProduct(lTo)
      if (p.z == 0.0) {
        // lines do not intersect
        None
      } else {
        val intersection = p.copy(x = p.x / p.z, y = p.y / p.z, z = 0.0)

        val fromIntersectionVector = intersection.minus(projectedFrom)
        val toIntersectionVector = projectedTo.minus(intersection)

        if (fromIntersectionVector.norm <= maxIntersectionDistance && toIntersectionVector.norm <= maxIntersectionDistance) {
          val i = mapping.inverse(intersection)
          val eFrom = geodesic.distance(from, i)
          val eTo = geodesic.distance(i, to)
          val fromSign = fromIntersectionVector.dotProduct(projectedFrom2.minus(projectedFrom)).signum
          val toSign = toIntersectionVector.dotProduct(projectedTo2.minus(projectedTo)).signum

          Some(eTo * fromSign + eFrom * toSign)
        } else {
          None
        }
      }
    }
  }

  def apply(center: geographic.Coordinate, metric: Metric[Point, Double]) = {
    new SimpleLocalCartesianProjection(Geography.point(center), metric)
  }
}