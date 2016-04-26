package thymeflow.spatial.geographic.geodesics.calculator

import org.apache.commons.math3.util.FastMath
import thymeflow.spatial.geographic.Coordinate
import thymeflow.spatial.metric.Metric

/**
  * @author David Montoya
  */
trait HaversineUnitSphereDistanceCalculator extends Metric[Coordinate, Double] {

  /**
    * Given two points on a unit sphere, calculate their distance apart in radians.
    *
    * @param from first point
    * @param to   second point
    * @return distance between points, in radians
    */
  def distance(from: Coordinate, to: Coordinate): Double = {
    val sinDeltaLat = Math.sin((to.latitude - from.latitude).toRadians / 2)
    val sinDeltaLon = Math.sin((to.longitude - from.longitude).toRadians / 2)
    val normedDist = sinDeltaLat * sinDeltaLat + sinDeltaLon * sinDeltaLon * Math.cos(from.latitude.toRadians) * Math.cos(to.latitude.toRadians)
    // FastMath has a very fast asin
    2 * FastMath.asin(Math.sqrt(normedDist))
  }

}

object HaversineUnitSphereDistanceCalculator extends HaversineUnitSphereDistanceCalculator
