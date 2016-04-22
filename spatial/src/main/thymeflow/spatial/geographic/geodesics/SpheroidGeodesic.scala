package thymeflow.spatial.geographic.geodesics

import com.typesafe.scalalogging.StrictLogging
import thymeflow.spatial.geographic.Coordinate
import thymeflow.spatial.geographic.datum.Ellipsoid
import thymeflow.spatial.geographic.geodesics.calculator.GeodeticCalculator

/**
  * @author David Montoya
  */
trait SpheroidGeodesic extends Geodesic
  with StrictLogging {

  val projectionSnapToEndDistanceTolerance = 1E-6
  private val radius = (ellipsoid.semiMajorAxis * 2.0 + ellipsoid.semiMinorAxis) / 3.0
  private val coordinateDegreesTolerance = 1E-14

  def adjustProjection: Boolean

  override def project(point: Coordinate, segment: (Coordinate, Coordinate)): (Coordinate, Double) = {
    val isSphere = ellipsoid.isSphere
    val (projection, sphereDistanceToProjection) = UnitSphereGeodesic.project(point, segment) match {
      case (c, unitSphereDistance) => (c, radius * unitSphereDistance)
    }
    if (isSphere) {
      (projection, sphereDistanceToProjection)
    } else {
      val adjustedProjection = if (adjustProjection) {
        val splitSegmentDistance = distance(segment._1, projection) + distance(segment._2, projection)
        val segmentLength = distance(segment._1, segment._2)
        if (splitSegmentDistance > 0) {
          val adjustedOffset = {
            val d = distance(segment._1, projection) * segmentLength / splitSegmentDistance
            // just in case
            if (d.isInfinity) {
              0.0
            } else {
              d
            }
          }
          val (_, azimuth) = inverse(segment._1, segment._2)
          val newProjection = direct(segment._1, adjustedOffset, azimuth)
          if ((segmentLength - adjustedOffset).abs < projectionSnapToEndDistanceTolerance) {
            segment._2
          } else if (adjustedOffset.abs < projectionSnapToEndDistanceTolerance) {
            segment._1
          } else {
            newProjection
          }
        } else {
          projection
        }
      } else {
        projection
      }
      (adjustedProjection, distance(point, adjustedProjection))
    }
  }

  override def inverse(from: Coordinate, to: Coordinate): (Double, Double) = {
    if (Math.abs(from.latitude - to.latitude) < coordinateDegreesTolerance && Math.abs(from.longitude - to.longitude) < coordinateDegreesTolerance) {
      (0.0, 180.00)
    } else {
      geodeticCalculator.inverse(from, to)
    }
  }

  override def direct(from: Coordinate, distance: Double, azimuth: Double): Coordinate = {
    geodeticCalculator.direct(from, distance, azimuth)
  }

  override def distance(from: Coordinate, to: Coordinate): Double = {
    geodeticCalculator.inverseDistance(from, to)
  }

  def geodeticCalculator: GeodeticCalculator

  override def startAzimuth(from: Coordinate, to: Coordinate): Double = {
    geodeticCalculator.inverseAzimuth(from, to)._1
  }

  override def endAzimuth(from: Coordinate, to: Coordinate): Double = {
    geodeticCalculator.inverseAzimuth(from, to)._2
  }

  protected def ellipsoid: Ellipsoid
}
