package com.thymeflow.spatial.mapping.threedimensional

import com.thymeflow.spatial
import com.thymeflow.spatial.cartesian.Envelope
import com.thymeflow.spatial.geographic.{Geography, Point, Trail}
import com.typesafe.scalalogging.StrictLogging

/**
  * @author David Montoya
  */
trait Envelope3DCartesianCalculator extends StrictLogging {

  protected val tolerance = 1.0E-12
  /* Initialize our 3-space axis points (x+, x-, y+, y-, z+, z-) */
  private val axisPoints = Array(
    spatial.cartesian.Coordinate(x = 1.0, y = 0.0, z = 0.0),
    spatial.cartesian.Coordinate(x = -1.0, y = 0.0, z = 0.0),
    spatial.cartesian.Coordinate(x = 0.0, y = 1.0, z = 0.0),
    spatial.cartesian.Coordinate(x = 0.0, y = -1.0, z = 0.0),
    spatial.cartesian.Coordinate(x = 0.0, y = 0.0, z = 1.0),
    spatial.cartesian.Coordinate(x = 0.0, y = 0.0, z = -1.0))

  def geographyEnvelope(geography: Geography): Envelope = {
    geography match {
      case point: Point =>
        spatial.cartesian.Envelope(
          UnitSphereGeographyTo3DCartesianMapping.geographicToCartesian(point.coordinate)
        )
      case trail: Trail =>
        val cartesianCoordinates = trail.coordinates.map(UnitSphereGeographyTo3DCartesianMapping.geographicToCartesian)
        cartesianCoordinates.sliding(2).foldLeft(None: Option[Envelope]) {
          case (previousEnvelopeOption, Seq(c1, c2)) =>
            val c1c2Envelope = segmentEnvelope(c1, c2)
            Some(previousEnvelopeOption.map(_.include(c1c2Envelope)).getOrElse(c1c2Envelope))
        }.get
    }
  }

  /**
    * Given an edge in spherical coordinates, calculate a
    * 3D envelope that fully contains it, taking into account the curvature
    * of the sphere on which it is inscribed.
    *
    * Any arc on the sphere defines a plane that bisects the sphere. In this plane,
    * the arc is a portion of a unit circle.
    * Projecting the end points of the axes (1,0,0), (-1,0,0) etc, into the plane
    * and normalizing yields potential extrema points. Those points on the
    * side of the plane-dividing line formed by the end points that is opposite
    * the origin of the plane are extrema and should be added to the bounding box.
    *
    * @param a1 the 3D cartesian representation of the first point
    * @param a2 the 3D cartesian representation of the second point
    * @return a 3D envelope containing the edge a1 -> a2 on the surface of the unit sphere.
    */
  def segmentEnvelope(a1: spatial.cartesian.Coordinate, a2: spatial.cartesian.Coordinate) = {

    /* Initialize the envelope with the edge end points */
    val initialEnvelope = spatial.cartesian.Envelope(a1).include(a2.toSeq)

    if (fixedPrecisionEquals(a1.x, a2.x) && fixedPrecisionEquals(a1.y, a2.y) && fixedPrecisionEquals(a1.z, a2.z)) {
      // points are the same, return initialEnvelope
      initialEnvelope
    } else if (fixedPrecisionEquals(a1.x, -a2.x) && fixedPrecisionEquals(a1.y, -a2.y) && fixedPrecisionEquals(a1.z, -a2.z)) {
      throw new IllegalArgumentException(s"cannot calculate Envelope from antipodal coordinates: $a1, $a2")
    } else {
      /* Create A3, a vector in the plane of A1/A2, orthogonal to A1  */
      val an = unitNormal(a1, a2)
      val a3 = unitNormal(an, a1)

      /* Project A1 and A2 into the 2-space formed by the plane A1/A3 */
      val r1 = spatial.cartesian.Coordinate(x = 1.0, y = 0.0)
      val r2 = spatial.cartesian.Coordinate(x = a2.dotProduct(a1), y = a2.dotProduct(a3))

      /* Initialize a 2-space origin point. */
      val o = spatial.cartesian.Coordinate(x = 0.0, y = 0.0)

      /* What side of the line joining R1/R2 is O? */
      val oSide = segmentSide(r1, r2, o)

      var resultEnvelope = initialEnvelope
      /* Add any extrema! */
      for (axisPoint <- axisPoints) {
        /* Convert 3-space axis points to 2-space unit vectors */
        val rx = spatial.cartesian.Coordinate(x = axisPoint.dotProduct(a1), y = axisPoint.dotProduct(a3)).normalize

        /* Any axis end on the side of R1/R2 opposite the origin */
        /* is an extreme point in the arc, so we add the 3-space */
        /* version of the point on R1/R2 to the gbox */
        if (segmentSide(r1, r2, rx) != oSide) {
          val xn = spatial.cartesian.Coordinate(
            x = rx.x * a1.x + rx.y * a3.x,
            y = rx.x * a1.y + rx.y * a3.y,
            z = rx.x * a1.z + rx.y * a3.z
          )

          resultEnvelope = resultEnvelope.include(xn.toSeq)
        }
      }
      resultEnvelope
    }
  }

  protected def fixedPrecisionEquals(d1: Double, d2: Double) = {
    Math.abs(d1 - d2) <= tolerance
  }

  /**
    * Calculates the unit normal to two vectors, trying to avoid
    * problems with over-narrow or over-wide cases.
    */
  protected def unitNormal(coordinate1: spatial.cartesian.Coordinate, coordinate2: spatial.cartesian.Coordinate) = {
    val dotProduct = coordinate1.dotProduct(coordinate2)

    /* If edge is really large, calculate a narrower equivalent angle A1/A3. */
    val p3 = if (dotProduct < 0) {
      coordinate1.plus(coordinate2).normalize
    }
    /* If edge is narrow, calculate a wider equivalent angle A1/A3. */
    else if (dotProduct > 0.95) {
      coordinate2.minus(coordinate1).normalize
    }
    /* Just keep the current angle in A1/A3. */
    else {
      coordinate2
    }
    /* Normals to the A-plane and B-plane */
    coordinate1.crossProduct(p3).normalize
  }

  /**
    *
    * @param p1 2d Point
    * @param p2 2d Point
    * @param q  2d Point
    * @return -1  if point Q is left of segment P
    *         1  if point Q is right of segment P
    *         0  if point Q in on segment P
    */
  protected def segmentSide(p1: spatial.cartesian.Coordinate, p2: spatial.cartesian.Coordinate, q: spatial.cartesian.Coordinate) = {
    val side = (q.x - p1.x) * (p2.y - p1.y) - (p2.x - p1.x) * (q.y - p1.y)
    if (side == 0.0) 0 else side.signum
  }
}

object Envelope3DCartesianCalculator extends Envelope3DCartesianCalculator