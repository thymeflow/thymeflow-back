package com.thymeflow.spatial.geographic.metric

import com.thymeflow.spatial.geographic.geodesics.Geodesic
import com.thymeflow.spatial.geographic.{Geography, Linear, Point, Trail}
import com.thymeflow.spatial.metric.LinearMetric

/**
  * @author David Montoya
  */
trait GeographyLinearMetric extends LinearMetric[Geography, Linear, Double] {

  def geodesic: Geodesic

  def length(linear: Linear): Double = {
    linear match {
      case point: Point => 0
      case lineString: Trail =>
        lineString.coordinates.sliding(2).map { case IndexedSeq(c1, c2) => geodesic.distance(c1, c2) }.sum
    }
  }

  override def distance(from: Geography, to: Geography): Double = {
    (from, to) match {
      case (p1: Point, p2: Point) => distance(p1, p2)
      case (p: Point, l: Trail) => distance(p, l)
      case (l: Trail, p: Point) => distance(p, l)
      case (l1: Trail, l2: Trail) => distance(l1, l2)
      case (_, _) => throw new NotImplementedError(s"distance is not implemented between an object of type ${from.getClass.getName} and an object of type ${to.getClass.getName}")
    }
  }

  protected def distance(point1: Point, point2: Point): Double = {
    geodesic.distance(point1.coordinate, point2.coordinate)
  }

  protected def distance(point: Point, lineString: Trail) = {
    lineString.coordinates.sliding(2).foldLeft(None: Option[Double]) {
      case (None, IndexedSeq(u, v)) => Some(geodesic.project(point.coordinate, (u, v))._2)
      case (Some(distance), IndexedSeq(u, v)) => Some(Math.min(geodesic.project(point.coordinate, (u, v))._2, distance))
    }.get
  }

  protected def distance(point: Trail, lineString: Trail) = {
    throw new NotImplementedError()
  }
}

