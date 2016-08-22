package com.thymeflow.spatial.geographic.geodesics

import com.thymeflow.spatial.geographic.Coordinate
import com.thymeflow.spatial.metric.Metric

/**
  * @author David Montoya
  */
trait Geodesic extends Metric[Coordinate, Double] {

  def distance(from: Coordinate, to: Coordinate): Double = {
    inverse(from, to)._1
  }

  def endAzimuth(from: Coordinate, to: Coordinate): Double = {
    180.0 + inverse(to, from)._2
  }

  def inverse(from: Coordinate, to: Coordinate): (Double, Double)

  def project(point: Coordinate, segment: (Coordinate, Coordinate)): (Coordinate, Double)

  def direct(from: Coordinate, distance: Double, azimuth: Double): Coordinate

  def direct(from: Coordinate, to: Coordinate, distance: Double): Coordinate = {
    val azimuth = startAzimuth(from, to)
    direct(from, distance, azimuth)
  }

  def startAzimuth(from: Coordinate, to: Coordinate): Double = {
    inverse(from, to)._2
  }
}
