package com.thymeflow.spatial.geographic.geodesics.calculator

import com.thymeflow.spatial.geographic.Coordinate

/**
  * @author David Montoya
  */
trait GeodeticCalculator {
  /**
    *
    * @param from     starting point
    * @param distance distance in meters
    * @param azimuth  azimuth in Degrees
    * @return The destination point
    */
  def direct(from: Coordinate, distance: Double, azimuth: Double): Coordinate

  /**
    *
    * @param from the starting point
    * @param to   the destination point
    * @return the distance in meters and start azimuth in degrees
    */
  def inverse(from: Coordinate, to: Coordinate): (Double, Double)

  /**
    *
    * @param from the starting point
    * @param to   the destination point
    * @return the distance in meters
    */
  def inverseDistance(from: Coordinate, to: Coordinate): Double

  /**
    *
    * @param from the starting point
    * @param to   the destination point
    * @return the start and end azimuths in degrees
    */
  def inverseAzimuth(from: Coordinate, to: Coordinate): (Double, Double)
}
