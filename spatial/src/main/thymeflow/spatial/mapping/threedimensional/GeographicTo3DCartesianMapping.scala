package thymeflow.spatial.mapping.threedimensional

import thymeflow.spatial

/**
  * @author David Montoya
  */
trait GeographicTo3DCartesianMapping {

  def geographicToCartesian(coordinate: spatial.geographic.Coordinate): spatial.cartesian.Coordinate

  def cartesianToGeographic(coordinate: spatial.cartesian.Coordinate): spatial.geographic.Coordinate

}
