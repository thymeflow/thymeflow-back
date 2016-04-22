package thymeflow.spatial.geographic

/**
  * @author David Montoya
  */
trait Coordinate {
  def longitude: Double

  def latitude: Double

  def longitudeRadians: Double = longitude.toRadians

  def latitudeRadians: Double = latitude.toRadians
}

case class CoordinateImpl(longitude: Double, latitude: Double) extends Coordinate {
  override def toString: String = {
    s"Coordinate(${longitude.toString}, ${latitude.toString})"
  }
}

object Coordinate {
  def apply(longitude: Double, latitude: Double): Coordinate = CoordinateImpl(longitude, latitude)

  def unapply(c: Coordinate): Option[(Double, Double)] = Some((c.longitude, c.latitude))
}