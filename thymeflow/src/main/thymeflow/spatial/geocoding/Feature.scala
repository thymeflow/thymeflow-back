package thymeflow.spatial.geocoding

import thymeflow.spatial.geographic.Point

/**
  * @author David Montoya
  */
trait Feature {
  def source: Source

  def point: Point

  def address: Address

  def name: Option[String]
}
