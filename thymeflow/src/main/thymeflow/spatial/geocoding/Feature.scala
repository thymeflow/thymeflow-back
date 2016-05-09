package thymeflow.spatial.geocoding

import thymeflow.spatial.Address
import thymeflow.spatial.geographic.Point

/**
  * @author David Montoya
  */
trait Feature {
  def source: FeatureSource

  def point: Point

  def address: Address

  def name: Option[String]
}
