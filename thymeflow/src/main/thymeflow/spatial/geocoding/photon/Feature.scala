package thymeflow.spatial.geocoding.photon

import thymeflow.spatial.geocoding
import thymeflow.spatial.geocoding.FeatureSource
import thymeflow.spatial.geographic.Point

/**
  * @author David Montoya
  */
case class Feature(name: Option[String] = None, point: Point, address: Address, source: FeatureSource) extends geocoding.Feature {
  def isValid = {
    address.isValid && source.isValid
  }
}
