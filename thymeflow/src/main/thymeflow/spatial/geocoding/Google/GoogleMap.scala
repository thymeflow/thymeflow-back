package thymeflow.spatial.geocoding.google

import thymeflow.spatial.geocoding.FeatureSource

/**
  * @author Thomas Pellissier Tanon
  */
case class GoogleMap(place_id: String) extends FeatureSource {
  override def isValid = true

  override def iri = "http://maps.google.com/?placeid=" + place_id //TODO: invalid link
}
