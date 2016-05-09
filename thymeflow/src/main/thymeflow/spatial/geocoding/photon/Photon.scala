package thymeflow.spatial.geocoding.photon

import thymeflow.spatial.geocoding.FeatureSource

/**
  * @author David Montoya
  */
case class Photon(osmId: Long = -1, osmType: String = "", osmKey: String = "", osmValue: String = "") extends FeatureSource {
  override def isValid = osmId != -1 && osmKey.nonEmpty && osmValue.nonEmpty && osmType.nonEmpty

  override def iri = "http://www.openstreetmap.org/node/" + osmId
}

