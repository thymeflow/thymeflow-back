package thymeflow.spatial.geocoding.photon

import thymeflow.spatial.geocoding.Source

/**
  * @author David Montoya
  */
case class Photon(osmId: Long = -1, osmType: String = "", osmKey: String = "", osmValue: String = "") extends Source {
  def isValid = osmId != -1 && osmKey.nonEmpty && osmValue.nonEmpty && osmType.nonEmpty
}

