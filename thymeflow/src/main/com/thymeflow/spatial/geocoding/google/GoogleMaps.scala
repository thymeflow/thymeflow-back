package com.thymeflow.spatial.geocoding.google

import com.thymeflow.spatial.geocoding.FeatureSource

/**
  * @author Thomas Pellissier Tanon
  */
case class GoogleMaps(place_id: String) extends FeatureSource {
  override def isValid = true

  override def iri = "http://maps.google.com/?placeid=" + place_id //TODO: invalid link
}
