package com.thymeflow.spatial.geocoding.google

import java.net.URLEncoder

import com.thymeflow.spatial.geocoding.FeatureSource
import com.thymeflow.spatial.geographic.Point

/**
  * @author Thomas Pellissier Tanon
  */
case class GoogleMaps(placeId: String, point: Point) extends FeatureSource {
  override def isValid = true

  // TODO: invalid link
  // The same placeId might have different coordinates, we prefer to create duplicate places.
  // It might be great to create links between places with the same id.
  override def iri = s"https://maps.googleapis.com/maps/api/place/?id=${URLEncoder.encode(placeId, "UTF-8")}&lat=${URLEncoder.encode(point.latitude.toString, "UTF-8")}&lon=${URLEncoder.encode(point.longitude.toString, "UTF-8")}"
}
