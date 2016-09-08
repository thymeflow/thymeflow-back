package com.thymeflow.spatial.geocoding.google

import com.thymeflow.spatial
import com.thymeflow.spatial.geocoding
import com.thymeflow.spatial.geocoding.FeatureSource
import com.thymeflow.spatial.geographic.{Geography, Point}

/**
  * @author David Montoya
  */
case class Place(name: Option[String], place_id: String, geometry: Geometry) extends geocoding.Feature {
  override def source: FeatureSource = GoogleMaps(place_id)

  override def address: spatial.Address = EmptyAddress

  override def point: Point = Geography.point(geometry.location.lng, geometry.location.lat)
}
