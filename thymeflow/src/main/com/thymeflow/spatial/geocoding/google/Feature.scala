package com.thymeflow.spatial.geocoding.google

import com.thymeflow.spatial
import com.thymeflow.spatial.geocoding
import com.thymeflow.spatial.geocoding.FeatureSource
import com.thymeflow.spatial.geographic.{Geography, Point}

/**
  * @author Thomas Pellissier Tanon
  */
case class Feature(formatted_address: String, address_components: Array[Component], geometry: Geometry, place_id: String)
  extends geocoding.Feature {
  override def source: FeatureSource = GoogleMaps(place_id, point)

  override def address: spatial.Address = Address(address_components)

  override def point: Point = Geography.point(geometry.location.lng, geometry.location.lat)

  override def name: Option[String] = Some(formatted_address)
}

private[google] case class Geometry(location: Location, location_type: Option[String])

private[google] case class Location(lat: Double, lng: Double)

private[google] case class Component(long_name: String, short_name: String, types: Array[String])
