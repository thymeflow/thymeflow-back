package thymeflow.spatial.geocoding.google

import thymeflow.spatial.geocoding
import thymeflow.spatial.geocoding.FeatureSource
import thymeflow.spatial.geographic.{Geography, Point}

/**
  * @author Thomas Pellissier Tanon
  */
case class Feature(formatted_address: String, address_components: Array[Component], geometry: Geometry, place_id: String)
  extends geocoding.Feature {
  override def source: FeatureSource = GoogleMap(place_id)

  override def address: geocoding.Address = new Address(address_components)

  override def point: Point = Geography.point(geometry.location.lng, geometry.location.lat)

  override def name: Option[String] = Some(formatted_address)
}

private[google] case class Geometry(location: Location) {
}

private[google] case class Location(lat: Double, lng: Double) {
}


private[google] case class Component(long_name: String, short_name: String, types: Array[String]) {
}
