package thymeflow.spatial.geocoding.google

import thymeflow.spatial.geocoding

/**
  * @author Thomas Pellissier Tanon
  */
case class Address(components: Array[Component]) extends geocoding.Address {

  override def houseNumber: Option[String] = findComponent("street_number")

  override def street: Option[String] = findComponent("street_address").orElse(findComponent("route"))

  override def city: Option[String] = findComponent("locality")

  override def postcode: Option[String] = findComponent("postal_code")

  override def state: Option[String] = findComponent("administrative_area_level_1")

  private def findComponent(typeStr: String): Option[String] = {
    for (component <- components) {
      if (component.types.contains(typeStr)) {
        return Some(component.long_name)
      }
    }
    None
  }

  override def country: Option[String] = findComponent("country")
}
