package com.thymeflow.spatial.geocoding.google

import com.thymeflow.spatial

/**
  * @author Thomas Pellissier Tanon
  */
case class Address(components: Array[Component]) extends spatial.Address {

  override def houseNumber: Option[String] = findComponent("street_number")

  override def street: Option[String] = findComponent("street_address").orElse(findComponent("route"))

  private def findComponent(typeStr: String): Option[String] = {
    for (component <- components) {
      if (component.types.contains(typeStr)) {
        return Some(component.long_name)
      }
    }
    None
  }

  override def locality: Option[String] = findComponent("locality")

  override def postalCode: Option[String] = findComponent("postal_code")

  override def region: Option[String] = findComponent("administrative_area_level_1")

  override def country: Option[String] = findComponent("country")
}

object EmptyAddress extends spatial.Address {
  override def houseNumber: Option[String] = None

  override def postalCode: Option[String] = None

  override def country: Option[String] = None

  override def region: Option[String] = None

  override def locality: Option[String] = None

  override def street: Option[String] = None
}