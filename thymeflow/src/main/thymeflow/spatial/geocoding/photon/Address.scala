package thymeflow.spatial.geocoding.photon

import thymeflow.spatial.geocoding

/**
  * @author David Montoya
  */

case class Address(houseNumber: Option[String] = None,
                   street: Option[String] = None,
                   city: Option[String] = None,
                   postcode: Option[String] = None,
                   state: Option[String] = None,
                   country: Option[String] = None) extends geocoding.Address {
  def isValid = true
}
