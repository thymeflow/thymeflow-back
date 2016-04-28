package thymeflow.spatial.geocoding

/**
  * @author David Montoya
  */
trait Address {

  def houseNumber: Option[String]

  def street: Option[String]

  def city: Option[String]

  def postcode: Option[String]

  def state: Option[String]

  def country: Option[String]
}
