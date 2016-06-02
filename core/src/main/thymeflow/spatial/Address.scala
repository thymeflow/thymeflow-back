package thymeflow.spatial

/**
  * @author David Montoya
  * @author Thomas Pellissier Tanon
  */
trait Address {

  def houseNumber: Option[String]

  def street: Option[String]

  def locality: Option[String]

  def postalCode: Option[String]

  def region: Option[String]

  def country: Option[String]
}
