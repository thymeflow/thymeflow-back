package thymeflow.spatial.geocoding

import akka.http.scaladsl.model.Uri
import thymeflow.actors._
import thymeflow.spatial.geocoding
import thymeflow.spatial.geographic.Point

import scala.concurrent.Future

/**
  * @author David Montoya
  */
trait Geocoder {
  def reverse(point: Point): Future[Traversable[Feature]]

  def direct(address: String): Future[Traversable[Feature]]

  def direct(address: String, locationBias: Point): Future[Traversable[Feature]]
}


object Geocoder {
  def photon(serviceUri: Uri) = geocoding.photon.Geocoder(serviceUri)

  def google() = throw new NotImplementedError()
}