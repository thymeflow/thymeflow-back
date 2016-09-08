package com.thymeflow.spatial.geocoding.google

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import com.thymeflow.spatial.geocoding
import com.thymeflow.spatial.geographic.Point
import spray.json.DefaultJsonProtocol

import scala.concurrent.{ExecutionContext, Future}

/**
  * Uses Google Maps Geocoding API to convert geographic coordinates to addresses and vice versa.
  * Requires an [[Api]] instance to query Google's API.
  *
  * @author Thomas Pellissier Tanon
  */
class AddressGeocoder(val api: Api)(implicit executionContext: ExecutionContext)
  extends geocoding.Geocoder with GeocoderResultJsonProtocol {

  val geocoderUri = Uri("https://maps.googleapis.com/maps/api/geocode/json")
  val halfSizeOfTheLocationBias = 0.01

  def doQuery(query: Query): Future[Traversable[geocoding.Feature]] = {
    api.doQuery[GeocoderResult](geocoderUri, query)(resultFormat).map(_.results)
  }

  override def reverse(point: Point): Future[Traversable[geocoding.Feature]] =
    doQuery(Query(("latlng", s"${point.latitude},${point.longitude}")))

  override def direct(address: String): Future[Traversable[geocoding.Feature]] =
    doQuery(Query(("address", address)))

  override def direct(address: String, locationBias: Point): Future[Traversable[geocoding.Feature]] =
    doQuery(Query(
      ("address", address),
      ("bounds", s"${locationBias.latitude - halfSizeOfTheLocationBias},${locationBias.longitude - halfSizeOfTheLocationBias}|${locationBias.latitude + halfSizeOfTheLocationBias},${locationBias.longitude + halfSizeOfTheLocationBias}")
    ))
}

private[google] case class GeocoderResult(results: Array[Feature])

private[google] trait GeocoderResultJsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val componentFormat = jsonFormat3(Component)
  implicit val addressFormat = jsonFormat1(Address)
  implicit val locationFormat = jsonFormat2(Location)
  implicit val geometryFormat = jsonFormat2(Geometry)
  implicit val featureFormat = jsonFormat4(Feature)
  implicit val resultFormat = jsonFormat1(GeocoderResult)
}

