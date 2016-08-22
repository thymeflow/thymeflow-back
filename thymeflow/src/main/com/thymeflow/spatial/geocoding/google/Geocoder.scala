package com.thymeflow.spatial.geocoding.google

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import com.thymeflow.spatial.geocoding
import com.thymeflow.spatial.geographic.Point
import spray.json.DefaultJsonProtocol

import scala.concurrent.{ExecutionContext, Future}

/**
  * Should always be used with a cache as there are API limits
  *
  * @author Thomas Pellissier Tanon
  */
class Geocoder(val apiKey: String)(implicit actorSystem: ActorSystem,
                                   materializer: Materializer,
                                   executionContext: ExecutionContext) extends geocoding.Geocoder with GeocoderResultJsonProtocol {

  val geocoderUri = Uri("https://maps.googleapis.com/maps/api/geocode/json")
  val halfSizeOfTheLocationBias = 0.01

  override def reverse(point: Point): Future[Traversable[geocoding.Feature]] =
    doQuery(Query(("latlng", s"${point.latitude},${point.longitude}")))

  private def doQuery(params: Query): Future[Traversable[Feature]] = {
    Http().singleRequest(HttpRequest(HttpMethods.GET, geocoderUri.withQuery(params.+:(("key", apiKey))))).flatMap {
      case HttpResponse(StatusCodes.OK, _, entity, _) =>
        Unmarshal(entity)
          .to[GeocoderResult](sprayJsonUnmarshaller[GeocoderResult](resultFormat), executionContext, materializer)
          .map(_.results)
    }
  }

  override def direct(address: String): Future[Traversable[geocoding.Feature]] = doQuery(Query(("address", address)))

  override def direct(address: String, locationBias: Point): Future[Traversable[geocoding.Feature]] =
    doQuery(Query(
      ("address", address),
      ("bounds", s"${locationBias.latitude - halfSizeOfTheLocationBias},${locationBias.longitude - halfSizeOfTheLocationBias}|${locationBias.latitude + halfSizeOfTheLocationBias},${locationBias.longitude + halfSizeOfTheLocationBias}")
    ))
}

object Geocoder {
  def apply()(implicit actorSystem: ActorSystem, materializer: Materializer, executionContext: ExecutionContext): Geocoder = {
    new Geocoder(com.thymeflow.config.default.getString("thymeflow.geocoder.google.api-key"))
  }

  private case class GeocoderResult()

}

private[google] case class GeocoderResult(results: Array[Feature]) {
}

private[google] trait GeocoderResultJsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val componentFormat = jsonFormat3(Component)
  implicit val addressFormat = jsonFormat1(Address)
  implicit val locationFormat = jsonFormat2(Location)
  implicit val geometryFormat = jsonFormat1(Geometry)
  implicit val featureFormat = jsonFormat4(Feature)
  implicit val resultFormat = jsonFormat1(GeocoderResult)
}
