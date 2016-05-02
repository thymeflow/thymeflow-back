package thymeflow.spatial.geocoding.Google

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import spray.json.DefaultJsonProtocol
import thymeflow.spatial.geocoding
import thymeflow.spatial.geographic.Point

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

  override def direct(address: String, locationBias: Point): Future[Traversable[geocoding.Feature]] = ???
}

object Geocoder {
  def apply()(implicit actorSystem: ActorSystem, materializer: Materializer, executionContext: ExecutionContext): Geocoder =
    new Geocoder("AIzaSyBe7Pp1BendNDiv8N0V_8rD4t5hJnQ27Vk")

  //TODO: remove when the repository will become public

  private case class GeocoderResult()

}

private[Google] case class GeocoderResult(results: Array[Feature]) {
}

private[Google] trait GeocoderResultJsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val componentFormat = jsonFormat3(Component)
  implicit val addressFormat = jsonFormat1(Address)
  implicit val locationFormat = jsonFormat2(Location)
  implicit val geometryFormat = jsonFormat1(Geometry)
  implicit val featureFormat = jsonFormat4(Feature)
  implicit val resultFormat = jsonFormat1(GeocoderResult)
}
