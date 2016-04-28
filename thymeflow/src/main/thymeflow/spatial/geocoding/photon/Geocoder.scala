package thymeflow.spatial.geocoding.photon

import java.io.{ByteArrayInputStream, InputStream}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import org.json4s._
import org.json4s.jackson.JsonMethods._
import thymeflow.spatial.geocoding
import thymeflow.spatial.geographic.{Geography, Point}

import scala.concurrent.ExecutionContext


class Geocoder private(serviceUri: Uri)(implicit actorSystem: ActorSystem,
                                        materializer: Materializer,
                                        executionContext: ExecutionContext) extends geocoding.Geocoder with StrictLogging {

  private val reverseGeocodingEndPoint = serviceUri.withPath(Uri.Path./("reverse"))
  private val directGeocodingEndPoint = serviceUri.withPath(Uri.Path./("api"))
  private implicit val addressUnmarshaller = implicitly[Unmarshaller[HttpEntity, Array[Byte]]].forContentTypes(ContentTypeRange(MediaTypes.`application/json`)).map {
    data => parseResponse(new ByteArrayInputStream(data))
  }
  private val http = Http()

  override def reverse(point: Point) = {
    getAddresses(reverseGeocodingEndPoint,
      Query("lon" -> point.longitude.toString, "lat" -> point.latitude.toString))
  }

  override def direct(address: String) = {
    direct(address, None)
  }

  override def direct(address: String, locationBias: Point) = {
    direct(address, Some(locationBias))
  }

  private def direct(address: String, locationBias: Option[Point]) = {
    val baseQuery = Query("q" -> address)
    val finalQuery = locationBias.map {
      point => baseQuery.+:("lon" -> point.longitude.toString).+:("lat" -> point.latitude.toString)
    }.getOrElse(baseQuery)
    getAddresses(directGeocodingEndPoint,
      finalQuery)
  }

  private def getAddresses(endpoint: Uri, query: Query) = {
    http.singleRequest(HttpRequest(method = GET, uri = endpoint.withQuery(query)))
      .flatMap(Unmarshal(_).to[Traversable[geocoding.Feature]])
  }

  /**
    * Parse the geocoder's JSON response
    *
    * @param data the raw geocoder's response
    * @return a Traversable of Features
    */
  protected def parseResponse(data: InputStream): Traversable[geocoding.Feature] = {
    // TODO: Use spray/akka.json instead of json4s here
    val featureCollection = parse(data)
    val featureBuilder = Array.newBuilder[Feature]
    featureCollection \ "features" match {
      case JArray(features) =>
        features.foreach {
          featureJson =>
            implicit val formats = org.json4s.DefaultFormats
            val coordinates = (featureJson \ "geometry" \ "coordinates").extract[Seq[Double]]
            val point = Geography.point(coordinates.head, coordinates.last)
            var address = Address()
            var source = Photon()
            var feature = Feature(point = point, address = address, source = source)
            featureJson \ "properties" match {
              case JObject(properties) =>
                properties.foreach {
                  case ("osm_key", string: JString) =>
                    source = source.copy(osmKey = string.s)
                  case ("osm_id", int: JInt) =>
                    source = source.copy(osmId = int.num.toLong)
                  case ("osm_type", string: JString) =>
                    source = source.copy(osmType = string.s)
                  case ("osm_value", string: JString) =>
                    source = source.copy(osmValue = string.s)
                  case ("country", string: JString) =>
                    address = address.copy(country = Some(string.s))
                  case ("name", string: JString) =>
                    feature = feature.copy(name = Some(string.s))
                  case ("housenumber", string: JString) =>
                    address = address.copy(houseNumber = Some(string.s))
                  case ("city", string: JString) =>
                    address = address.copy(city = Some(string.s))
                  case ("street", string: JString) =>
                    address = address.copy(street = Some(string.s))
                  case ("postcode", string: JString) =>
                    address = address.copy(postcode = Some(string.s))
                  case ("state", string: JString) =>
                    address = address.copy(state = Some(string.s))
                  case s =>
                  //logger.info(s"${s._1} -> ${write(s._2)}")
                }
              case _ => throw new Error("Invalid GeoJSON.")
            }
            feature = feature.copy(address = address).copy(source = source)
            if (feature.isValid) {
              featureBuilder += feature
            } else {
              logger.error(s"Invalid feature $feature")
            }

        }
      case _ => throw new Error("Invalid GeoJSON.")
    }
    val result = featureBuilder.result()
    result
  }
}

object Geocoder {
  def apply(serviceUri: Uri)(implicit actorSystem: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) = {
    new Geocoder(serviceUri)
  }

}