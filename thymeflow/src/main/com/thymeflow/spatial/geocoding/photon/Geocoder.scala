package com.thymeflow.spatial.geocoding.photon

import java.io.{ByteArrayInputStream, InputStream}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.Materializer
import com.thymeflow.spatial.geographic.{Geography, Point}
import com.thymeflow.spatial.{SimpleAddress, geocoding}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import spray.json._

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

  private def getAddresses(endpoint: Uri, query: Query) = {
    http.singleRequest(HttpRequest(method = GET, uri = endpoint.withQuery(query)))
      .flatMap(Unmarshal(_).to[Traversable[geocoding.Feature]])
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

  /**
    * Parse the geocoder's JSON response
    *
    * @param data the raw geocoder's response
    * @return a Traversable of Features
    */
  protected def parseResponse(data: InputStream): Traversable[geocoding.Feature] = {
    val featureCollection = JsonParser(IOUtils.toByteArray(data))
    val featureBuilder = Array.newBuilder[Feature]
    featureCollection.asJsObject.fields("features") match {
      case JsArray(features) =>
        features.foreach {
          case featureJson: JsObject =>
            import DefaultJsonProtocol._
            val coordinates = featureJson.fields("geometry").asJsObject.fields("coordinates").convertTo[Seq[Double]]
            val point = Geography.point(coordinates.head, coordinates.last)
            var address = SimpleAddress()
            var source = Photon()
            var feature = Feature(point = point, address = address, source = source)
            featureJson.fields("properties").asJsObject.fields.foreach {
              case ("osm_key", string: JsString) =>
                source = source.copy(osmKey = string.value)
              case ("osm_id", int: JsNumber) =>
                source = source.copy(osmId = int.value.toLong)
              case ("osm_type", string: JsString) =>
                source = source.copy(osmType = string.value)
              case ("osm_value", string: JsString) =>
                source = source.copy(osmValue = string.value)
              case ("country", string: JsString) =>
                address = address.copy(country = Some(string.value))
              case ("name", string: JsString) =>
                feature = feature.copy(name = Some(string.value))
              case ("housenumber", string: JsString) =>
                address = address.copy(houseNumber = Some(string.value))
              case ("city", string: JsString) =>
                address = address.copy(locality = Some(string.value))
              case ("street", string: JsString) =>
                address = address.copy(street = Some(string.value))
              case ("postcode", string: JsString) =>
                address = address.copy(postalCode = Some(string.value))
              case ("state", string: JsString) =>
                address = address.copy(region = Some(string.value))
                  case s =>
                  //logger.info(s"${s._1} -> ${write(s._2)}")
                }
            feature = feature.copy(address = address).copy(source = source)
            if (feature.isValid) {
              featureBuilder += feature
            } else {
              logger.error(s"Invalid feature $feature")
            }
          case _ => throw new Error("Invalid JSON.")
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