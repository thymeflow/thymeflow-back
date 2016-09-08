package com.thymeflow.spatial.geocoding.google

import java.nio.file.Path

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import com.thymeflow.spatial.geocoding.google.Api._
import com.thymeflow.utilities.Cached
import com.typesafe.config.Config
import spray.json.{DefaultJsonProtocol, JsObject, JsString, JsValue, JsonFormat}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

/**
  * Provides a method for querying Google APIs.
  * Internally caches results in a persistent disk cache,
  * in an attempt to cope with API usage limits.
  *
  * ==Overview==
  * It is preferred to cache API results directly, so that in case requirements rapidly change, the cache is still useful.
  *
  * @author David Montoya
  */
class Api(apiKey: String,
          protected val persistentCachePath: Option[Path])(implicit actorSystem: ActorSystem,
                                                           materializer: Materializer,
                                                           executionContext: ExecutionContext) extends ApiJsonProtocol with Cached {

  protected def cacheName = "google-api"

  private val http = Http()
  private val requestsCache = getOrMakeHashMap[Request, JsObject]("requests")

  def doQuery[T](uri: Uri, params: Query)(implicit jsonFormat: JsonFormat[T]): Future[T] = {
    val request = Request(uri = uri, params = params)
    requestsCache.getOrSet((serializedRequest) => {
      http.singleRequest(HttpRequest(HttpMethods.GET,
        serializedRequest.uri.withQuery(serializedRequest.params.+:("key" -> apiKey)))).flatMap {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          Unmarshal(entity).to[JsObject].map {
            response =>
              def errorMessage = response.fields.get("error_message").collect { case JsString(errorMessage) => errorMessage }
              response.fields.get("status") match {
                case Some(JsString(status)) if status == "ZERO_RESULTS" || status == "OK" => response
                case Some(JsString("OVER_QUERY_LIMIT")) => throw new OverQueryLimit(errorMessage)
                case Some(JsString("INVALID_REQUEST")) => throw new InvalidRequest(errorMessage)
                case Some(JsString("NOT_FOUND")) => throw new NotFound(errorMessage)
                case Some(JsString("REQUEST_DENIED")) => throw new RequestDenied(errorMessage)
                case _ => throw new UnknownError(errorMessage)
              }
          }
      }
    })(request).map {
      result => result.convertTo[T]
    }
  }
}

object Api {

  abstract class ApiException(errorMessage: Option[String]) extends Exception(errorMessage.orNull)

  class OverQueryLimit(errorMessage: Option[String]) extends ApiException(errorMessage)

  class InvalidRequest(errorMessage: Option[String]) extends ApiException(errorMessage)

  class NotFound(errorMessage: Option[String]) extends ApiException(errorMessage)

  class RequestDenied(errorMessage: Option[String]) extends ApiException(errorMessage)

  class UnknownError(errorMessage: Option[String]) extends ApiException(errorMessage)

  def apply(persistentCachePath: Option[Path])(implicit actorSystem: ActorSystem,
                                               materializer: Materializer,
                                               executionContext: ExecutionContext,
                                               config: Config) = {
    new Api(config.getString("thymeflow.geocoder.google.api-key"), persistentCachePath)
  }

  private[google] case class Request(uri: Uri, params: Query)

  private[google] trait ApiJsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
    implicit val uriFormat: JsonFormat[Uri] = new JsonFormat[Uri] {
      override def read(json: JsValue): Uri =
        json match {
          case s: JsString => Uri(s.value)
          case _ => throw new IllegalArgumentException(s"Cannot parse $json as Uri.")
        }

      override def write(obj: Uri): JsValue = JsString(obj.toString)
    }
    val mapFormat = implicitly[JsonFormat[Vector[(String, String)]]]
    implicit val queryFormat: JsonFormat[Query] = new JsonFormat[Query] {
      override def read(json: JsValue): Query = Query(mapFormat.read(json): _*)

      override def write(obj: Query): JsValue = mapFormat.write(obj.sorted.toVector)
    }
    implicit val requestFormat: JsonFormat[Request] = jsonFormat2(Request)
  }

}
