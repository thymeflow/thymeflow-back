package com.thymeflow.spatial.geocoding.google

import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import com.thymeflow.spatial.geocoding
import com.thymeflow.spatial.geocoding.{Feature => BaseFeature}
import com.thymeflow.spatial.geographic.Point
import spray.json.JsonFormat

import scala.concurrent.{ExecutionContext, Future}

/**
  * Uses Google Places API to convert geographic coordinates to places and vice versa.
  * Requires an [[Api]] instance to query Google's API.
  *
  * @author David Montoya
  */
class PlacesGeocoder(api: Api, radius: Long = 5000)(implicit executionContext: ExecutionContext)
  extends geocoding.Geocoder with PlacesResultJsonProtocol {

  private val nearbySearchUri = Uri("https://maps.googleapis.com/maps/api/place/nearbysearch/json")
  private val autocompleteUri = Uri("https://maps.googleapis.com/maps/api/place/autocomplete/json")
  private val detailsUri = Uri("https://maps.googleapis.com/maps/api/place/details/json")

  override def reverse(point: Point): Future[Traversable[BaseFeature]] =
    nearbyPlaces(Query(("location", s"${point.latitude},${point.longitude}")))

  private def nearbyPlaces(params: Query): Future[Traversable[BaseFeature]] = {
    api.doQuery[PlacesResult](nearbySearchUri, params.+:("radius" -> radius.toString)).map(_.results)
  }

  override def direct(address: String): Future[Traversable[BaseFeature]] = {
    api.doQuery[AutocompleteResult](autocompleteUri, Query("input" -> address)).flatMap {
      result =>
        result.predictions match {
          case Array(prediction) =>
            api.doQuery[PlaceDetailsResult](detailsUri, Query("placeid" -> prediction.place_id)).map(_.result: Traversable[BaseFeature])
          case _ =>
            Future.successful(Traversable.empty)
        }
    }
  }

  override def direct(address: String, locationBias: Point): Future[Traversable[BaseFeature]] =
    nearbyPlaces(Query(
      ("name", address),
      ("location", s"${locationBias.latitude},${locationBias.longitude}")
    ))
}


private[google] case class Prediction(place_id: String)

private[google] case class AutocompleteResult(predictions: Array[Prediction])

private[google] case class PlacesResult(results: Array[Place])

private[google] case class PlaceDetailsResult(result: Option[Place])

private[google] trait PlacesResultJsonProtocol extends GeocoderResultJsonProtocol {
  implicit val placeFormat: JsonFormat[Place] = jsonFormat3(Place)
  implicit val placesResultFormat: JsonFormat[PlacesResult] = jsonFormat1(PlacesResult)
  implicit val predictionFormat: JsonFormat[Prediction] = jsonFormat1(Prediction)
  implicit val autocompleteResultFormat: JsonFormat[AutocompleteResult] = jsonFormat1(AutocompleteResult)
  implicit val placeDetailsResultFormat: JsonFormat[PlaceDetailsResult] = jsonFormat1(PlaceDetailsResult)
}