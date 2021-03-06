package com.thymeflow.spatial.geocoding

import java.nio.file.Path

import akka.http.scaladsl.model.Uri
import com.thymeflow.actors._
import com.thymeflow.spatial.geocoding
import com.thymeflow.spatial.geocoding.google.Api
import com.thymeflow.spatial.geographic.Point
import com.typesafe.config.Config

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
  /**
    * Geocoder client for Photon (http://photon.komoot.de/)
    *
    * @param serviceUri the Photon geocoder's URI
    * @return a PhotonGeocoder
    */
  def photon(serviceUri: Uri) = geocoding.photon.Geocoder(serviceUri)

  /**
    * Wraps the geocoder's responses within some persistent cache
    *
    * @param geocoder the geocoder to cache
    * @return a Geocoder that caches responses
    */
  def cached(geocoder: Geocoder, persistentCacheFile: Option[Path] = None) = new CachedGeocoder(geocoder, persistentCacheFile)

  /**
    * Geocoder client using Google API
    * Requests to Google API are cached.
    *
    * @return a Geocoder
    */
  def google(persistentCacheFile: Option[Path] = None)(implicit config: Config) = geocoding.google.Geocoder(Api(persistentCacheFile))
}