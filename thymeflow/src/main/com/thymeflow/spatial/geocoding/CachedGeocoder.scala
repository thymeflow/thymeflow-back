package com.thymeflow.spatial.geocoding

import java.nio.file.Path

import com.thymeflow.spatial.SimpleAddress
import com.thymeflow.spatial.geocoding.CachedGeocoder.CachedGeocoderProtocol
import com.thymeflow.spatial.geographic.Point
import com.thymeflow.spatial.geographic.impl.{Point => SimplePoint}
import com.thymeflow.utilities.Cached
import com.thymeflow.utilities.Cached.CachedMap
import com.typesafe.scalalogging.StrictLogging
import spray.json._

import scala.concurrent.{ExecutionContext, Future}


/**
  *
  * @author Thomas pellissier Tanon
  * @author David Montoya
  */
object CachedGeocoder {

  object CachedGeocoderProtocol extends DefaultJsonProtocol {
    implicit val pointFormat = jsonFormat(SimplePoint, "longitude", "latitude")
    implicit val simpleAddressFormat = jsonFormat6(SimpleAddress)
    implicit val simpleFeatureSourceFormat = jsonFormat2(SimpleFeatureSource)
    implicit val simpleFeatureFormat = jsonFormat(
      (name: Option[String], point: SimplePoint, address: SimpleAddress, source: SimpleFeatureSource) => SimpleFeature(name, point, address, source), "name", "point", "address", "source")
  }
}

class CachedGeocoder(geocoder: Geocoder, protected val persistentCachePath: Option[Path] = None)(implicit executionContext: ExecutionContext)
  extends Geocoder with Cached with StrictLogging {

  override protected def cacheName: String = "CachedGeocoder"

  import CachedGeocoderProtocol._

  private val reverseCache = getOrMakeHashMap[SimplePoint, Vector[SimpleFeature]]("geocoder-reverse-json")
  private val directCache = getOrMakeHashMap[String, Vector[SimpleFeature]]("geocoder-direct-json")
  private val directWithBiasCache = getOrMakeHashMap[(String, SimplePoint), Vector[SimpleFeature]]("geocoder-direct-bias-json")

  def featuresToSimpleFeatures(features: Traversable[Feature]) = {
    features.map {
      feature => SimpleFeature(name = feature.name,
        point = SimplePoint(feature.point.longitude, feature.point.latitude),
        address = SimpleAddress(feature.address.houseNumber,
          feature.address.street,
          feature.address.locality,
          feature.address.postalCode,
          feature.address.region, feature.address.country),
        source = SimpleFeatureSource(isValid = feature.source.isValid,
          iri = feature.source.iri)
      )
    }.toVector
  }

  private def getOrSetSimpleFeatures[K](cache: CachedMap[K, Vector[SimpleFeature]], set: K => Future[Traversable[Feature]])(key: K): Future[Traversable[Feature]] = {
    cache.getOrSet {
      (key: K) => set(key).map(featuresToSimpleFeatures)
    }(key)
  }

  override def reverse(point: Point): Future[Traversable[Feature]] = getOrSetSimpleFeatures(reverseCache, (point: Point) => geocoder.reverse(point).map(featuresToSimpleFeatures))(SimplePoint(point.longitude, point.latitude))

  override def direct(address: String): Future[Traversable[Feature]] = getOrSetSimpleFeatures(directCache, (address: String) => geocoder.direct(address).map(featuresToSimpleFeatures))(address)

  override def direct(address: String, locationBias: Point): Future[Traversable[Feature]] =
    getOrSetSimpleFeatures(directWithBiasCache, parentDirectWithTuple)((address, SimplePoint(locationBias.longitude, locationBias.latitude)))

  private def parentDirectWithTuple(tuple: (String, Point)): Future[Traversable[Feature]] = geocoder.direct(tuple._1, tuple._2).map(featuresToSimpleFeatures)
}
