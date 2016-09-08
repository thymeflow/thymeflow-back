package com.thymeflow.spatial.geocoding.google

import com.thymeflow.spatial.geocoding.{Feature => BaseFeature, Geocoder => BaseGeocoder}
import com.thymeflow.spatial.geographic.Point
import com.thymeflow.spatial.metric.Metric

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

/**
  * Combines Google Maps Geocoding API and Google Places API to convert geographic coordinates to places/addresses and vice versa.
  * Internally uses [[AddressGeocoder]] and [[PlacesGeocoder]]
  * Requires an [[Api]] instance to query Google's API.
  *
  * @author David Montoya
  */
class PlacesAddressGeocoder(api: Api,
                            metric: Metric[Point, Double],
                            locationBiasDistanceFilter: Double = 10000)(implicit executionContext: ExecutionContext) extends BaseGeocoder {
  private val addressGeocoder = new AddressGeocoder(api)
  private val placesGeocoder = new PlacesGeocoder(api)

  override def reverse(point: Point): Future[Traversable[BaseFeature]] = {
    addressGeocoder.reverse(point).flatMap {
      features =>
        features.headOption match {
          case Some(feature: Feature) if feature.geometry.location_type.exists(s => s == "ROOFTOP" || s == "RANGE_INTERPOLATED") && feature.name.nonEmpty =>
            placesGeocoder.direct(feature.name.get, feature.point).map {
              placeFeatures =>
                placeFeatures.filter {
                  placeFeature => placeFeature.point == feature.point
                } ++ features
            }
          case _ =>
            Future.successful(features)
        }
    }
  }

  override def direct(address: String): Future[Traversable[BaseFeature]] = {
    placesGeocoder.direct(address).flatMap {
      features =>
        if (features.isEmpty) {
          addressGeocoder.direct(address)
        } else {
          Future.successful(features)
        }
    }

  }

  @tailrec
  private def combineResults[T](results1: Vector[T],
                                results2: Vector[T],
                                cumulatedResults: Vector[T] = Vector.empty)(implicit ordering: Ordering[T]): Vector[T] = {
    (results1, results2) match {
      case (IndexedSeq(), _) => cumulatedResults ++ results2
      case (_, IndexedSeq()) => cumulatedResults ++ results1
      case (results1Head +: results1Tail, results2Head +: results2Tail) =>
        if (ordering.gt(results1Head, results2Head)) {
          combineResults(results1, results2Tail, cumulatedResults :+ results2Head)
        } else {
          combineResults(results1Tail, results2, cumulatedResults :+ results1Head)
        }
    }
  }

  override def direct(address: String, locationBias: Point): Future[Traversable[BaseFeature]] = {
    def featuresWithDistanceToLocationBias(features: Traversable[BaseFeature]) = {
      features.map {
        feature => (feature, metric.distance(feature.point, locationBias))
      }
    }
    placesGeocoder.direct(address, locationBias).zip(addressGeocoder.direct(address, locationBias)).map {
      case (placeFeatures, addressFeatures) =>
        implicit val ordering = new Ordering[(BaseFeature, Double)] {
          override def compare(x: (BaseFeature, Double), y: (BaseFeature, Double)): Int = x._2.compareTo(y._2)
        }
        combineResults(featuresWithDistanceToLocationBias(placeFeatures).toVector,
          featuresWithDistanceToLocationBias(addressFeatures).toVector).collect {
          case (feature, distance) if distance <= locationBiasDistanceFilter => feature
        }
    }
  }
}
