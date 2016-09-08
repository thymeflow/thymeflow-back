package com.thymeflow.spatial.geocoding.google

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.thymeflow.spatial.geographic.metric.models.WGS84SphereHaversinePointMetric
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext

/**
  * @author David Montoya
  */
object Geocoder {

  def apply(api: Api)(implicit actorSystem: ActorSystem,
                      materializer: Materializer,
                      executionContext: ExecutionContext,
                      config: Config) = {
    new PlacesAddressGeocoder(api: Api, WGS84SphereHaversinePointMetric)
  }

}
