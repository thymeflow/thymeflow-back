package com.thymeflow.spatial.geocoding

import akka.Done
import akka.http.scaladsl.model.Uri
import akka.stream.scaladsl.Source
import com.thymeflow.actors._
import com.thymeflow.spatial
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.Span
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

/**
  * @author David Montoya
  */
class GeocoderSpec extends FlatSpec with Matchers with ScalaFutures {
  "PhotonGeocoder" should "reverse geocode" in {
    // Setup ENV VAR PHOTON_GEOCODER_TEST_URI before running this test
    // PHOTON_GEOCODER_TEST_URI = "http://localhost:2322
    sys.env.get("PHOTON_GEOCODER_TEST_URI").map(x => Geocoder.photon(Uri(x))).foreach {
      geocoder =>
        val concurrency = 2
        val addresses = Vector(
          "11 Rue de la Baume, 75008 Paris",
          "16 Passage Raguinot 75012 Paris",
          "75 Rue Lecourbe, 75015 Paris",
          "30 Avenue d'Iéna, 75116 Paris",
          "14 Avenue Claude Vellefaux, 75010 Paris",
          "14 Rue de Romainville, 75019 Paris",
          "25 Rue Plumet, 75015 Paris",
          "13 Boulevard de Magenta, 75010 Paris",
          "32 Rue Ganneron, 75018 Paris",
          "1 Rue Rivoli, Paris, France"
        )

        val searchPointsCount = 100
        val centerLatitude = 48.5
        val centerLongitude = 2.33
        val searchArcRadius = 1
        val generator = new Random

        val searchPoints = (for (i <- 1 to searchPointsCount) yield {
          spatial.geographic.Geography.point(centerLongitude + (generator.nextDouble() - 0.5) * 2 * searchArcRadius, centerLatitude + (generator.nextDouble() - 0.5) * 2 * searchArcRadius)
        }).toVector

        val addressQuerySource = Source.fromIterator(() => addresses.map(x => () => geocoder.direct(x)).iterator)
        val pointQuerySource = Source.fromIterator(() => searchPoints.map(x => () => geocoder.reverse(x)).iterator)

        val future = addressQuerySource.mapAsyncUnordered(concurrency)(_ ()).runForeach { _ => () }.flatMap {
          x => pointQuerySource.mapAsyncUnordered(concurrency)(_ ()).runForeach { _ => () }
        }
        future.futureValue(Timeout(Span.Max)) should equal(Done)


    }
  }
}
