package com.thymeflow.spatial.geocoding

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import akka.stream.scaladsl.Source
import com.thymeflow.actors._
import com.thymeflow.spatial.SimpleAddress
import com.thymeflow.spatial.geographic.{Geography, Point}
import org.apache.commons.io.FileUtils
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures._
import org.scalatest.time.{Millis, Span}

import scala.concurrent.Future

/**
  * @author David Montoya
  */


class CachedGeocoderSpec extends FlatSpec with Matchers {
  private val rand = scala.util.Random
  implicit val patienceConfig = PatienceConfig(timeout = Span.Max, interval = scaled(Span(100, Millis)))

  private def generateFeatures(n: Int) = {
    (1 to n).map {
      _ => SimpleFeature(name = Some(s"Feature${rand.nextLong}"),
        point = Geography.point(rand.nextDouble(), rand.nextDouble),
        address = SimpleAddress(houseNumber = Some(rand.nextInt.toString),
          street = Some(s"Street ${rand.nextLong}"),
          locality = Some(s"Locality ${rand.nextLong}"),
          postalCode = Some(s"PostalCode ${rand.nextLong}"),
          region = Some(s"Region ${rand.nextLong}"),
          country = Some(s"Country ${rand.nextLong}")
        ),
        source = SimpleFeatureSource(isValid = rand.nextBoolean(), iri = s"http://www.example.com/geo/${rand.nextLong()}")
      )
    }
  }

  private def getCachedGeocoder(geocoder: Geocoder, disk: Boolean) = {
    val (pathOption, directoryOption) = if (disk) {
      val directory = Files.createTempDirectory(s"thymeflow-test-${UUID.randomUUID().toString}")
      (Some(Paths.get(directory.toString, "geocoder-cache")), Some(directory))
    } else {
      (None, None)
    }
    (new CachedGeocoder(geocoder, pathOption), directoryOption)
  }

  private def closeCachedGeocoder(cachedGeocoder: CachedGeocoder, directoryOption: Option[Path]) = {
    directoryOption.foreach {
      directory =>
        cachedGeocoder.close()
        FileUtils.deleteDirectory(directory.toFile)
    }
  }

  private def generateGeocoder(n: Int, r: Int) = {
    val reverseQueries = (1 to n).map {
      i => (Geography.point(i, i * 2), generateFeatures(r))
    }.toMap.withDefaultValue(Traversable.empty)

    val directQueries = (1 to n).map {
      i => (s"AddressQuery $i", generateFeatures(r))
    }.toMap.withDefaultValue(Traversable.empty)

    val directWithBiasQueries = (1 to n).map {
      i => ((s"AddressQuery $i", Geography.point(i, i * 2)), generateFeatures(r))
    }.toMap.withDefaultValue(Traversable.empty)

    val timesQueried = new AtomicInteger(0)
    (new Geocoder {
      override def reverse(point: Point): Future[Traversable[Feature]] = Future.successful({
        timesQueried.incrementAndGet()
        reverseQueries(point)
      })

      override def direct(address: String): Future[Traversable[Feature]] = Future.successful({
        timesQueried.incrementAndGet()
        directQueries(address)
      })

      override def direct(address: String, locationBias: Point): Future[Traversable[Feature]] = Future.successful({
        timesQueried.incrementAndGet()
        directWithBiasQueries((address, locationBias))
      })
    }, reverseQueries.keys, directQueries.keys, directWithBiasQueries.keys, () => timesQueried.get)
  }

  def testRetrieveFromCacheOnSecondQuery(disk: Boolean) = {
    val (geocoder, reverseQueries, directQueries, directWithBiasQueries, getTimesQueries) = generateGeocoder(100, 10)
    val (cachedGeocoder, directoryOption) = getCachedGeocoder(geocoder, disk = disk)
    val f1 = Source.fromIterator(() => reverseQueries.iterator).mapAsync(4)(cachedGeocoder.reverse)
    val f2 = Source.fromIterator(() => directQueries.iterator).mapAsync(4)(cachedGeocoder.direct)
    val f3 = Source.fromIterator(() => directWithBiasQueries.iterator).mapAsync(4) { case (x, bias) => cachedGeocoder.direct(x, bias) }
    val s = f1.concat(f2).concat(f3)
    whenReady(s.runForeach(x => ()).flatMap {
      _ =>
        val n = getTimesQueries()
        s.runForeach(x => ()).map {
          _ => (n, getTimesQueries())
        }
    }) {
      case (n1, n2) =>
        n1 should be(reverseQueries.size + directQueries.size + directWithBiasQueries.size)
        n1 should be(n2)
        closeCachedGeocoder(cachedGeocoder, directoryOption)
    }
  }

  def testRetrieveSameFeaturesAsOriginalGeocoder(disk: Boolean) = {
    val (geocoder, reverseQueries, directQueries, directWithBiasQueries, getTimesQueries) = generateGeocoder(100, 10)
    val (cachedGeocoder, directoryOption) = getCachedGeocoder(geocoder, disk = disk)
    val f1 = Source.fromIterator(() => reverseQueries.iterator).mapAsync(4)(x => geocoder.reverse(x).flatMap(r1 => cachedGeocoder.reverse(x).map(r2 => (r1, r2))))
    val f2 = Source.fromIterator(() => directQueries.iterator).mapAsync(4)(x => geocoder.direct(x).flatMap(r1 => cachedGeocoder.direct(x).map(r2 => (r1, r2))))
    val f3 = Source.fromIterator(() => directWithBiasQueries.iterator).mapAsync(4) { case (x, bias) => geocoder.direct(x, bias).flatMap(r1 => cachedGeocoder.direct(x, bias).map(r2 => (r1, r2))) }
    val r = f1.concat(f2).concat(f3).runFold(Vector.empty[(Traversable[Feature], Traversable[Feature])]) {
      case (v, f) => v :+ f
    }
    whenReady(r) {
      results =>
        results.foreach {
          case (r1, r2) => r1 should be(r2)
        }
        closeCachedGeocoder(cachedGeocoder, directoryOption)
    }
  }


  "DiskCachedGeocoder" should "retrieve from cache on second query" in {
    testRetrieveFromCacheOnSecondQuery(disk = true)
  }

  "CachedGeocoder" should "retrieve from cache on second query" in {
    testRetrieveFromCacheOnSecondQuery(disk = false)
  }

  "DiskCachedGeocoder" should "retrieve same Features as original geocoder" in {
    testRetrieveSameFeaturesAsOriginalGeocoder(disk = true)
  }

  "CachedGeocoder" should "retrieve same Features as original geocoder" in {
    testRetrieveSameFeaturesAsOriginalGeocoder(disk = false)
  }

}

