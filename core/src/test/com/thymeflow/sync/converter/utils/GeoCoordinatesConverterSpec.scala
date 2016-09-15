package com.thymeflow.sync.converter.utils

import com.thymeflow.rdf.model.SimpleHashModel
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.impl.SimpleValueFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

/**
  * @author David Montoya
  */
class GeoCoordinatesConverterSpec extends FlatSpec with Matchers with StrictLogging {

  private val geoUriRegex = """^geo:(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)(?:,(-?\d+(?:\.\d+)?))?(?:;(?:crs=([\p{Alpha}\d-]+)))?(?:;(?:u=(\d+(?:\.\d+)?)))?""".r

  "GeoCoordinatesConverter" should "not lose precision" in {
    val rand = new Random()
    def generateRandomNumber(from: Double, to: Double, scale: Int) = {
      BigDecimal(rand.nextDouble * (to - from) + from).setScale(scale, BigDecimal.RoundingMode.FLOOR).toDouble
    }
    val valueFactory = SimpleValueFactory.getInstance()
    val converter = new GeoCoordinatesConverter(valueFactory)

    try {
      (1 to 10000).foreach {
        i =>
          val longitude = generateRandomNumber(-180d, 180d, i % 16)
          BigDecimal(rand.nextDouble * 360d - 180d).setScale(i % 16, BigDecimal.RoundingMode.FLOOR).toDouble
          val latitude = generateRandomNumber(-89d, 90d, i % 16)
          val elevation = if (rand.nextBoolean) Some(generateRandomNumber(0d, 20000d, i % 16)) else None
          val uncertainty = if (rand.nextBoolean) Some(generateRandomNumber(0d, 1000d, i % 16)) else None
          val model = new SimpleHashModel()
          val geo = converter.convert(longitude, latitude, elevation, uncertainty, model)
          withClue(geo.stringValue()) {
            geo.stringValue() match {
              case geoUriRegex(geoLatitude, geoLongitude, geoElevation, geoCrsn, geoUncertainty) =>
                geoLatitude.toDouble should be(latitude)
                geoLongitude.toDouble should be(longitude)
                Option(geoElevation).map(_.toDouble) should be(elevation)
                Option(geoUncertainty).map(_.toDouble) should be(uncertainty)
              case _ => fail(s"Could not parse 'geo' URI ${geo.stringValue()}")
            }
          }
      }
    } catch {
      case e: NumberFormatException =>
        fail(s"Could not parse number in 'geo' URI", e)
    }
  }
}
