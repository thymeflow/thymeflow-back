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

  "GeoCoordinatesConverter" should "not lose precision" in {
    val valueFactory = SimpleValueFactory.getInstance()
    val converter = new GeoCoordinatesConverter(valueFactory)
    val rand = new Random()

    (1 to 10000).foreach {
      i =>
        val longitude = BigDecimal(rand.nextDouble * 360d - 180d).setScale(i % 16, BigDecimal.RoundingMode.FLOOR).toDouble
        val latitude = BigDecimal(rand.nextDouble * 180d - 90d).setScale(i % 16, BigDecimal.RoundingMode.FLOOR).toDouble
        val elevation = if (rand.nextBoolean) Some(BigDecimal(rand.nextDouble * 20000d - 10000d).setScale(i % 16, BigDecimal.RoundingMode.FLOOR).toDouble) else None
        val uncertainty = if (rand.nextBoolean()) Some(BigDecimal(rand.nextDouble * 1000d).setScale(i % 16, BigDecimal.RoundingMode.FLOOR).toDouble) else None
        val model = new SimpleHashModel()
        val geo = converter.convert(longitude, latitude, elevation, uncertainty, model)
        geo.stringValue() should be(s"geo:${latitude.toString},${longitude.toString}${elevation.map("," + _.toString).getOrElse("")}${uncertainty.map(";u=" + _.toString).getOrElse("")}")
        logger.info(geo.stringValue())
    }
  }
}
