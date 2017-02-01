package com.thymeflow.sync.converter

import java.io.InputStream
import java.time.Instant
import java.time.temporal.ChronoUnit

import com.thymeflow.rdf.model.SimpleHashModel
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.thymeflow.sync.converter.utils.GeoCoordinatesConverter
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.eclipse.rdf4j.model.vocabulary.{RDF, XMLSchema}
import org.eclipse.rdf4j.model.{IRI, Model, Resource, ValueFactory}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsValue, JsonParser}

/**
  * @author David Montoya
  */
class GoogleLocationHistoryConverter(valueFactory: ValueFactory)(implicit config: Config) extends Converter with StrictLogging with DefaultJsonProtocol {

  private val geoCoordinatesConverter = new GeoCoordinatesConverter(valueFactory)

  implicit val locationFormat = jsonFormat7(Location)
  implicit val locationHistoryFormat = jsonFormat1(LocationHistory)

  override def convert(stream: InputStream, context: Option[String] => IRI, createSourceContext: (Model, String) => IRI): Iterator[(IRI, Model)] = {
    convert(JsonParser(IOUtils.toByteArray(stream)), context)
  }

  private def convert(json: JsValue, context: Option[String] => IRI): Iterator[(IRI, Model)] = {
    try {
      convert(json.convertTo[LocationHistory], context)
    } catch {
      case e: DeserializationException =>
        logger.error("Error parsing LocationHistory from JSON value.", e)
        Iterator.empty
    }
  }

  private def convert(locationHistory: LocationHistory, context: Option[String] => IRI): Iterator[(IRI, Model)] = {
    val locationsGroupedByDay = locationHistory.locations.view.map {
      location => (location.time, location)
    }.collect {
      case (Some(time), location) => (time, location)
    }.groupBy {
      case (time, _) => time.truncatedTo(ChronoUnit.DAYS)
    }
    logger.info(s"Converting ${locationHistory.locations.length} locations...")
    locationsGroupedByDay.iterator.map {
      case (day, locations) =>
        val dayContext = context(Some(day.toString))
        val model = new SimpleHashModel(valueFactory)
        val converter = new ToModelConverter(model, dayContext)
        converter.convert(locations)
        (dayContext, model)
    }
  }

  private class ToModelConverter(model: Model, context: Resource) {
    def convert(locations: Traversable[(Instant, Location)]): Unit = {
      locations.foreach {
        case (time, location) => convert(time, location)
      }
    }

    def convert(time: Instant, location: Location): Unit = {
      val geoCoordinatesNode = geoCoordinatesConverter.convert(location.longitude, location.latitude, location.altitude, location.accuracy.map(_.toDouble), model)
      val timeGeoLocationNode = valueFactory.createBNode()
      model.add(timeGeoLocationNode, RDF.TYPE, Personal.LOCATION, context)
      model.add(timeGeoLocationNode, SchemaOrg.GEO, geoCoordinatesNode, context)
      model.add(timeGeoLocationNode, Personal.TIME, valueFactory.createLiteral(time.toString, XMLSchema.DATETIME), context)

      // less frequent
      location.velocity.foreach(magnitude => {
        val velocityNode = valueFactory.createBNode()
        model.add(velocityNode, RDF.TYPE, Personal.GEO_VECTOR, context)
        model.add(geoCoordinatesNode, Personal.VELOCITY, velocityNode, context)
        model.add(velocityNode, Personal.MAGNITUDE, valueFactory.createLiteral(magnitude), context)
        location.heading.foreach(heading =>
          model.add(geoCoordinatesNode, Personal.ANGLE, valueFactory.createLiteral(heading), context)
        )
      })
    }
  }
}

private[converter] case class Location(timestampMs: String,
                                       latitudeE7: Long,
                                       longitudeE7: Long,
                                       accuracy: Option[Float],
                                       velocity: Option[Float],
                                       altitude: Option[Double],
                                       heading: Option[Float]) {
  def longitude = longitudeE7 / 1e7

  def latitude = latitudeE7 / 1e7

  def time =
    try {
      Some(Instant.ofEpochMilli(timestampMs.toLong))
    } catch {
      case e: NumberFormatException => None
    }
}

private[converter] case class LocationHistory(locations: Array[Location])