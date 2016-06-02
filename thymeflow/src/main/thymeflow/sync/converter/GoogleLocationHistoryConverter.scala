package thymeflow.sync.converter

import java.io.InputStream
import java.time.Instant

import com.fasterxml.jackson.databind.SerializationFeature
import com.typesafe.scalalogging.StrictLogging
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import org.openrdf.model.{Model, Resource, ValueFactory}
import thymeflow.rdf.model.SimpleHashModel
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.sync.converter.utils.GeoCoordinatesConverter
import thymeflow.utilities.ExceptionUtils

/**
  * @author David Montoya
  */
// The following classes (Location/LocationHistory) must not be within an object/class scope
// otherwise Json4s won't be able to parse extract them using Reflection.
private case class Location(timestampMs: String,
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

private case class LocationHistory(locations: Seq[Location])


class GoogleLocationHistoryConverter(valueFactory: ValueFactory) extends Converter with StrictLogging {

  private val geoCoordinatesConverter = new GeoCoordinatesConverter(valueFactory)
  // Do not close inputStreams by default
  org.json4s.jackson.JsonMethods.mapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false)

  private implicit val formats = DefaultFormats

  // TODO: MappingException might be thrown by convert methods if JSON is invalid, should we wrap it?
  override def convert(str: String, context: Resource): Model = {
    convert(parse(str).extract[LocationHistory], context)
  }

  override def convert(inputStream: InputStream, context: Resource): Model = {
    try {
      convert(parse(inputStream).extract[LocationHistory], context)
    } catch {
      case e: MappingException =>
        logger.error(ExceptionUtils.getUnrolledStackTrace(e))
        new SimpleHashModel(valueFactory)
    }
  }

  private def convert(locationHistory: LocationHistory, context: Resource): Model = {
    val model = new SimpleHashModel(valueFactory)
    val converter = new ToModelConverter(model, context)
    converter.convert(locationHistory)
    logger.info("Extraction of " + locationHistory.locations.size + " locations done.")
    model
  }

  private class ToModelConverter(model: Model, context: Resource) {
    def convert(locationHistory: LocationHistory): Unit = {
      locationHistory.locations.foreach(convert)
    }

    def convert(location: Location): Unit = {
      location.time match {
        case Some(time) =>
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
        case None =>
          logger.warn(s"Ignoring location with invalid timestamp {location=$location}.")
      }
    }
  }
}
