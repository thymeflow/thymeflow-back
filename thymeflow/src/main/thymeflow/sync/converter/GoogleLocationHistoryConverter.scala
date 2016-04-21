package thymeflow.sync.converter

import java.io.InputStream
import java.time.Instant

import com.fasterxml.jackson.databind.SerializationFeature
import com.typesafe.scalalogging.StrictLogging
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import org.openrdf.model.{IRI, Model, ValueFactory}
import pkb.rdf.model.SimpleHashModel
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}
import pkb.sync.converter.Converter
import pkb.utilities.ExceptionUtils

/**
  * @author David Montoya
  */

case class Location(timestampMs: String,
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

case class LocationHistory(locations: Seq[Location])

class GoogleLocationHistoryConverter(valueFactory: ValueFactory) extends Converter with StrictLogging {

  // Do not close inputStreams by default
  org.json4s.jackson.JsonMethods.mapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false)

  private implicit val formats = DefaultFormats

  // TODO: MappingException might be thrown by convert methods if JSON is invalid, should we wrap it?
  override def convert(str: String, context: IRI): Model = {
    convert(parse(str).extract[LocationHistory], context)
  }

  private def convert(locationHistory: LocationHistory, context: IRI): Model = {
    val model = new SimpleHashModel(valueFactory)
    val converter = new ToModelConverter(model, context)
    converter.convert(locationHistory)
    model
  }

  override def convert(inputStream: InputStream, context: IRI): Model = {
    try {
      convert(parse(inputStream).extract[LocationHistory], context)
    } catch {
      case e: MappingException =>
        logger.error(ExceptionUtils.getUnrolledStackTrace(e))
        new SimpleHashModel(valueFactory)
    }
  }

  private class ToModelConverter(model: Model, context: IRI) {
    def convert(locationHistory: LocationHistory): Unit = {
      locationHistory.locations.foreach {
        location => convert(location)
      }
    }

    def convert(location: Location): Unit = {
      location.time match {
        case Some(time) =>
          val geoCoordinatesNode = valueFactory.createBNode()
          val timeGeoLocationNode = valueFactory.createBNode()
          model.add(timeGeoLocationNode, RDF.TYPE, Personal.TIME_GEO_LOCATION, context)
          model.add(timeGeoLocationNode, SchemaOrg.DATE_CREATED, valueFactory.createLiteral(time.toString, XMLSchema.DATETIME), context)

          model.add(geoCoordinatesNode, RDF.TYPE, SchemaOrg.GEO_COORDINATES, context)
          model.add(timeGeoLocationNode, SchemaOrg.GEO, Personal.TIME_GEO_LOCATION, context)
          model.add(geoCoordinatesNode, SchemaOrg.LONGITUDE, valueFactory.createLiteral(location.longitude), context)
          model.add(geoCoordinatesNode, SchemaOrg.LATITUDE, valueFactory.createLiteral(location.latitude), context)
          location.accuracy.foreach {
            case accuracy =>
              model.add(geoCoordinatesNode, Personal.ACCURACY, valueFactory.createLiteral(accuracy), context)
          }
          // less frequent
          location.altitude.foreach {
            case altitude =>
              model.add(geoCoordinatesNode, SchemaOrg.ELEVATION, valueFactory.createLiteral(altitude), context)
          }
          // less frequent
          location.velocity.foreach {
            case magnitude =>
              val velocityNode = valueFactory.createBNode()
              model.add(velocityNode, RDF.TYPE, Personal.GEO_VECTOR, context)
              model.add(geoCoordinatesNode, Personal.VELOCITY, velocityNode, context)
              model.add(velocityNode, Personal.MAGNITUDE, valueFactory.createLiteral(magnitude), context)
              location.heading.foreach {
                case heading =>
                  model.add(geoCoordinatesNode, Personal.ANGLE, valueFactory.createLiteral(heading), context)
              }
          }
        case None =>
          logger.warn(s"Ignoring location with invalid timestamp {location=$location}.")
      }
    }
  }

}
