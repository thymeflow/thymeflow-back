package thymeflow.sync.converter

import java.io.InputStream
import java.time.Instant

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import org.openrdf.model.{Model, Resource, ValueFactory}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsValue, JsonParser}
import thymeflow.rdf.model.SimpleHashModel
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.sync.converter.utils.GeoCoordinatesConverter
import thymeflow.utilities.ExceptionUtils

/**
  * @author David Montoya
  */
class GoogleLocationHistoryConverter(valueFactory: ValueFactory) extends Converter with StrictLogging with DefaultJsonProtocol {

  private val geoCoordinatesConverter = new GeoCoordinatesConverter(valueFactory)

  implicit val locationFormat = jsonFormat7(Location)
  implicit val locationHistoryFormat = jsonFormat1(LocationHistory)

  override def convert(str: String, context: Resource): Model = {
    convert(JsonParser(str), context)
  }

  override def convert(inputStream: InputStream, context: Resource): Model = {
    convert(JsonParser(IOUtils.toByteArray(inputStream)), context)
  }

  private def convert(json: JsValue, context: Resource): Model = {
    try {
      convert(json.convertTo[LocationHistory], context)
    } catch {
      case e: DeserializationException =>
        logger.error(ExceptionUtils.getUnrolledStackTrace(e))
        new SimpleHashModel(valueFactory)
    }
  }

  private def convert(locationHistory: LocationHistory, context: Resource): Model = {
    val model = new SimpleHashModel(valueFactory)
    val converter = new ToModelConverter(model, context)
    converter.convert(locationHistory)
    logger.info("Extraction of " + locationHistory.locations.length + " locations done.")
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