package pkb.sync.converter.utils

import java.util.regex.{Matcher, Pattern}

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Model, ValueFactory}
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}

/**
  * @author Thomas Pellissier Tanon
  */
class GeoCoordinatesConverter(valueFactory: ValueFactory) extends StrictLogging {

  private val geoUriPattern = Pattern.compile("^geo:([+-]?\\d+(?:\\.\\d+)?),([+-]?\\d+(?:\\.\\d+)?)$")

  /**
    * Parses simple geo: URI following the pattern geo:LATITUDE,LONGITUDE
    */
  def convertGeoUri(geoUri: String, model: Model): Option[IRI] = {
    val matcher: Matcher = geoUriPattern.matcher(geoUri)
    if (!matcher.find) {
      logger.warn("The geo URI " + geoUri + " is invalid")
      return None
    }

    val latitude = matcher.group(1).toFloat
    val longitude = matcher.group(2).toFloat
    Some(convert(longitude, latitude, None, None, model))
  }

  /**
    * Creates a simple geo from a (longitude,latitude,elevation,accuracy) tuple
    */
  def convert(longitude: Double, latitude: Double, elevationOption: Option[Double], uncertaintyOption: Option[Float], model: Model): IRI = {
    val elevationSuffix = elevationOption.map(x => s",${x.toString}").getOrElse("")
    val accuracySuffix = uncertaintyOption.map(x => s";u=${x.toString}").getOrElse("")
    val geoResource = valueFactory.createIRI(s"geo:${latitude.toString},${longitude.toString}$elevationSuffix$accuracySuffix")
    model.add(geoResource, RDF.TYPE, SchemaOrg.GEO_COORDINATES)
    model.add(geoResource, SchemaOrg.LATITUDE, valueFactory.createLiteral(latitude))
    model.add(geoResource, SchemaOrg.LONGITUDE, valueFactory.createLiteral(longitude))
    uncertaintyOption.foreach {
      uncertainty =>
        model.add(geoResource, Personal.UNCERTAINTY, valueFactory.createLiteral(uncertainty))
    }
    elevationOption.foreach {
      elevation =>
        model.add(geoResource, SchemaOrg.ELEVATION, valueFactory.createLiteral(elevation))
    }
    geoResource
  }
}
