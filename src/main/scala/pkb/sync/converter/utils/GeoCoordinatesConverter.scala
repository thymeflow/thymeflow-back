package pkb.sync.converter.utils

import java.util.regex.{Matcher, Pattern}

import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Model, ValueFactory}
import org.slf4j.{Logger, LoggerFactory}
import pkb.vocabulary.SchemaOrg

/**
  * @author Thomas Pellissier Tanon
  */
class GeoCoordinatesConverter(valueFactory: ValueFactory) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[GeoCoordinatesConverter])

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
    val geoResource = valueFactory.createIRI("geo:" + latitude + "," + longitude)
    model.add(geoResource, RDF.TYPE, SchemaOrg.GEO_COORDINATES)
    model.add(geoResource, SchemaOrg.LATITUDE, valueFactory.createLiteral(latitude))
    model.add(geoResource, SchemaOrg.LONGITUDE, valueFactory.createLiteral(longitude))
    Some(geoResource)
  }
}
