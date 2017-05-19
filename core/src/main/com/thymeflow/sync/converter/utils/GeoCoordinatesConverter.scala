package com.thymeflow.sync.converter.utils

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.Locale

import com.thymeflow.rdf.model.StatementSet
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.typesafe.scalalogging.StrictLogging
import ezvcard.util.GeoUri
import org.eclipse.rdf4j.model.vocabulary.{GEO, RDF}
import org.eclipse.rdf4j.model.{IRI, ValueFactory}

import scala.language.implicitConversions

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
class GeoCoordinatesConverter(valueFactory: ValueFactory) extends StrictLogging {

  /**
    * Parses geo: URI
    */
  def convertGeoUri(geoUri: String, statements: StatementSet): Option[IRI] = {
    implicit def doubleToOption(double: java.lang.Double): Option[Double] = Option(double).map(_.doubleValue())

    try {
      val uri = GeoUri.parse(geoUri)
      Some(convert(uri.getCoordB, uri.getCoordA, uri.getCoordC, uri.getUncertainty, statements))
    } catch {
      case _: IllegalArgumentException =>
        logger.warn(s"The geo URI $geoUri is invalid")
        None
    }
  }

  val decimalFormat = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ROOT))
  decimalFormat.setMaximumFractionDigits(340)

  def format(d: Double) = decimalFormat.format(d)

  /**
    * Creates a simple geo from a (longitude,latitude,elevation,accuracy) tuple
    */
  def convert(longitude: Double, latitude: Double, elevationOption: Option[Double], uncertaintyOption: Option[Double], statements: StatementSet): IRI = {
    val elevationSuffix = elevationOption.map(x => s",${format(x)}").getOrElse("")
    val uncertaintySuffix = uncertaintyOption.map(x => s";u=${format(x)}").getOrElse("")
    val geoResource = valueFactory.createIRI(s"geo:${format(latitude)},${format(longitude)}$elevationSuffix$uncertaintySuffix")

    val uriBuilder = new GeoUri.Builder(latitude, longitude)
    elevationOption.foreach(uriBuilder.coordC(_))
    uncertaintyOption.foreach(uriBuilder.uncertainty(_))
    statements.add(geoResource, RDF.TYPE, SchemaOrg.GEO_COORDINATES, geoResource)
    statements.add(geoResource, SchemaOrg.LATITUDE, valueFactory.createLiteral(latitude), geoResource)
    statements.add(geoResource, SchemaOrg.LONGITUDE, valueFactory.createLiteral(longitude), geoResource)
    uncertaintyOption.foreach(uncertainty =>
      statements.add(geoResource, Personal.UNCERTAINTY, valueFactory.createLiteral(uncertainty), geoResource)
    )
    elevationOption.foreach(elevation =>
      statements.add(geoResource, SchemaOrg.ELEVATION, valueFactory.createLiteral(elevation), geoResource)
    )
    statements.add(
      geoResource,
      GEO.AS_WKT,
      valueFactory.createLiteral(s"POINT (${longitude.toString} ${latitude.toString})", GEO.WKT_LITERAL),
      geoResource
    )
    geoResource
  }
}
