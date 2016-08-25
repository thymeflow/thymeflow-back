package com.thymeflow.sync.converter.utils

import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.typesafe.scalalogging.StrictLogging
import ezvcard.util.GeoUri
import org.openrdf.model.vocabulary.{GEO, RDF}
import org.openrdf.model.{IRI, Model, ValueFactory}

import scala.language.implicitConversions

/**
  * @author Thomas Pellissier Tanon
  */
class GeoCoordinatesConverter(valueFactory: ValueFactory) extends StrictLogging {

  /**
    * Parses geo: URI
    */
  def convertGeoUri(geoUri: String, model: Model): Option[IRI] = {
    implicit def doubleToOption(double: java.lang.Double): Option[Double] = Option(double).map(_.doubleValue())

    try {
      val uri = GeoUri.parse(geoUri)
      Some(convert(uri.getCoordB, uri.getCoordA, uri.getCoordC, uri.getUncertainty, model))
    } catch {
      case _: IllegalArgumentException =>
        logger.warn(s"The geo URI $geoUri is invalid")
        None
    }
  }

  /**
    * Creates a simple geo from a (longitude,latitude,elevation,accuracy) tuple
    */
  def convert(longitude: Double, latitude: Double, elevationOption: Option[Double], uncertaintyOption: Option[Double], model: Model): IRI = {
    val uriBuilder = new GeoUri.Builder(latitude, longitude)
    elevationOption.foreach(uriBuilder.coordC(_))
    uncertaintyOption.foreach(uriBuilder.uncertainty(_))

    val geoResource = valueFactory.createIRI(uriBuilder.build().toString(Integer.MAX_VALUE))
    model.add(geoResource, RDF.TYPE, SchemaOrg.GEO_COORDINATES)
    model.add(geoResource, SchemaOrg.LATITUDE, valueFactory.createLiteral(latitude))
    model.add(geoResource, SchemaOrg.LONGITUDE, valueFactory.createLiteral(longitude))
    uncertaintyOption.foreach(uncertainty =>
        model.add(geoResource, Personal.UNCERTAINTY, valueFactory.createLiteral(uncertainty))
    )
    elevationOption.foreach(elevation =>
        model.add(geoResource, SchemaOrg.ELEVATION, valueFactory.createLiteral(elevation))
    )
    model.add(
      geoResource,
      GEO.AS_WKT,
      valueFactory.createLiteral(s"POINT ($longitude $latitude)", GEO.WKT_LITERAL)
    )
    geoResource
  }
}
