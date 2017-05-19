package com.thymeflow.enricher

import com.thymeflow.rdf.model.StatementSet
import com.thymeflow.rdf.model.vocabulary.SchemaOrg
import com.thymeflow.spatial.geocoding.Feature
import com.thymeflow.sync.converter.utils.{GeoCoordinatesConverter, PostalAddressConverter}
import org.eclipse.rdf4j.model.vocabulary.RDF
import org.eclipse.rdf4j.model.{Resource, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
class FeatureConverter(valueFactory: ValueFactory) {

  private val geoCoordinatesConverter = new GeoCoordinatesConverter(valueFactory)
  private val postalAddressConverter = new PostalAddressConverter(valueFactory)

  def convert(feature: Feature, statements: StatementSet): Resource = {
    val placeResource = valueFactory.createIRI(feature.source.iri)

    statements.add(placeResource, RDF.TYPE, SchemaOrg.PLACE, placeResource)
    feature.name.foreach(name =>
      statements.add(placeResource, SchemaOrg.NAME, valueFactory.createLiteral(name), placeResource)
    )
    statements.add(placeResource, SchemaOrg.ADDRESS, postalAddressConverter.convert(feature.address, statements, null), placeResource)
    statements.add(placeResource, SchemaOrg.GEO, geoCoordinatesConverter.convert(feature.point.longitude, feature.point.latitude, None, None, statements), placeResource)

    placeResource
  }
}
