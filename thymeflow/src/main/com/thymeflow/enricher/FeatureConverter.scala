package com.thymeflow.enricher

import com.thymeflow.rdf.model.vocabulary.SchemaOrg
import com.thymeflow.spatial.geocoding.Feature
import com.thymeflow.sync.converter.utils.{GeoCoordinatesConverter, PostalAddressConverter}
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{Model, Resource, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
class FeatureConverter(valueFactory: ValueFactory) {

  private val geoCoordinatesConverter = new GeoCoordinatesConverter(valueFactory)
  private val postalAddressConverter = new PostalAddressConverter(valueFactory)

  def convert(feature: Feature, model: Model): Resource = {
    val placeResource = valueFactory.createIRI(feature.source.iri)

    model.add(placeResource, RDF.TYPE, SchemaOrg.PLACE)
    feature.name.foreach(name =>
      model.add(placeResource, SchemaOrg.NAME, valueFactory.createLiteral(name))
    )
    model.add(placeResource, SchemaOrg.ADDRESS, postalAddressConverter.convert(feature.address, model, null))
    model.add(placeResource, SchemaOrg.GEO, geoCoordinatesConverter.convert(feature.point.longitude, feature.point.latitude, None, None, model))

    placeResource
  }
}
