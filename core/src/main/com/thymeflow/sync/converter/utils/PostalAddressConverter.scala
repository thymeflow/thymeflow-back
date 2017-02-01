package com.thymeflow.sync.converter.utils

import com.thymeflow.rdf.model.vocabulary.SchemaOrg
import com.thymeflow.spatial.Address
import org.eclipse.rdf4j.model.vocabulary.RDF
import org.eclipse.rdf4j.model.{Model, Resource, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  *
  *         TODO: do address components normalization
  */
class PostalAddressConverter(valueFactory: ValueFactory) {

  private val uuidConverter = new UUIDConverter(valueFactory)

  def convert(address: Address, model: Model, context: Resource): Resource = {
    val addressResource = uuidConverter.createBNode(address)
    model.add(addressResource, RDF.TYPE, SchemaOrg.POSTAL_ADDRESS, context)
    val countryResource = address.country.map(country => {
      val countryResource = uuidConverter.createBNode("country:" + country)
      model.add(countryResource, RDF.TYPE, SchemaOrg.COUNTRY, context)
      model.add(countryResource, RDF.TYPE, SchemaOrg.PLACE, context)
      model.add(countryResource, SchemaOrg.NAME, valueFactory.createLiteral(country), context)
      model.add(addressResource, SchemaOrg.ADDRESS_COUNTRY, countryResource, context)
      countryResource
    })
    val regionResource = address.region.map(region => {
      val regionResource = uuidConverter.createBNode(
        countryResource.map(_.getID + "/").getOrElse("") + "region:" + region
      )
      model.add(regionResource, RDF.TYPE, SchemaOrg.PLACE, context)
      model.add(regionResource, SchemaOrg.NAME, valueFactory.createLiteral(region), context)
      model.add(addressResource, SchemaOrg.ADDRESS_REGION, regionResource, context)
      countryResource.map(model.add(regionResource, SchemaOrg.CONTAINED_IN_PLACE, _, context))
      regionResource
    })
    address.locality.foreach(locality => {
      val localityResource = uuidConverter.createBNode(
        regionResource.orElse(countryResource).map(_.getID + "/").getOrElse("") + "locality:" + locality
      )
      model.add(localityResource, RDF.TYPE, SchemaOrg.PLACE, context)
      model.add(localityResource, SchemaOrg.NAME, valueFactory.createLiteral(locality), context)
      model.add(addressResource, SchemaOrg.ADDRESS_LOCALITY, localityResource, context)
      countryResource.map(model.add(localityResource, SchemaOrg.CONTAINED_IN_PLACE, _, context))
      regionResource.map(model.add(localityResource, SchemaOrg.CONTAINED_IN_PLACE, _, context))
    })
    address.postalCode.foreach(postalCode =>
      model.add(addressResource, SchemaOrg.POSTAL_CODE, valueFactory.createLiteral(postalCode), context)
    )
    address.street.foreach(street =>
      model.add(addressResource, SchemaOrg.STREET_ADDRESS, valueFactory.createLiteral(
        address.houseNumber.map(_ + " ").getOrElse("") + street
      ), context)
    )
    addressResource
  }
}
