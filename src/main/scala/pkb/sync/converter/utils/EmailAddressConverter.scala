package pkb.sync.converter.utils

import java.net.URI

import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Model, ValueFactory}
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
class EmailAddressConverter(valueFactory: ValueFactory) {

  /**
    * Create a EmailAddress resource from a mailto: URI
    */
  def convert(mailtoUri: URI, model: Model): IRI = {
    convert(mailtoUri.getSchemeSpecificPart, model)
  }

  /**
    * Create a EmailAddress resource from an email address
    */
  def convert(address: String, model: Model): IRI = {
    val addressResource = valueFactory.createIRI("mailto:" + address.toLowerCase) //TODO: use David's code for normalization
    model.add(addressResource, RDF.TYPE, Personal.EMAIL_ADDRESS)
    model.add(addressResource, SchemaOrg.NAME, valueFactory.createLiteral(address))
    addressResource
  }
}
