package pkb.sync.converter.utils

import java.net.URI
import java.util.Locale

import org.apache.james.mime4j.field.address.AddressBuilder
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Model, ValueFactory}
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
class EmailAddressConverter(valueFactory: ValueFactory) {

  // RFC2822 matching non aText
  private val nonATextPattern =
    """[^A-Za-z0-9!#\$%&'*+\-/=?\^_`{|}~.]""".r

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
    val mailbox = AddressBuilder.DEFAULT.parseMailbox(address)
    Option(mailbox.getLocalPart) match {
      case Some(localPart) =>
        val domain = Option(mailbox.getDomain).getOrElse("")
        convert(localPart = localPart, domain = domain, model)
      case _ => throw new IllegalArgumentException(s"Invalid email address: $address")
    }
  }

  /**
    * Create a EmailAddress resource from an email address
    */
  def convert(localPart: String, domain: String, model: Model): IRI = {
    val domainLowerCase = domain.toLowerCase(Locale.ROOT)
    // Note: Non RFC compliant, but most servers consider localPart to be case insensitive
    val localPartLowerCase = localPart.toLowerCase(Locale.ROOT)
    val address =
      if (nonATextPattern.findFirstMatchIn(localPart).nonEmpty) {
        s""""$localPartLowerCase"@$domainLowerCase"""
      } else {
        s"$localPartLowerCase@$domainLowerCase"
      }
    val addressResource = valueFactory.createIRI("mailto:" + address)
    model.add(addressResource, RDF.TYPE, Personal.EMAIL_ADDRESS)
    model.add(addressResource, SchemaOrg.NAME, valueFactory.createLiteral(address))
    addressResource
  }
}
