package com.thymeflow.sync.converter.utils

import java.net.URI
import java.util.Locale

import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.typesafe.scalalogging.StrictLogging
import org.apache.james.mime4j.field.address.AddressBuilder
import org.eclipse.rdf4j.model.vocabulary.RDF
import org.eclipse.rdf4j.model.{IRI, Model, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
object EmailAddressConverter {
  // RFC2822 matching non aText
  val nonATextPattern = """[^A-Za-z0-9!#\$%&'*+\-/=?\^_`{|}~.]""".r

  def concatenateLocalPartAndDomain(localPart: String, domain: String) = {
    // TODO: Check if this is the relevant method
    // We must check that localPart is not already escaped ...
    if (EmailAddressConverter.nonATextPattern.findFirstMatchIn(localPart).nonEmpty) {
      s""""$localPart"@$domain"""
    } else {
      s"$localPart@$domain"
    }
  }
}

class EmailAddressConverter(valueFactory: ValueFactory) extends StrictLogging {

  /**
    * Create a EmailAddress resource from a mailto: URI
    */
  def convert(mailtoUri: URI, model: Model): Option[IRI] = {
    convert(mailtoUri.getSchemeSpecificPart, model)
  }

  /**
    * Create a EmailAddress resource from an email address
    */
  def convert(address: String, model: Model): Option[IRI] = {
    if (address == "undisclosed-recipients:;") {
      None
    } else {
      try {
        Some(AddressBuilder.DEFAULT.parseMailbox(address)).flatMap(x => Option(x.getLocalPart).map((x, _))) match {
          case Some((mailbox, localPart)) =>
            val domain = Option(mailbox.getDomain).getOrElse("")
            convert(localPart = localPart, domain = domain, model)
          case _ =>
            None
        }
      } catch {
        case ex: Exception =>
          logger.warn(s"Could not parse email address: $address")
          None
      }
    }
  }

  /**
    * Create a EmailAddress resource from an email address given by its localPart and domain
    */
  def convert(localPart: String, domain: String, model: Model): Option[IRI] = {
    val domainLowerCase = domain.toLowerCase(Locale.ROOT)
    // Note: Non RFC compliant, but most servers consider localPart to be case insensitive
    val localPartLowerCase = localPart.toLowerCase(Locale.ROOT)
    val address = EmailAddressConverter.concatenateLocalPartAndDomain(localPartLowerCase, domainLowerCase)
    val addressResource = valueFactory.createIRI("mailto:" + address)
    model.add(addressResource, Personal.LOCAL_PART, valueFactory.createLiteral(localPartLowerCase))
    model.add(addressResource, Personal.DOMAIN, valueFactory.createLiteral(domain))
    model.add(addressResource, RDF.TYPE, Personal.EMAIL_ADDRESS)
    model.add(addressResource, SchemaOrg.NAME, valueFactory.createLiteral(address))
    Some(addressResource)
  }
}
