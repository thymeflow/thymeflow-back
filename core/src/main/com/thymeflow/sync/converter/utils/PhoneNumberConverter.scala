package com.thymeflow.sync.converter.utils

import java.net.URI

import com.google.i18n.phonenumbers.{NumberParseException, PhoneNumberUtil, Phonenumber}
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.typesafe.scalalogging.StrictLogging
import org.eclipse.rdf4j.model.vocabulary.RDF
import org.eclipse.rdf4j.model.{IRI, Model, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
class PhoneNumberConverter(valueFactory: ValueFactory, defaultRegion: String) extends StrictLogging {

  private val phoneUtil = PhoneNumberUtil.getInstance

  def convert(telUri: URI, model: Model): Option[IRI] = {
    convert(telUri.toString, model)
  }

  def convert(rawNumber: String, model: Model): Option[IRI] = {
    parseNumber(rawNumber).map(number => {
      val phoneNumberResource = valueFactory.createIRI(phoneUtil.format(number, PhoneNumberUtil.PhoneNumberFormat.RFC3966))
      model.add(phoneNumberResource, RDF.TYPE, Personal.PHONE_NUMBER)
      model.add(phoneNumberResource, RDF.TYPE, classForPhoneNumberType(phoneUtil.getNumberType(number)))
      model.add(phoneNumberResource, SchemaOrg.NAME, valueFactory.createLiteral(phoneUtil.format(number, PhoneNumberUtil.PhoneNumberFormat.INTERNATIONAL)))
      phoneNumberResource
    })
  }

  def buildTelUri(rawNumber: String): Option[String] = {
    parseNumber(rawNumber).map(phoneUtil.format(_, PhoneNumberUtil.PhoneNumberFormat.RFC3966))
  }

  private def parseNumber(rawNumber: String): Option[Phonenumber.PhoneNumber] = {
    try {
      val number: Phonenumber.PhoneNumber = phoneUtil.parse(rawNumber, defaultRegion)
      if (!phoneUtil.isValidNumber(number)) {
        logger.warn("The telephone number " + rawNumber + " is invalid")
        return None
      }
      Some(number)
    } catch {
      case e: NumberParseException =>
        logger.warn("The telephone number " + rawNumber + " is invalid", e)
        None
    }
  }

  private def classForPhoneNumberType(phoneNumberType: PhoneNumberUtil.PhoneNumberType): IRI = {
    phoneNumberType match {
      case PhoneNumberUtil.PhoneNumberType.MOBILE => Personal.CELLPHONE_NUMBER
      case _ => Personal.PHONE_NUMBER
    }
  }
}
