package com.thymeflow.sync.converter.utils

import java.net.{URI, URISyntaxException}

import org.eclipse.rdf4j.model.{IRI, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
class EmailMessageUriConverter(valueFactory: ValueFactory) {

  private val messageIdPattern = "^(?://)?(?:%3[cC]|<)(.*)(?:%3[eE]|>).*$".r

  /**
    * Creates an email URI from a message: URI like "message:<fffffff@gmail.com>" or "mid:fffff@gmail.com"
    */
  def convert(messageUri: URI): IRI = {
    convert(messageUri.getSchemeSpecificPart)
  }

  /**
    * Creates an email IRI from a message id like "<fffffff@gmail.com>" or "fffffff@gmail.com"
    */
  def convert(messageId: String): IRI = {
    var cleanMessageId = messageId
    cleanMessageId match {
      case messageIdPattern(id) => cleanMessageId = id
      case _ =>
        throw new IllegalArgumentException(s"Invalid email message id: $messageId")
    }
    try {
      val messageResource = valueFactory.createIRI(new URI("mid", cleanMessageId, null).toString)
      messageResource
    } catch {
      case _: URISyntaxException =>
        throw new IllegalArgumentException(s"Invalid email message id: $messageId")
    }
  }
}
