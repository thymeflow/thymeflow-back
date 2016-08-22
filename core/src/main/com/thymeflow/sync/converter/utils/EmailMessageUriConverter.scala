package com.thymeflow.sync.converter.utils

import java.net.{URI, URISyntaxException}
import java.util.regex.Pattern

import org.openrdf.model.{IRI, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
class EmailMessageUriConverter(valueFactory: ValueFactory) {

  private val messageIdPattern = Pattern.compile("^(?://)?(?:%3[cC]|<)(.*)(?:%3[eE]|>).*$")

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
    val matcher = messageIdPattern.matcher(cleanMessageId)
    if (matcher.find) {
      cleanMessageId = matcher.group(1)
    } else {
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
