package thymeflow.sync.converter.utils

import java.net.URI
import java.util.regex.Pattern

import org.openrdf.model.{IRI, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
class EmailMessageUriConverter(valueFactory: ValueFactory) {

  private val messageIdPattern = Pattern.compile("^(?://)?(?:%3[cC]|<)(.*)(?:%3[eE]|>)$")

  /**
    * Creates a EmailMessage resource from a message: URI like "message:<fffffff@gmail.com>"
    */
  def convert(messageUri: URI): IRI = {
    convert(messageUri.getSchemeSpecificPart)
  }

  /**
    * Creates a EmailMessage resource from a message id like "<fffffff@gmail.com>"
    */
  def convert(messageId: String): IRI = {
    var cleanMessageId = messageId
    val matcher = messageIdPattern.matcher(cleanMessageId)
    if (matcher.find) {
      cleanMessageId = matcher.group(1)
    }
    val messageResource = valueFactory.createIRI("message:%c3" + cleanMessageId + "%3e")
    messageResource
  }
}
