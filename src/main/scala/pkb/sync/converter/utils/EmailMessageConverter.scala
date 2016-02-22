package pkb.sync.converter.utils

import java.net.URI
import java.util.regex.Pattern

import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Model, ValueFactory}
import pkb.vocabulary.SchemaOrg

/**
  * @author Thomas Pellissier Tanon
  */
class EmailMessageConverter(valueFactory: ValueFactory) {

  private val messageIdPattern = Pattern.compile("^(?://)?(?:%3[cC]|<)(.*)(?:%3[eE]|>)$")

  /**
    * Creates a EmailMessage resource from a message: URI like "message:<fffffff@gmail.com>"
    */
  def convert(messageUri: URI, model: Model): IRI = {
    convert(messageUri.getSchemeSpecificPart, model)
  }

  /**
    * Creates a EmailMessage resource from a message id like "<fffffff@gmail.com>"
    */
  def convert(messageId: String, model: Model): IRI = {
    var cleanMessageId = messageId
    val matcher = messageIdPattern.matcher(cleanMessageId)
    if (matcher.find) {
      cleanMessageId = matcher.group(1)
    }
    val messageResource = valueFactory.createIRI("message:%c3" + cleanMessageId + "%3e")
    model.add(messageResource, RDF.TYPE, SchemaOrg.EMAIL_MESSAGE)
    messageResource
  }
}
