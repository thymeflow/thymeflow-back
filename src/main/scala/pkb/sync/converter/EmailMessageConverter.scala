package pkb.sync.converter

import java.io.{File, FileInputStream}
import javax.mail.Message.RecipientType
import javax.mail._
import javax.mail.internet.{InternetAddress, MimeMessage}

import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Model, Resource, ValueFactory}
import org.slf4j.LoggerFactory
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}
import pkb.sync.converter.utils.{EmailAddressConverter, EmailMessageUriConverter}

/**
  * @author Thomas Pellissier Tanon
  */
class EmailMessageConverter(valueFactory: ValueFactory) {

  private val logger = LoggerFactory.getLogger(classOf[EmailMessageConverter])
  private val emailAddressConverter = new EmailAddressConverter(valueFactory)
  private val emailMessageUriConverter = new EmailMessageUriConverter(valueFactory)

  def convert(messages: Traversable[Message]): Model = {
    val model = new LinkedHashModel
    messages.foreach(message => convert(message, model))
    model
  }

  private def convert(message: Message, model: Model): Resource = {
    val messageResource = resourceForMessage(message, model)

    addAddresses(message.getFrom, messageResource, SchemaOrg.AUTHOR, model)
    addAddresses(message.getRecipients(RecipientType.TO), messageResource, Personal.PRIMARY_RECIPIENT, model)
    addAddresses(message.getRecipients(RecipientType.CC), messageResource, Personal.COPY_RECIPIENT, model)
    addAddresses(message.getRecipients(RecipientType.BCC), messageResource, Personal.BLIND_COPY_RECIPIENT, model)
    Option(message.getSentDate).foreach(date =>
      model.add(messageResource, SchemaOrg.DATE_PUBLISHED, valueFactory.createLiteral(date))
    )
    Option(message.getSubject).foreach(subject =>
      model.add(messageResource, SchemaOrg.HEADLINE, valueFactory.createLiteral(subject))
    )

    messageResource
  }

  private def addAddresses(addresses: Array[Address], messageResource: Resource, relation: IRI, model: Model): Unit = {
    Option(addresses).foreach(addresses =>
      addresses.foreach(address =>
        convert(address, model).foreach(personResource => model.add(messageResource, relation, personResource))
      )
    )
  }

  private def convert(address: Address, model: Model): Option[Resource] = {
    address match {
      case internetAddress: InternetAddress => Some(convert(internetAddress, model))
      case _ =>
        logger.warn("Unknown address type: " + address.getType)
        None
    }
  }

  private def convert(address: InternetAddress, model: Model): Resource = {
    val personResource = valueFactory.createBNode
    Option(address.getPersonal).foreach(name =>
      model.add(personResource, SchemaOrg.NAME, valueFactory.createLiteral(address.getPersonal))
    )
    Option(address.getAddress).foreach(email =>
      model.add(personResource, SchemaOrg.EMAIL, emailAddressConverter.convert(address.getAddress, model))
    )
    personResource
  }

  private def resourceForMessage(message: Message, model: Model): Resource = {
    message match {
      case mimeMessage: MimeMessage => emailMessageUriConverter.convert(mimeMessage.getMessageID, model)
      case _ =>
        val messageResource = valueFactory.createBNode()
        model.add(messageResource, RDF.TYPE, SchemaOrg.EMAIL_MESSAGE)
        messageResource
    }
  }

  def convert(file: File): Model = {
    convert(new MimeMessage(null, new FileInputStream(file)))
  }

  def convert(message: Message): Model = {
    val model = new LinkedHashModel
    convert(message, model)
    model
  }
}
