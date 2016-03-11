package pkb.sync.converter

import java.io.{ByteArrayInputStream, InputStream}
import javax.mail.Message.RecipientType
import javax.mail._
import javax.mail.internet.{AddressException, InternetAddress, MimeMessage}

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Model, Resource, ValueFactory}
import pkb.rdf.model.SimpleHashModel
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}
import pkb.sync.converter.utils.{EmailAddressConverter, EmailMessageUriConverter}

/**
  * @author Thomas Pellissier Tanon
  */
class EmailMessageConverter(valueFactory: ValueFactory) extends Converter with StrictLogging {

  private val emailAddressConverter = new EmailAddressConverter(valueFactory)
  private val emailMessageUriConverter = new EmailMessageUriConverter(valueFactory)

  def convert(stream: InputStream): Model = {
    convert(new MimeMessage(null, stream))
  }

  override def convert(str: String): Model = {
    convert(new MimeMessage(null, new ByteArrayInputStream(str.getBytes)))
  }

  def convert(message: Message): Model = {
    val model = new SimpleHashModel(valueFactory)
    convert(message, model)
    model
  }

  private def convert(message: Message, model: Model): Resource = {
    val messageResource = resourceForMessage(message, model)

    try {
      addAddresses(message.getFrom, messageResource, SchemaOrg.AUTHOR, model)
    } catch {
      case e: AddressException => ()
    }
    try {
      addAddresses(message.getRecipients(RecipientType.TO), messageResource, Personal.PRIMARY_RECIPIENT, model)
    } catch {
      case e: AddressException => ()
    }
    try {
      addAddresses(message.getRecipients(RecipientType.CC), messageResource, Personal.COPY_RECIPIENT, model)
    } catch {
      case e: AddressException => ()
    }
    try {
      addAddresses(message.getRecipients(RecipientType.BCC), messageResource, Personal.BLIND_COPY_RECIPIENT, model)
    } catch {
      case e: AddressException => ()
    }

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
      case mimeMessage: MimeMessage =>
        Option(mimeMessage.getMessageID).map(messageId =>
          emailMessageUriConverter.convert(mimeMessage.getMessageID, model)
        ).getOrElse(blankNodeForMessage(model))
      case _ => blankNodeForMessage(model)
    }
  }

  private def blankNodeForMessage(model: Model): Resource = {
    val messageResource = valueFactory.createBNode()
    model.add(messageResource, RDF.TYPE, SchemaOrg.EMAIL_MESSAGE)
    messageResource
  }

  def convert(messages: Traversable[Message]): Model = {
    val model = new SimpleHashModel(valueFactory)
    messages.foreach(message => convert(message, model))
    model
  }
}
