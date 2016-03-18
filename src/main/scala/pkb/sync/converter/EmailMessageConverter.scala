package pkb.sync.converter

import java.io.{ByteArrayInputStream, InputStream}
import javax.mail.Message.RecipientType
import javax.mail.internet.{AddressException, InternetAddress, MimeMessage}
import javax.mail.{Address, Message}

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Model, Resource, ValueFactory}
import pkb.rdf.model.SimpleHashModel
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}
import pkb.sync.converter.utils.{EmailAddressNameConverter, EmailAddressConverter, EmailMessageUriConverter}

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */

class EmailMessageConverter(valueFactory: ValueFactory) extends Converter with StrictLogging {

  private val emailAddressConverter = new EmailAddressConverter(valueFactory)
  private val emailAddressNameConverter = new EmailAddressNameConverter(valueFactory)
  private val emailMessageUriConverter = new EmailMessageUriConverter(valueFactory)

  def convert(stream: InputStream): Model = {
    convert(new MimeMessage(null, stream))
  }

  def convert(message: Message): Model = {
    val model = new SimpleHashModel(valueFactory)
    convert(message, model)
    model
  }

  private def convert(message: Message, model: Model): Resource = {
    val messageResource = resourceForMessage(message, model)

    // use sent date as published date. If sent date is invalid, use received date.
    (Option(message.getSentDate) match {
      case date@Some(_) => date
      case None => Option(message.getReceivedDate)
    }).foreach(date =>
      model.add(messageResource, SchemaOrg.DATE_PUBLISHED, valueFactory.createLiteral(date))
    )
    Option(message.getSubject).foreach(subject =>
      model.add(messageResource, SchemaOrg.HEADLINE, valueFactory.createLiteral(subject))
    )

    addAddresses({
      message.getFrom
    }, messageResource, SchemaOrg.AUTHOR, model)
    addAddresses({
      message.getRecipients(RecipientType.TO)
    }, messageResource, Personal.PRIMARY_RECIPIENT, model)
    addAddresses({
      message.getRecipients(RecipientType.CC)
    }, messageResource, Personal.COPY_RECIPIENT, model)
    addAddresses({
      message.getRecipients(RecipientType.BCC)
    }, messageResource, Personal.BLIND_COPY_RECIPIENT, model)

    messageResource
  }

  private def addAddresses(addresses: => Array[Address], messageResource: Resource, relation: IRI, model: Model): Unit = {
    try {
      // addresses() can return null
      Option(addresses).foreach(addresses =>
        addresses.foreach(address =>
          convert(address, model).foreach(personResource => model.add(messageResource, relation, personResource))
        )
      )
    } catch {
      case e: AddressException => None
    }
  }

  private def convert(address: Address, model: Model): Option[Resource] = {
    address match {
      case internetAddress: InternetAddress => convert(internetAddress, model)
      case _ =>
        logger.warn("Unknown address type: " + address.getType)
        None
    }
  }

  private def convert(address: InternetAddress, model: Model): Option[Resource] = {
    emailAddressConverter.convert(address.getAddress, model).map {
      emailAddressResource =>
        val personResource = valueFactory.createBNode
        Option(address.getPersonal).foreach(name =>
          emailAddressNameConverter.convert(address.getPersonal, address.getAddress).foreach{
            case name => model.add(personResource, SchemaOrg.NAME, name)
          }
        )
        model.add(personResource, SchemaOrg.EMAIL, emailAddressResource)
        personResource
    }
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

  override def convert(str: String): Model = {
    convert(new MimeMessage(null, new ByteArrayInputStream(str.getBytes)))
  }

  def convert(messages: Traversable[Message]): Model = {
    val model = new SimpleHashModel(valueFactory)
    messages.foreach(message => convert(message, model))
    model
  }
}
