package pkb.sync.converter

import java.io.{ByteArrayInputStream, InputStream}
import javax.mail.Message.RecipientType
import javax.mail.internet.{AddressException, InternetAddress, MimeMessage}
import javax.mail.{Address, Message}

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model._
import org.openrdf.model.vocabulary.RDF
import pkb.rdf.model.SimpleHashModel
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}
import pkb.sync.converter.utils.{EmailAddressConverter, EmailAddressNameConverter, EmailMessageUriConverter, UUIDConverter}

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */

class EmailMessageConverter(valueFactory: ValueFactory) extends Converter with StrictLogging {

  private val emailAddressConverter = new EmailAddressConverter(valueFactory)
  private val emailAddressNameConverter = new EmailAddressNameConverter(valueFactory)
  private val emailMessageUriConverter = new EmailMessageUriConverter(valueFactory)
  private val uuidConverter = new UUIDConverter(valueFactory)

  override def convert(stream: InputStream, context: IRI): Model = {
    convert(new MimeMessage(null, stream), context)
  }

  override def convert(str: String, context: IRI): Model = {
    convert(new MimeMessage(null, new ByteArrayInputStream(str.getBytes)), context)
  }

  def convert(message: Message, context: IRI): Model = {
    val model = new SimpleHashModel(valueFactory)
    val converter = new ToModelConverter(model, context)
    converter.convert(message)
    model
  }

  private class ToModelConverter(model: Model, context: IRI) {
    def convert(message: Message): Resource = {
      val messageResource = resourceForMessage(message)
      model.add(messageResource, RDF.TYPE, SchemaOrg.EMAIL_MESSAGE, context)

      // use sent date as published date. If sent date is invalid, use received date.
      (Option(message.getSentDate) match {
        case date@Some(_) => date
        case None => Option(message.getReceivedDate)
      }).foreach(date =>
        model.add(messageResource, SchemaOrg.DATE_PUBLISHED, valueFactory.createLiteral(date), context)
      )
      Option(message.getSubject).foreach(subject =>
        model.add(messageResource, SchemaOrg.HEADLINE, valueFactory.createLiteral(subject), context)
      )

      addAddresses({
        message.getFrom
      }, messageResource, SchemaOrg.AUTHOR)
      addAddresses({
        message.getRecipients(RecipientType.TO)
      }, messageResource, Personal.PRIMARY_RECIPIENT)
      addAddresses({
        message.getRecipients(RecipientType.CC)
      }, messageResource, Personal.COPY_RECIPIENT)
      addAddresses({
        message.getRecipients(RecipientType.BCC)
      }, messageResource, Personal.BLIND_COPY_RECIPIENT)

      messageResource
    }

    private def addAddresses(addresses: => Array[Address], messageResource: Resource, relation: IRI): Unit = {
      try {
        // addresses() can return null
        Option(addresses).foreach(addresses =>
          addresses.foreach(address =>
            convert(address).foreach(personResource => model.add(messageResource, relation, personResource, context))
          )
        )
      } catch {
        case e: AddressException => None
      }
    }

    private def convert(address: Address): Option[Resource] = {
      address match {
        case internetAddress: InternetAddress => convert(internetAddress)
        case _ =>
          logger.warn("Unknown address type: " + address.getType)
          None
      }
    }

    private def convert(address: InternetAddress): Option[Resource] = {
      emailAddressConverter.convert(address.getAddress, model).map(emailAddressResource => {
        val personResource = uuidConverter.create(address.toString)
        model.add(personResource, RDF.TYPE, Personal.AGENT)
        Option(address.getPersonal).foreach(name =>
          emailAddressNameConverter.convert(name, address.getAddress).foreach{
            case x => model.add(personResource, SchemaOrg.NAME, x)
          }
        )
        model.add(personResource, SchemaOrg.EMAIL, emailAddressResource)
        personResource
      })
    }

    private def resourceForMessage(message: Message): Resource = {
      message match {
        case mimeMessage: MimeMessage =>
          Option(mimeMessage.getMessageID).map(messageId =>
            emailMessageUriConverter.convert(mimeMessage.getMessageID)
          ).getOrElse(valueFactory.createBNode())
        case _ => valueFactory.createBNode()
      }
    }
  }
}
