package thymeflow.sync.converter

import java.io.{ByteArrayInputStream, InputStream}
import javax.mail.Message.RecipientType
import javax.mail.internet.{AddressException, InternetAddress, MimeMessage}
import javax.mail.{Address, Message, Multipart, Part}

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model._
import org.openrdf.model.vocabulary.RDF
import thymeflow.rdf.model.SimpleHashModel
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.sync.converter.utils.{EmailAddressConverter, EmailAddressNameConverter, EmailMessageUriConverter, UUIDConverter}

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

      Option(message.getSentDate).foreach(date =>
        model.add(messageResource, SchemaOrg.DATE_SENT, valueFactory.createLiteral(date), context)
      )
      Option(message.getReceivedDate).foreach(date =>
        model.add(messageResource, SchemaOrg.DATE_RECEIVED, valueFactory.createLiteral(date), context)
      )
      Option(message.getSubject).foreach(subject =>
        model.add(messageResource, SchemaOrg.HEADLINE, valueFactory.createLiteral(subject), context)
      )

      addAddresses({
        message.getFrom
      }, messageResource, SchemaOrg.SENDER)
      addAddresses({
        message.getRecipients(RecipientType.TO)
      }, messageResource, Personal.PRIMARY_RECIPIENT, SchemaOrg.RECIPIENT)
      addAddresses({
        message.getRecipients(RecipientType.CC)
      }, messageResource, Personal.COPY_RECIPIENT, SchemaOrg.RECIPIENT)
      addAddresses({
        message.getRecipients(RecipientType.BCC)
      }, messageResource, Personal.BLIND_COPY_RECIPIENT, SchemaOrg.RECIPIENT)

      message match {
        case mimeMessage: MimeMessage =>
          addMimeMessageExtra(mimeMessage, messageResource)
        case _ =>
      }

      messageResource
    }

    private def addAddresses(addresses: => Array[Address], messageResource: Resource, relations: IRI*): Unit = {
      try {
        // addresses() can return null
        Option(addresses).foreach(addresses =>
          addresses.foreach(address =>
            relations.foreach(relation =>
              convert(address).foreach(personResource => model.add(messageResource, relation, personResource, context))
            )
          )
        )
      } catch {
        case e: AddressException =>
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
        val personResource = uuidConverter.createIRI(address)
        model.add(personResource, RDF.TYPE, Personal.AGENT)
        Option(address.getPersonal).foreach(name =>
          emailAddressNameConverter.convert(name, address.getAddress).foreach(name =>
            model.add(personResource, SchemaOrg.NAME, name)
          )
        )
        model.add(personResource, SchemaOrg.EMAIL, emailAddressResource)
        personResource
      })
    }

    private def addMimeMessageExtra(message: MimeMessage, messageResource: Resource): Unit = {
      Option(message.getContentLanguage).foreach(_.foreach(language =>
        model.add(messageResource, SchemaOrg.IN_LANGUAGE, valueFactory.createLiteral(language), context) //TODO: is it valid ISO code? if yes, use good XSD type
      ))

      Option(message.getHeader("In-Reply-To", null)).map(inReplyTo =>
        model.add(messageResource, Personal.IN_REPLY_TO, emailMessageUriConverter.convert(inReplyTo), context)
      )

      //addPart(message, messageResource)
    }

    private def addPart(part: Part, messageResource: Resource): Unit = {
      if (hasAttachmentDisposition(part)) {
        return
      }

      if (part.isMimeType("message/rfc822")) {
        part.getContent match {
          case content: Message =>
            model.add(messageResource, SchemaOrg.HAS_PART, convert(content), context)
          case _ =>
            logger.warn("Badly encoded message/rfc822 message content")
        }
      } else if (part.isMimeType("multipart/*")) {
        part.getContent match {
          case content: Multipart =>
            (0 until content.getCount).foreach(i => {
              if (i == 1 || part.isMimeType("multipart/alternative")) {
                addPart(content.getBodyPart(i), messageResource)
              }
            })
          case _ =>
            logger.warn("Badly encoded multipart message content")
        }
      } else if (part.isMimeType("text/plain")) {
        part.getContent match {
          case content: String =>
            model.add(messageResource, SchemaOrg.TEXT, valueFactory.createLiteral(content), context)
          case _ =>
            logger.warn("Badly encoded text/plain message content")
        }
      } else {
        logger.info("Ignored email content type: " + part.getContentType)
      }
    }

    private def hasAttachmentDisposition(part: Part): Boolean = {
      Option(part.getDisposition).exists(_.equalsIgnoreCase(Part.ATTACHMENT))
    }

    private def resourceForMessage(message: Message): Resource = {
      message match {
        case mimeMessage: MimeMessage =>
          Option(mimeMessage.getMessageID).map(messageId =>
            emailMessageUriConverter.convert(messageId)
          ).getOrElse(valueFactory.createBNode())
        case _ => valueFactory.createBNode()
      }
    }
  }
}
