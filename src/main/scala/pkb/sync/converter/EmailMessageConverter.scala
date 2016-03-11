package pkb.sync.converter

import java.io.{ByteArrayInputStream, InputStream}
import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal
import java.util.Locale
import javax.mail.Message.RecipientType
import javax.mail.{Address, Message}
import javax.mail.internet.{InternetAddress, AddressException, MimeMessage}
import org.apache.james.mime4j.field.address.AddressBuilder

import scala.collection.JavaConverters._
import com.typesafe.scalalogging.StrictLogging
import org.apache.james.mime4j.codec.{DecodeMonitor, DecoderUtil}
import org.apache.james.mime4j.dom.address.{Group, Mailbox}
import org.apache.james.mime4j.field.AddressListFieldImpl
import org.apache.james.mime4j.stream._
import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Model, Resource, ValueFactory}
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}
import pkb.sync.converter.utils.{EmailAddressConverter, EmailMessageUriConverter}
import pkb.utilities.mail.LenientDateParser

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */

case class EmailAddress(name: Option[String],
                        localPart: String,
                        domain: String)

case class EmailMessage(messageId: Option[String],
                        subject: Option[String],
                        date: Option[Temporal],
                        from: Seq[EmailAddress],
                        to: Seq[EmailAddress],
                        cc: Seq[EmailAddress],
                        bcc: Seq[EmailAddress])

class EmailMessageConverter(valueFactory: ValueFactory) extends Converter with StrictLogging {

  private val emailAddressConverter = new EmailAddressConverter(valueFactory)
  private val emailMessageUriConverter = new EmailMessageUriConverter(valueFactory)

  override def convert(str: String): Model = {
    convert(new ByteArrayInputStream(str.getBytes))
  }

  def convert(inputStream: InputStream): Model = {
    val config = new MimeConfig()
    config.setMaxLineLen(-1)
    config.setMaxHeaderLen(-1)
    val mimeTokenStream = new MimeTokenStream(config)
    mimeTokenStream.parse(inputStream)
    convert(mimeTokenStream)
  }

  def convert(mimeTokenStream: MimeTokenStream): Model = {
    val model = new LinkedHashModel
    convert(mimeTokenStream, model)
    model
  }

  private def convert(mimeTokenStream: MimeTokenStream, model: Model): Resource = {
    var state = mimeTokenStream.getState
    var deliveryDateOption: Option[Temporal] = None
    var dateOption: Option[Temporal] = None

    var messageId: Option[String] = None
    var to: Seq[EmailAddress] = Vector()
    var from: Seq[EmailAddress] = Vector()
    var bcc: Seq[EmailAddress] = Vector()
    var cc: Seq[EmailAddress] = Vector()
    var subjectOption: Option[String] = None

    def decode(body: String) = {
      DecoderUtil.decodeEncodedWords(body, DecodeMonitor.SILENT)
    }

    while (state ne EntityState.T_END_OF_STREAM) {
      {
        state match {
          case EntityState.T_FIELD =>
            val field = mimeTokenStream.getField
            val fieldName = field.getName.toLowerCase(Locale.ROOT)
            fieldName match {
              case "message-id" =>
                messageId = Some(field.getBody)
              case "delivery-date" =>
                deliveryDateOption = parseDate(field)
              case "date" =>
                dateOption = parseDate(field)
              case "to" | "from" | "cc" | "bcc" =>
                def emailAddress(mailbox: Mailbox) = {
                  Option(mailbox.getLocalPart).map {
                    case localPart =>
                      val domain = Option(mailbox.getDomain).getOrElse("")
                      val name = Option(mailbox.getName)
                      EmailAddress(name = name,
                        localPart = localPart,
                        domain = domain)
                  }
                }
                val addressList = AddressListFieldImpl.PARSER.parse(field, DecodeMonitor.SILENT).getAddressList
                if (addressList != null) {
                  val contacts = addressList.asScala.flatMap {
                    case group: Group =>
                      for (mailbox <- group.getMailboxes.asScala;
                           contact <- emailAddress(mailbox)) yield {
                        contact
                      }
                    case mailbox: Mailbox =>
                      emailAddress(mailbox)
                  }
                  fieldName match {
                    case "from" => from ++= contacts
                    case "to" => to ++= contacts
                    case "bcc" => bcc ++= contacts
                    case "cc" => cc ++= contacts
                  }
                }
              case "subject" =>
                subjectOption = Some(decode(field.getBody))
              case _ =>
            }
          case _ =>
        }
      }
      state = mimeTokenStream.next
    }
    val adjustedDateOption = adjustDeliveryDate(deliveryDateOption, dateOption)
    convert(EmailMessage(messageId = messageId, subject = subjectOption, date = adjustedDateOption, from = from, to = to, bcc = bcc, cc = cc), model)
  }

  private def convert(message: EmailMessage, model: Model): Resource = {
    val messageResource = resourceForMessage(message, model)

    addAddresses(message.from, messageResource, SchemaOrg.AUTHOR, model)
    addAddresses(message.to, messageResource, Personal.PRIMARY_RECIPIENT, model)
    addAddresses(message.cc, messageResource, Personal.COPY_RECIPIENT, model)
    addAddresses(message.bcc, messageResource, Personal.BLIND_COPY_RECIPIENT, model)

    message.date.foreach(date =>
      model.add(messageResource, SchemaOrg.DATE_PUBLISHED, valueFactory.createLiteral(date.toString))
    )
    message.subject.foreach(subject =>
      model.add(messageResource, SchemaOrg.HEADLINE, valueFactory.createLiteral(subject))
    )

    messageResource
  }

  private def addAddresses(addresses: Traversable[EmailAddress], messageResource: Resource, relation: IRI, model: Model): Unit = {
    addresses.foreach(address => {
      val personResource = convert(address, model)
      model.add(messageResource, relation, personResource)
    })
  }

  private def convert(address: EmailAddress, model: Model): Resource = {
    val personResource = valueFactory.createBNode
    address.name.foreach(name =>
      model.add(personResource, SchemaOrg.NAME, valueFactory.createLiteral(name))
    )
    model.add(personResource, SchemaOrg.EMAIL, emailAddressConverter.convert(localPart = address.localPart, domain = address.domain, model))
    personResource
  }

  private def resourceForMessage(message: EmailMessage, model: Model): Resource = {
    message.messageId.map(messageId =>
      emailMessageUriConverter.convert(messageId, model)
    ).getOrElse(blankNodeForMessage(model))
  }

  private def blankNodeForMessage(model: Model): Resource = {
    val messageResource = valueFactory.createBNode()
    model.add(messageResource, RDF.TYPE, SchemaOrg.EMAIL_MESSAGE)
    messageResource
  }

  def adjustDeliveryDate(deliveryDateOption: Option[Temporal], dateOption: Option[Temporal]) = {
    deliveryDateOption match {
      case Some(deliveryDate: OffsetDateTime) =>
        dateOption match {
          case Some(localDateTime: LocalDateTime) =>
            // Use the delivery date to guess the time offset of our date.
            val deliveryDateUTCLocalDateTime = deliveryDate.atZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime
            def durationToSecondsDouble(duration: Duration) = {
              duration.getSeconds.toDouble + duration.getNano.toDouble / 1000000000.0d
            }
            val d = durationToSecondsDouble(Duration.between(deliveryDateUTCLocalDateTime, localDateTime))
            val offsetHours = Math.round(d / 3600.00).toInt
            Some(OffsetDateTime.of(localDateTime, ZoneOffset.ofHours(offsetHours)))
          case _ => dateOption
        }
      case _ =>
        dateOption match {
          case Some(localDateTime: LocalDateTime) =>
            // Assume the time is UTC
            Some(OffsetDateTime.of(localDateTime, ZoneOffset.ofHours(0)))
          case _ => dateOption
        }
    }
  }

  def parseDate(field: Field) = {
    val date = LenientDateParser.parse(field.getBody)
    if (date.isEmpty) {
      logger.warn(s"Cannot parse date ${field.getBody}")
    }
    date
  }

  def convert(messages: Traversable[Message]): Model = {
    val model = new LinkedHashModel
    messages.foreach(message => convert(message, model))
    model
  }

  private def convert(message: Message, model: Model): EmailMessage = {
    val messageIdOption = message match {
      case mimeMessage: MimeMessage =>
        Option(mimeMessage.getMessageID)
      case _ => None
    }

    def emailAddress(address: Address) = {
      address match {
        case internetAddress: InternetAddress =>
          Option(internetAddress.getAddress).flatMap {
            case email =>
              val mailbox = AddressBuilder.DEFAULT.parseMailbox(email)
              Option(mailbox.getLocalPart).map {
                case localPart =>
                  val domain = Option(mailbox.getDomain).getOrElse("")
                  EmailAddress(name = Option(internetAddress.getPersonal),
                    localPart = localPart,
                    domain = domain)
              }
          }
        case _ =>
          logger.warn("Unknown address type: " + address.getType)
          None
      }
    }

    def addAddresses(addresses: () => Array[Address]): Vector[EmailAddress] = {
      try {
        addresses().flatMap(emailAddress)(scala.collection.breakOut)
      } catch {
        case e: AddressException => Vector()
      }
    }
    val from = addAddresses(() => {
      message.getFrom
    })
    Vector(RecipientType.TO, RecipientType.CC, RecipientType.BCC).map {
      case recipientType =>
        addAddresses(() => {
          message.getRecipients(recipientType)
        })
    } match {
      case Vector(to, cc, bcc) =>
        val dateOption = Option(message.getSentDate).map {
          case date => date.toInstant
        }
        val subjectOption = Option(message.getSubject)
        EmailMessage(messageId = messageIdOption, from = from, to = to, cc = cc, bcc = bcc, date = dateOption, subject = subjectOption)
    }
  }
}
