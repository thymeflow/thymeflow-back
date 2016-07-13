package thymeflow.sync.facebook

import java.text.{ParseException, SimpleDateFormat}
import java.time.OffsetDateTime
import java.time.chrono.IsoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, DateTimeParseException, ResolverStyle}
import java.time.temporal.ChronoField._
import java.util.Locale

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model._
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import thymeflow.rdf.model.SimpleHashModel
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.spatial.Address
import thymeflow.sync.converter.utils.{EmailAddressConverter, GeoCoordinatesConverter, PostalAddressConverter}

/**
  * @author David Montoya
  */
class FacebookConverter(valueFactory: ValueFactory) extends StrictLogging {

  final val namespace = "https://graph.facebook.com/"
  private val emailAddressConverter = new EmailAddressConverter(valueFactory)
  private val geoCoordinatesConverter = new GeoCoordinatesConverter(valueFactory)
  private val postalAddressConverter = new PostalAddressConverter(valueFactory)

  def convert(me: Me, context: IRI): Model = {
    val model = new SimpleHashModel()
    val meNode = valueFactory.createIRI(namespace, me.id)

    model.add(meNode, RDF.TYPE, Personal.AGENT, context)
    model.add(meNode, RDF.TYPE, SchemaOrg.PERSON, context)

    me.birthday.flatMap(convertDate).foreach(birthday =>
      model.add(meNode, SchemaOrg.BIRTH_DATE, birthday, context)
    )

    me.first_name.foreach(firstName =>
      model.add(meNode, SchemaOrg.GIVEN_NAME, valueFactory.createLiteral(firstName), context)
    )

    me.last_name.foreach(lastName =>
      model.add(meNode, SchemaOrg.FAMILY_NAME, valueFactory.createLiteral(lastName), context)
    )

    me.gender.foreach(gender =>
      model.add(meNode, SchemaOrg.GENDER, valueFactory.createLiteral(gender), context)
    )

    me.email.foreach(email =>
      emailAddressConverter.convert(email, model).foreach(emailNode =>
        model.add(meNode, SchemaOrg.EMAIL, emailNode, context)
      )
    )

    me.bio.foreach(bio =>
      model.add(meNode, SchemaOrg.DESCRIPTION, valueFactory.createLiteral(bio), context)
    )

    me.taggable_friends.data.foreach(taggableFriend => {
      val taggableFriendNode = valueFactory.createIRI(namespace, taggableFriend.id)

      model.add(taggableFriendNode, RDF.TYPE, Personal.AGENT, context)
      model.add(taggableFriendNode, RDF.TYPE, SchemaOrg.PERSON, context)
      taggableFriend.name.foreach(name =>
        model.add(taggableFriendNode, SchemaOrg.NAME, valueFactory.createLiteral(name), context)
      )
      taggableFriend.picture.foreach(picture =>
        picture.data.url.foreach(url => {
          val imageNode = valueFactory.createIRI(url)
          model.add(taggableFriendNode, SchemaOrg.IMAGE, imageNode, context)
          model.add(taggableFriendNode, RDF.TYPE, SchemaOrg.IMAGE_OBJECT, context)
        })
      )
    })
    model
  }

  def convert(event: Event, model: Model, context: IRI): Resource = {
    val eventNode = valueFactory.createIRI(namespace, event.id)
    model.add(eventNode, RDF.TYPE, SchemaOrg.EVENT, context)

    event.start_time.flatMap(convertIsoOffsetDateTime).foreach(startTime =>
      model.add(eventNode, SchemaOrg.START_DATE, startTime, context)
    )

    event.end_time.flatMap(convertIsoOffsetDateTime).foreach(endTime =>
      model.add(eventNode, SchemaOrg.END_DATE, endTime, context)
    )

    event.description.foreach(description =>
      model.add(eventNode, SchemaOrg.DESCRIPTION, valueFactory.createLiteral(description), context)
    )

    event.name.foreach(name =>
      model.add(eventNode, SchemaOrg.NAME, valueFactory.createLiteral(name), context)
    )

    event.cover.foreach(_.source.foreach(source => {
      val imageNode = valueFactory.createIRI(source)
      model.add(eventNode, SchemaOrg.IMAGE, imageNode, context)
      model.add(eventNode, RDF.TYPE, SchemaOrg.IMAGE_OBJECT, context)
    })
    )

    event.place.foreach {
      place =>
        val placeNode = convert(place, model, context)
        model.add(eventNode, SchemaOrg.LOCATION, placeNode)
    }

    event.invited.foreach {
      invitee =>
        val personNode = convert(invitee, model, context)
        if (invitee.rsvp_status == "attending") {
          model.add(eventNode, SchemaOrg.ATTENDEE, personNode, context)
        }
    }

    eventNode
  }

  def convert(eventPlace: EventPlace, model: Model, context: IRI): Resource = {
    val placeResource =
      eventPlace.id match {
        case Some(id) => valueFactory.createIRI(namespace, id)
        case None => valueFactory.createBNode()
      }
    model.add(placeResource, RDF.TYPE, SchemaOrg.PLACE, context)

    eventPlace.name.foreach {
      name =>
        model.add(placeResource, SchemaOrg.NAME, valueFactory.createLiteral(name), context)
    }

    eventPlace.location.foreach {
      location =>

        (location.longitude, location.latitude) match {
          case (Some(longitude), Some(latitude)) =>
            val geo = geoCoordinatesConverter.convert(longitude, latitude, None, None, model)
            model.add(placeResource, SchemaOrg.GEO, geo, context)
          case _ =>
        }

        val address = new Address {
          override def houseNumber: Option[String] = None

          override def postalCode: Option[String] = location.zip

          override def country: Option[String] = location.country

          override def region: Option[String] = Vector(location.state, location.region).flatten match {
            case Vector() => None
            case v => Some(v.mkString(" "))
          }

          override def locality: Option[String] = location.city

          override def street: Option[String] = location.street
        }

        val postalAddressResource = postalAddressConverter.convert(address, model, context)
        model.add(placeResource, SchemaOrg.ADDRESS, postalAddressResource, context)
    }
    placeResource
  }

  def convert(invitee: Invitee, model: Model, context: IRI): Resource = {
    val personNode = valueFactory.createIRI(namespace, invitee.id)

    model.add(personNode, RDF.TYPE, Personal.AGENT, context)
    model.add(personNode, RDF.TYPE, SchemaOrg.PERSON, context)
    model.add(personNode, SchemaOrg.NAME, valueFactory.createLiteral(invitee.name), context)

    personNode
  }


  private val isoOffsetDateTimeParser = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    .appendOffset("+HHmm", "Z")
    .toFormatter(Locale.ROOT)
    .withResolverStyle(ResolverStyle.STRICT)
    .withChronology(IsoChronology.INSTANCE)

  private val isoOffsetDateTimeFormatter = new DateTimeFormatterBuilder()
    .append(DateTimeFormatter.ISO_LOCAL_DATE)
    .appendLiteral('T')
    .appendValue(HOUR_OF_DAY, 2)
    .appendLiteral(':')
    .appendValue(MINUTE_OF_HOUR, 2)
    .appendLiteral(':')
    .appendValue(SECOND_OF_MINUTE, 2)
    .optionalStart()
    .appendFraction(NANO_OF_SECOND, 0, 9, true)
    .optionalEnd()
    .appendOffset("+HH:MM", "Z")
    .toFormatter(Locale.ROOT)

  def convertIsoOffsetDateTime(dateTime: String) = {
    try {
      val parsedDateTime = OffsetDateTime.parse(dateTime, isoOffsetDateTimeParser)
      Some(valueFactory.createLiteral(parsedDateTime.format(isoOffsetDateTimeFormatter), XMLSchema.DATETIME))
    } catch {
      case e: DateTimeParseException =>
        logger.warn(s"Invalid Facebook ISO Offset DateTime $dateTime")
        None
    }
  }

  def convertDate(date: String): Option[Literal] = {
    try {
      val ymd = new SimpleDateFormat("MM/dd/yyyy").parse(date)
      Some(valueFactory.createLiteral(new SimpleDateFormat("yyyy-MM-dd").format(ymd), XMLSchema.DATE))
    } catch {
      case e: ParseException =>
        try {
          val y = new SimpleDateFormat("yyyy").parse(date)
          Some(valueFactory.createLiteral(new SimpleDateFormat("yyyy").format(y), XMLSchema.GYEAR))
        } catch {
          case e: ParseException =>
            try {
              val y = new SimpleDateFormat("MM/dd").parse(date)
              Some(valueFactory.createLiteral(new SimpleDateFormat("MM-ddd").format(y), XMLSchema.GMONTHDAY))
            } catch {
              case e: ParseException =>
                logger.info(s"Invalid Facebook Date $date")
                None
            }
        }
    }
  }

}
