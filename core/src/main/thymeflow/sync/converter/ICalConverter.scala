package thymeflow.sync.converter

import java.io.InputStream
import java.net.{MalformedURLException, URI, URL}
import java.text.SimpleDateFormat
import javax.xml.datatype.{DatatypeConstants, DatatypeFactory}

import biweekly.component.VEvent
import biweekly.property._
import biweekly.util.{Duration, ICalDate}
import biweekly.{Biweekly, ICalendar}
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model._
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import thymeflow.rdf.model.SimpleHashModel
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.sync.converter.utils.{EmailAddressConverter, EmailMessageUriConverter, GeoCoordinatesConverter, UUIDConverter}

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
class ICalConverter(valueFactory: ValueFactory) extends Converter with StrictLogging {

  private val emailAddressConverter = new EmailAddressConverter(valueFactory)
  private val emailMessageConverter = new EmailMessageUriConverter(valueFactory)
  private val geoCoordinatesConverter = new GeoCoordinatesConverter(valueFactory)
  private val uuidConverter = new UUIDConverter(valueFactory)

  override def convert(str: String, context: IRI): Model = {
    convert(Biweekly.parse(str).all.asScala, context)
  }

  private def convert(calendars: Traversable[ICalendar], context: IRI): Model = {
    val model = new SimpleHashModel(valueFactory)
    val converter = new ToModelConverter(model, context)
    for (calendar <- calendars) {
      converter.convert(calendar)
    }
    model
  }

  override def convert(stream: InputStream, context: IRI): Model = {
    convert(Biweekly.parse(stream).all.asScala, context)
  }

  private class ToModelConverter(model: Model, context: IRI) {
    def convert(calendar: ICalendar) {
      for (event <- calendar.getEvents.asScala) {
        convert(event)
      }
      //TODO: other types
    }

    private def convert(event: VEvent): Resource = {
      val eventResource = resourceFromUid(event.getUid)
      model.add(eventResource, RDF.TYPE, SchemaOrg.EVENT, context)

      event.getProperties.asScala.foreach(entry =>
        entry.getValue.asScala.foreach {
          //ATTENDEE
          case attendee: Attendee => model.add(eventResource, SchemaOrg.ATTENDEE, convert(attendee), context)
          //DESCRIPTION
          case description: Description =>
            if (description.getValue != "") {
              model.add(eventResource, SchemaOrg.DESCRIPTION, valueFactory.createLiteral(description.getValue.trim), context)
            }
          //DEND
          case dateEnd: DateEnd => model.add(eventResource, SchemaOrg.END_DATE, convert(dateEnd), context)
          //DSTART
          case dateStart: DateStart => model.add(eventResource, SchemaOrg.START_DATE, convert(dateStart), context)
          //DURATION
          case duration: DurationProperty => model.add(eventResource, SchemaOrg.DURATION, convert(duration), context)
          //LOCATION
          case location: Location =>
            if (location.getValue != "") {
              model.add(eventResource, SchemaOrg.LOCATION, convert(location), context)
            }
          //ORGANIZER
          case organizer: Organizer => model.add(eventResource, SchemaOrg.ORGANIZER, convert(organizer), context)
          //SUMMARY
          case summary: Summary =>
            if (summary.getValue != "") {
              model.add(eventResource, SchemaOrg.NAME, valueFactory.createLiteral(summary.getValue.trim), context)
            }
          //URL
          case url: Url =>
            try {
              val uri = new URI(url.getValue)
              if (uri.getScheme == "message") {
                val messageResource = emailMessageConverter.convert(uri)
                model.add(messageResource, RDF.TYPE, SchemaOrg.EMAIL_MESSAGE, context)
                model.add(messageResource, SchemaOrg.ABOUT, eventResource, context)
              } else {
                convert(url).foreach(url => model.add(eventResource, SchemaOrg.URL, url, context))
              }
            } catch {
              case e: IllegalArgumentException =>
                logger.warn("The URL " + url.getValue + " is invalid", e)
                None
            }
          // X-APPLE-STRUCTURED-LOCATION
          case property: RawProperty if property.getName == "X-APPLE-STRUCTURED-LOCATION" =>
            model.add(eventResource, SchemaOrg.LOCATION, convertXAppleStructuredLocation(property), context)
          case _ =>
        }
      )

      eventResource
    }

    private def convert(attendee: Attendee): Resource = {
      val attendeeResource = uuidConverter.createBNode(attendee)
      model.add(attendeeResource, RDF.TYPE, Personal.AGENT, context)

      Option(attendee.getCommonName).foreach(name =>
        model.add(attendeeResource, SchemaOrg.NAME, valueFactory.createLiteral(name), context)
      )
      Option(attendee.getEmail).foreach(email =>
        emailAddressConverter.convert(email, model).foreach {
          resource => model.add(attendeeResource, SchemaOrg.EMAIL, resource, context)
        }
      )
      Option(attendee.getUri).foreach(url =>
        try {
          val uri = new URI(url)
          if (uri.getScheme == "message") {
            emailAddressConverter.convert(uri, model).foreach {
              resource => model.add(attendeeResource, SchemaOrg.EMAIL, resource, context)
            }
          } else {
            model.add(attendeeResource, SchemaOrg.URL, valueFactory.createIRI(url), context)
            logger.info("Attendee address that is not a mailto URI: " + url)
          }
        } catch {
          case e: IllegalArgumentException =>
            logger.warn("The URL " + url + " is invalid", e)
        }
      )

      attendeeResource
    }

    private def convert(date: DateOrDateTimeProperty): Literal = {
      convert(date.getValue)
    }

    private def convert(date: ICalDate): Literal = {
      if (date.hasTime) {
        valueFactory.createLiteral(date)
      }
      else {
        valueFactory.createLiteral(new SimpleDateFormat("yyyy-MM-dd").format(date), XMLSchema.DATE)
      }
    }

    private def convert(duration: DurationProperty): Literal = {
      convert(duration.getValue)
    }

    private def convert(duration: Duration): Literal = {
      val days = Option(duration.getWeeks)
        .map[Integer](weeks => weeks * 7 + Option(duration.getDays).getOrElse[Integer](0))
        .orElse(Option(duration.getDays))

      val xmlDuration = DatatypeFactory.newInstance().newDuration(
        !duration.isPrior,
        DatatypeConstants.FIELD_UNDEFINED,
        DatatypeConstants.FIELD_UNDEFINED,
        days.getOrElse[Integer](DatatypeConstants.FIELD_UNDEFINED),
        Option(duration.getHours).getOrElse[Integer](DatatypeConstants.FIELD_UNDEFINED),
        Option(duration.getMinutes).getOrElse[Integer](DatatypeConstants.FIELD_UNDEFINED),
        Option(duration.getSeconds).getOrElse[Integer](DatatypeConstants.FIELD_UNDEFINED)
      )
      valueFactory.createLiteral(xmlDuration.toString, XMLSchema.DAYTIMEDURATION)
    }

    private def convert(location: Location): Resource = {
      val placeResource = uuidConverter.createBNode(location)
      model.add(placeResource, RDF.TYPE, SchemaOrg.PLACE, context)
      model.add(placeResource, SchemaOrg.NAME, valueFactory.createLiteral(location.getValue.replaceAll("\n", " ").trim), context)
      placeResource
    }

    private def convert(organizer: Organizer): Resource = {
      val organizerResource = uuidConverter.createBNode(organizer)
      model.add(organizerResource, RDF.TYPE, Personal.AGENT, context)

      Option(organizer.getCommonName).foreach(name =>
        model.add(organizerResource, SchemaOrg.NAME, valueFactory.createLiteral(name), context)
      )
      Option(organizer.getEmail).foreach(email =>
        emailAddressConverter.convert(email, model).foreach {
          resource => model.add(organizerResource, SchemaOrg.EMAIL, resource, context)
        }
      )
      Option(organizer.getUri).foreach(url =>
        try {
          val uri = new URI(url)
          if (uri.getScheme == "message") {
            emailAddressConverter.convert(uri, model).foreach {
              resource => model.add(organizerResource, SchemaOrg.EMAIL, resource, context)
            }
          } else {
            model.add(organizerResource, SchemaOrg.URL, valueFactory.createIRI(url), context)
            logger.info("Organizer address that is not a mailto URI: " + url)
          }
        } catch {
          case e: IllegalArgumentException =>
            logger.warn("The URL " + url + " is invalid", e)
        }
      )

      organizerResource
    }

    private def convert(url: Url): Option[Resource] = {
      try {
        Some(valueFactory.createIRI(new URL(url.getValue).toString))
      } catch {
        case e: MalformedURLException =>
          logger.warn("The URL " + url.getValue + " is invalid", e)
          None
      }

    }

    private def convertXAppleStructuredLocation(xAppleStructuredLocation: RawProperty): Resource = {
      val placeResource = convert(new Location(xAppleStructuredLocation.getParameter("X-TITLE")))
      geoCoordinatesConverter.convertGeoUri(xAppleStructuredLocation.getValue, model).foreach(
        coordinatesResource => model.add(placeResource, SchemaOrg.GEO, coordinatesResource, context)
      )
      placeResource
    }

    private def resourceFromUid(uid: Uid): Resource = {
      if (uid == null) {
        valueFactory.createBNode()
      } else {
        uuidConverter.convert(uid.getValue)
      }
    }
  }
}
