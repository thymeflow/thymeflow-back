package pkb.sync.converter

import java.io.File
import java.net.{MalformedURLException, URI, URL}
import java.text.SimpleDateFormat

import biweekly.{Biweekly, ICalendar}
import biweekly.component.VEvent
import biweekly.property._
import biweekly.util.ICalDate
import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import org.openrdf.model.{Literal, Model, Resource, ValueFactory}
import org.slf4j.LoggerFactory
import pkb.sync.converter.utils.{EmailAddressConverter, EmailMessageConverter, GeoCoordinatesConverter, UUIDConverter}
import pkb.vocabulary.SchemaOrg

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
class ICalConverter(valueFactory: ValueFactory) {

  private val logger = LoggerFactory.getLogger(classOf[ICalConverter])
  private val emailAddressConverter = new EmailAddressConverter(valueFactory)
  private val emailMessageConverter = new EmailMessageConverter(valueFactory)
  private val geoCoordinatesConverter = new GeoCoordinatesConverter(valueFactory)
  private val uuidConverter = new UUIDConverter(valueFactory)


  def convert(file: File): Model = {
    convert(Biweekly.parse(file).all.asScala)
  }

  def convert(calendars: Iterable[ICalendar]): Model = {
    val model = new LinkedHashModel
    for (calendar <- calendars) {
      convert(calendar, model)
    }
    model
  }

  def convert(calendar: ICalendar, model: Model) {
    for (event <- calendar.getEvents.asScala) {
      convert(event, model)
    }
    //TODO: other types
  }

  private def convert(event: VEvent, model: Model): Resource = {
    val eventResource = resourceFromUid(event.getUid)
    model.add(eventResource, RDF.TYPE, SchemaOrg.EVENT)

    for (entry <- event.getProperties.iterator.asScala) {
      for (property <- entry.getValue.asScala) {
        property match {
          //ATTENDEE
          case attendee: Attendee => model.add(eventResource, SchemaOrg.ATTENDEE, convert(attendee, model))
          //DESCRIPTION
          case description: Description =>
            if (description.getValue != "") {
              model.add(eventResource, SchemaOrg.DESCRIPTION, valueFactory.createLiteral(description.getValue))
            }
          //DEND
          case dateEnd: DateEnd => model.add(eventResource, SchemaOrg.END_DATE, convert(dateEnd))
          //DSTART
          case dateStart: DateStart => model.add(eventResource, SchemaOrg.START_DATE, convert(dateStart))
          //TODO: DURATION
          //LOCATION
          case location: Location =>
            if (location.getValue != "") {
              model.add(eventResource, SchemaOrg.LOCATION, convert(location, model))
            }
          //ORGANIZER
          case organizer: Organizer => model.add(eventResource, SchemaOrg.ORGANIZER, convert(organizer, model))
          //SUMMARY
          case summary: Summary =>
            if (summary.getValue != "") {
              model.add(eventResource, SchemaOrg.NAME, valueFactory.createLiteral(summary.getValue))
            }
          //URL
          case url: Url =>
            try {
              val uri = new URI(url.getValue)
              if (uri.getScheme == "message") {
                model.add(emailMessageConverter.convert(uri, model), SchemaOrg.ABOUT, eventResource)
              } else {
                convert(url).foreach(url => model.add(eventResource, SchemaOrg.URL, url))
                logger.info("Event URL that is not an message: URI: " + uri)
              }
            } catch {
              case e: IllegalArgumentException =>
                logger.warn("The URL " + url.getValue + " is invalid", e)
                None
            }
          case property: RawProperty =>
            // X-APPLE-STRUCTURED-LOCATION
            if (property.getName == "X-APPLE-STRUCTURED-LOCATION") {
              model.add(eventResource, SchemaOrg.LOCATION, convertXAppleStructuredLocation(property, model))
            }
          case _ =>
        }
      }
    }

    eventResource
  }

  private def convert(attendee: Attendee, model: Model): Resource = {
    val attendeeResource = valueFactory.createBNode()

    Option(attendee.getCommonName).foreach(name =>
      model.add(attendeeResource, SchemaOrg.NAME, valueFactory.createLiteral(name))
    )
    Option(attendee.getEmail).foreach(email =>
      model.add(attendeeResource, SchemaOrg.EMAIL, emailAddressConverter.convert(email, model))
    )
    Option(attendee.getUri).foreach(url =>
      try {
        val uri = new URI(url)
        if (uri.getScheme == "message") {
          model.add(attendeeResource, SchemaOrg.EMAIL, emailAddressConverter.convert(uri, model))
        } else {
          model.add(attendeeResource, SchemaOrg.URL, valueFactory.createIRI(url))
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

  private def convert(location: Location, model: Model): Resource = {
    val placeResource = valueFactory.createBNode()
    model.add(placeResource, RDF.TYPE, SchemaOrg.PLACE)
    model.add(placeResource, SchemaOrg.NAME, valueFactory.createLiteral(location.getValue))
    placeResource
  }

  private def convert(organizer: Organizer, model: Model): Resource = {
    val attendeeResource = valueFactory.createBNode()

    Option(organizer.getCommonName).foreach(name =>
      model.add(attendeeResource, SchemaOrg.NAME, valueFactory.createLiteral(name))
    )
    Option(organizer.getEmail).foreach(email =>
      model.add(attendeeResource, SchemaOrg.EMAIL, emailAddressConverter.convert(email, model))
    )
    Option(organizer.getUri).foreach(url =>
      try {
        val uri = new URI(url)
        if (uri.getScheme == "message") {
          model.add(attendeeResource, SchemaOrg.EMAIL, emailAddressConverter.convert(uri, model))
        } else {
          model.add(attendeeResource, SchemaOrg.URL, valueFactory.createIRI(url))
          logger.info("Organizer address that is not a mailto URI: " + url)
        }
      } catch {
        case e: IllegalArgumentException =>
          logger.warn("The URL " + url + " is invalid", e)
      }
    )

    attendeeResource
  }

  private def convert(url: Url): Option[Resource] = {
    try {
      Some(valueFactory.createIRI(new URL(url.getValue).toString))
    }
    catch {
      case e: MalformedURLException =>
        logger.warn("The URL " + url.getValue + " is invalid", e)
        None
    }

  }

  private def convertXAppleStructuredLocation(xAppleStructuredLocation: RawProperty, model: Model): Resource = {
    val placeResource = valueFactory.createBNode()
    model.add(placeResource, RDF.TYPE, SchemaOrg.PLACE)
    model.add(placeResource, SchemaOrg.NAME, valueFactory.createLiteral(xAppleStructuredLocation.getParameter("X-TITLE")))
    geoCoordinatesConverter.convertGeoUri(xAppleStructuredLocation.getValue, model).foreach(
      coordinatesResource => model.add(placeResource, SchemaOrg.GEO, coordinatesResource)
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
