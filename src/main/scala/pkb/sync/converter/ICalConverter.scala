package pkb.sync.converter

import java.io.{File, FileInputStream}
import java.util.Date

import net.fortuna.ical4j.data.CalendarBuilder
import net.fortuna.ical4j.model.component.VEvent
import net.fortuna.ical4j.model.property._
import net.fortuna.ical4j.model.{Calendar, Parameter, Property}
import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.vocabulary.RDF
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
    val inputStream = new FileInputStream(file)
    val builder = new CalendarBuilder
    convert(builder.build(inputStream))
  }

  def convert(calendar: Calendar): Model = {
    val model = new LinkedHashModel

    for (component <- calendar.getComponents().asScala) {
      component match {
        case event: VEvent => convert(event, model)
        case _ => //We ignore all other cases
      }
    }
    model
  }

  private def convert(event: VEvent, model: Model): Resource = {
    val eventResource = resourceFromUid(event.getUid)
    model.add(eventResource, RDF.TYPE, SchemaOrg.EVENT)

    //ATTENDEE
    for (property <- event.getProperties(Property.ATTENDEE).asScala) {
      model.add(eventResource, SchemaOrg.ATTENDEE, convert(property.asInstanceOf[Attendee], model))
    }
    //DESCRIPTION
    val description = event.getDescription
    if (description != null && description.getValue != "") {
      model.add(eventResource, SchemaOrg.DESCRIPTION, convert(description))
    }
    //DEND
    val endDate = event.getEndDate
    if (endDate != null) {
      model.add(eventResource, SchemaOrg.END_DATE, convert(endDate))
    }
    //DSTART
    val dateStart = event.getStartDate
    if (dateStart != null) {
      model.add(eventResource, SchemaOrg.START_DATE, convert(dateStart))
    }
    //TODO: DURATION
    //LOCATION + X-APPLE-STRUCTURED-LOCATION
    val xAppleStructuredLocation = event.getProperty("X-APPLE-STRUCTURED-LOCATION")
    if (xAppleStructuredLocation != null) {
      model.add(eventResource, SchemaOrg.LOCATION, convertXAppleStructuredLocation(xAppleStructuredLocation, model))
    } else {
      val location = event.getLocation
      if (location != null) {
        model.add(eventResource, SchemaOrg.LOCATION, convert(location, model))
      }
    }
    //ORGANIZER
    val organizer = event.getOrganizer
    if (organizer != null) {
      model.add(eventResource, SchemaOrg.ORGANIZER, convert(organizer, model))
    }
    //SUMMARY
    val summary = event.getSummary
    if (summary != null) {
      model.add(eventResource, SchemaOrg.NAME, convert(summary))
    }
    //URL
    val url = event.getUrl
    if (url != null) {
      if (url.getUri.getScheme == "message") {
        model.add(emailMessageConverter.convert(url.getUri, model), SchemaOrg.ABOUT, eventResource)
      } else {
        model.add(eventResource, SchemaOrg.URL, convert(url))
        logger.info("Event URL that is not an message: URI: " + url.getUri)
      }
    }

    eventResource
  }

  private def convert(attendee: Attendee, model: Model): Resource = {
    val attendeeResource = valueFactory.createBNode()

    val name = attendee.getParameter(Parameter.CN)
    if (name != null) {
      model.add(attendeeResource, SchemaOrg.NAME, valueFactory.createLiteral(name.getValue))
    }

    val calAddress = attendee.getCalAddress
    if (calAddress != null) {
      if (calAddress.getScheme == "mailto") {
        model.add(attendeeResource, SchemaOrg.EMAIL, emailAddressConverter.convert(calAddress, model))
      } else {
        model.add(attendeeResource, SchemaOrg.URL, valueFactory.createIRI(calAddress.toString))
        logger.info("Attendee address that is not a mailto URI: " + calAddress)
      }
    }

    attendeeResource
  }

  private def convert(description: Description): Literal = {
    valueFactory.createLiteral(description.getValue)
  }

  private def convert(date: DateProperty): Literal = {
    valueFactory.createLiteral(Date.from(date.getDate.toInstant))
  }

  private def convert(location: Location, model: Model): Resource = {
    val placeResource = valueFactory.createBNode()
    model.add(placeResource, RDF.TYPE, SchemaOrg.PLACE)
    model.add(placeResource, SchemaOrg.NAME, valueFactory.createLiteral(location.getValue))
    placeResource
  }

  private def convert(organizer: Organizer, model: Model): Resource = {
    val attendeeResource = valueFactory.createBNode()

    val name = organizer.getParameter(Parameter.CN)
    if (name != null) {
      model.add(attendeeResource, SchemaOrg.NAME, valueFactory.createLiteral(name.getValue))
    }

    val calAddress = organizer.getCalAddress
    if (calAddress != null) {
      if (calAddress.getScheme == "mailto") {
        model.add(attendeeResource, SchemaOrg.EMAIL, emailAddressConverter.convert(calAddress, model))
      } else {
        model.add(attendeeResource, SchemaOrg.URL, valueFactory.createIRI(calAddress.toString))
        logger.info("Organizer address that is not a mailto URI: " + calAddress)
      }
    }

    attendeeResource
  }

  private def convert(summary: Summary): Literal = {
    valueFactory.createLiteral(summary.getValue)
  }

  private def convert(url: Url): Resource = {
    valueFactory.createIRI(url.getUri.toString)
  }

  private def convertXAppleStructuredLocation(xAppleStructuredLocation: Property, model: Model): Resource = {
    val placeResource = valueFactory.createBNode()
    model.add(placeResource, RDF.TYPE, SchemaOrg.PLACE)
    model.add(placeResource, SchemaOrg.NAME, valueFactory.createLiteral(xAppleStructuredLocation.getParameter("X-TITLE").getValue))
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
