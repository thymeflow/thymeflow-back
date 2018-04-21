package com.thymeflow.sync.converter

import java.io.InputStream
import java.net.{MalformedURLException, URI, URL}
import java.text.SimpleDateFormat
import javax.xml.datatype.{DatatypeConstants, DatatypeFactory}

import biweekly.component.VEvent
import biweekly.property._
import biweekly.util.{Duration, ICalDate}
import biweekly.{Biweekly, ICalendar}
import com.thymeflow.rdf.model.StatementSet
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.thymeflow.sync.converter.utils.{EmailAddressConverter, EmailMessageUriConverter, GeoCoordinatesConverter, UUIDConverter}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.eclipse.rdf4j.model._
import org.eclipse.rdf4j.model.vocabulary.{RDF, XMLSchema}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
class ICalConverter(valueFactory: ValueFactory)(implicit config: Config) extends Converter with StrictLogging {

  private val emailAddressConverter = new EmailAddressConverter(valueFactory)
  private val emailMessageConverter = new EmailMessageUriConverter(valueFactory)
  private val geoCoordinatesConverter = new GeoCoordinatesConverter(valueFactory)
  private val uuidConverter = new UUIDConverter(valueFactory)

  def convert(str: String, context: Resource): StatementSet = {
    convert(Biweekly.parse(str).all.asScala, context)
  }

  private def convert(calendars: Traversable[ICalendar], context: Resource): StatementSet = {
    val statements = StatementSet.empty(valueFactory)
    val converter = new ToModelConverter(statements, context)
    for (calendar <- calendars) {
      converter.convert(calendar)
    }
    statements
  }

  override def convert(stream: InputStream, context: Option[String] => IRI, createSourceContext: (StatementSet, String) => IRI): Iterator[(IRI, StatementSet)] = {
    val wholeContext = context(None)
    Iterator((wholeContext, convert(Biweekly.parse(stream).all.asScala, wholeContext)))
  }

  private class ToModelConverter(statements: StatementSet, context: Resource) {
    def convert(calendar: ICalendar) {
      for (event <- calendar.getEvents.asScala) {
        convert(event)
      }
      //TODO: other types
    }

    private def convert(event: VEvent): Resource = {
      val eventResource = resourceFromId(event.getUid, event.getRecurrenceId, event.getSequence)
      statements.add(eventResource, RDF.TYPE, SchemaOrg.EVENT, context)

      event.getProperties.asScala.foreach(entry =>
        entry.getValue.asScala.foreach {
          //ATTENDEE
          case attendee: Attendee => statements.add(eventResource, SchemaOrg.ATTENDEE, convert(attendee), context)
          //DESCRIPTION
          case description: Description =>
            if (description.getValue != "") {
              statements.add(eventResource, SchemaOrg.DESCRIPTION, valueFactory.createLiteral(description.getValue.trim), context)
            }
          //DEND
          case dateEnd: DateEnd => statements.add(eventResource, SchemaOrg.END_DATE, convert(dateEnd), context)
          //DSTART
          case dateStart: DateStart => statements.add(eventResource, SchemaOrg.START_DATE, convert(dateStart), context)
          //DURATION
          case duration: DurationProperty => statements.add(eventResource, SchemaOrg.DURATION, convert(duration), context)
          //LOCATION
          case location: Location =>
            if (location.getValue != "") {
              statements.add(eventResource, SchemaOrg.LOCATION, convert(location), context)
            }
          //ORGANIZER
          case organizer: Organizer => statements.add(eventResource, SchemaOrg.ORGANIZER, convert(organizer), context)
          //SUMMARY
          case summary: Summary =>
            if (summary.getValue != "") {
              statements.add(eventResource, SchemaOrg.NAME, valueFactory.createLiteral(summary.getValue.trim), context)
            }
          //URL
          case url: Url =>
            try {
              val uri = new URI(url.getValue)
              if (uri.getScheme == "message") {
                val messageResource = emailMessageConverter.convert(uri)
                statements.add(messageResource, RDF.TYPE, SchemaOrg.EMAIL_MESSAGE, context)
                statements.add(messageResource, SchemaOrg.ABOUT, eventResource, context)
              } else {
                convert(url).foreach(url => statements.add(eventResource, SchemaOrg.URL, url, context))
              }
            } catch {
              case e: IllegalArgumentException =>
                logger.warn("The URL " + url.getValue + " is invalid", e)
                None
            }
          // X-APPLE-STRUCTURED-LOCATION
          case property: RawProperty if property.getName == "X-APPLE-STRUCTURED-LOCATION" =>
            statements.add(eventResource, SchemaOrg.LOCATION, convertXAppleStructuredLocation(property), context)
          case _ =>
        }
      )

      eventResource
    }

    private def convert(attendee: Attendee): Resource = {
      val attendeeResource = uuidConverter.createIRI(attendee)
      statements.add(attendeeResource, RDF.TYPE, Personal.AGENT, context)

      Option(attendee.getCommonName).foreach(name =>
        statements.add(attendeeResource, SchemaOrg.NAME, valueFactory.createLiteral(name), context)
      )
      Option(attendee.getEmail).foreach(email =>
        emailAddressConverter.convert(email, statements).foreach {
          resource => statements.add(attendeeResource, SchemaOrg.EMAIL, resource, context)
        }
      )
      Option(attendee.getUri).foreach(url => {
        statements.add(attendeeResource, SchemaOrg.URL, valueFactory.createIRI(url), context)
        logger.info("Attendee address that is not a mailto URI: " + url)
      })

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
      implicit def integerToOption(int: Integer): Option[Int] = Option(int).map(_.intValue())

      val days = Option(duration.getWeeks)
        .map[Int](_ * 7 + duration.getDays.getOrElse(0))
        .orElse(duration.getDays)

      val xmlDuration = DatatypeFactory.newInstance().newDuration(
        !duration.isPrior,
        DatatypeConstants.FIELD_UNDEFINED,
        DatatypeConstants.FIELD_UNDEFINED,
        days.getOrElse(DatatypeConstants.FIELD_UNDEFINED),
        duration.getHours.getOrElse(DatatypeConstants.FIELD_UNDEFINED),
        duration.getMinutes.getOrElse(DatatypeConstants.FIELD_UNDEFINED),
        duration.getSeconds.getOrElse(DatatypeConstants.FIELD_UNDEFINED)
      )
      valueFactory.createLiteral(xmlDuration.toString, XMLSchema.DAYTIMEDURATION)
    }

    private def convert(location: Location): Resource = {
      val placeResource = uuidConverter.createBNode(location)
      statements.add(placeResource, RDF.TYPE, SchemaOrg.PLACE, context)
      statements.add(placeResource, SchemaOrg.NAME, valueFactory.createLiteral(location.getValue.replaceAll("\n", " ").trim), context)
      placeResource
    }

    private def convert(organizer: Organizer): Resource = {
      val organizerResource = uuidConverter.createIRI(organizer)
      statements.add(organizerResource, RDF.TYPE, Personal.AGENT, context)

      Option(organizer.getCommonName).foreach(name =>
        statements.add(organizerResource, SchemaOrg.NAME, valueFactory.createLiteral(name), context)
      )
      Option(organizer.getEmail).foreach(email =>
        emailAddressConverter.convert(email, statements).foreach {
          resource => statements.add(organizerResource, SchemaOrg.EMAIL, resource, context)
        }
      )
      Option(organizer.getUri).foreach(url => {
        statements.add(organizerResource, SchemaOrg.URL, valueFactory.createIRI(url), context)
        logger.info("Organizer address that is not a mailto URI: " + url)
      })

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
      geoCoordinatesConverter.convertGeoUri(xAppleStructuredLocation.getValue, statements).foreach(
        coordinatesResource => statements.add(placeResource, SchemaOrg.GEO, coordinatesResource, context)
      )
      placeResource
    }

    private def resourceFromId(uid: Uid, recurrenceId: RecurrenceId, sequence: Sequence): Resource = {
      if (uid == null) {
        valueFactory.createBNode()
      } else {
        uuidConverter.convert(Vector(uid, Option(recurrenceId).map(_.getValue.toString).getOrElse(""), Option(sequence).map(_.getValue.toString).getOrElse("")).mkString("\u0000"))
      }
    }
  }
}
