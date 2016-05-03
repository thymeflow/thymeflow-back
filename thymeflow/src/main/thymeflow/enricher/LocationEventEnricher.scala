package thymeflow.enricher

import javax.xml.bind.DatatypeConverter

import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.XMLSchema
import org.openrdf.model.{Literal, Resource}
import org.openrdf.query.{BindingSet, QueryLanguage}
import org.openrdf.repository.RepositoryConnection
import thymeflow.actors._
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.ModelDiff
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  *         All times are here stored as miliseconds since the epoch
  */
class LocationEventEnricher(repositoryConnection: RepositoryConnection) extends Enricher with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "LocationEventEnricher")

  private val overlapMinRatio = 0.1

  private val eventsQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?event ?start ?end WHERE {
      ?event a <${SchemaOrg.EVENT}> ;
             <${SchemaOrg.START_DATE}> ?start ;
             <${SchemaOrg.END_DATE}> ?end .
      } ORDER BY ?start"""
  )
  private val staysQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?event ?start ?end WHERE {
      ?event a <${Personal.STAY_EVENT}> ;
            <${SchemaOrg.START_DATE}> ?start ;
            <${SchemaOrg.END_DATE}> ?end .
    } ORDER BY ?start"""
  )

  /**
    * Run the enrichments defined by this Enricher
    */
  override def enrich(diff: ModelDiff): Unit = {
    val events = getEvents.toBuffer

    Await.result(getStays.map(stay =>
      events.filter(event => {
        //We look for not null intersection (remark: may not work if bounds are exactly equal)
        if (event.start <= stay.start) {
          //We should have stay beginning is between event.start and event.end
          event.end >= stay.start && isMatchIntervalBiggerThanRatioOfEvent(stay.start, Math.min(event.end, stay.end), event)
        } else {
          //We should have the event beginning is between stay.start and stay.end
          event.start <= stay.end && isMatchIntervalBiggerThanRatioOfEvent(event.start, Math.min(event.end, stay.end), event)
        }
      }).map(event => {
        repositoryConnection.begin()
        repositoryConnection.add(event.resource, SchemaOrg.LOCATION, stay.resource, inferencerContext)
        repositoryConnection.commit()
        event
      }).size
    ).runFold(0)(_ + _).map(addedConnections =>
      logger.info(s"$addedConnections added between events and stay locations")
    ), Duration.Inf)
  }

  private def isMatchIntervalBiggerThanRatioOfEvent(start: Long, end: Long, event: TimeIntervalResource): Boolean = {
    (end - start).toFloat / event.length > overlapMinRatio
  }

  private def getEvents: Iterator[TimeIntervalResource] = {
    eventsQuery.evaluate().map(parseTimeIntervalTuple).collect {
      case Some(timeIntervalResource) => timeIntervalResource
    }
  }

  private def getStays: Source[TimeIntervalResource, _] = {
    Source.fromIterator(() => staysQuery.evaluate()).map(parseTimeIntervalTuple).collect {
      case Some(timeIntervalResource) => timeIntervalResource
    }
  }

  private def parseTimeIntervalTuple(bindingSet: BindingSet): Option[TimeIntervalResource] = {
    (
      Option(bindingSet.getValue("event").asInstanceOf[Resource]),
      Option(bindingSet.getValue("start").asInstanceOf[Literal]).flatMap(parseLiteralAsTime),
      Option(bindingSet.getValue("end").asInstanceOf[Literal]).flatMap(parseLiteralAsTime)
      ) match {
      case (Some(resource), Some(start), Some(end)) => Some(TimeIntervalResource(resource, start, end))
      case _ => None
    }
  }

  private def parseLiteralAsTime(literal: Literal): Option[Long] = {
    literal.getDatatype match {
      case XMLSchema.DATE => Some(DatatypeConverter.parseDate(literal.getLabel).getTimeInMillis)
      case XMLSchema.DATETIME => Some(DatatypeConverter.parseDateTime(literal.getLabel).getTimeInMillis)
      case _ => None
    }
  }

  private case class TimeIntervalResource(resource: Resource, start: Long, end: Long) {
    def length = end - start

    override def equals(obj: scala.Any): Boolean = {
      obj match {
        case other: TimeIntervalResource => other.resource == resource
        case _ => false
      }
    }

    override def hashCode(): Int = resource.hashCode()
  }

}
