package thymeflow.enricher

import javax.xml.bind.DatatypeConverter

import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.{OWL, XMLSchema}
import org.openrdf.model.{Literal, Resource}
import org.openrdf.query.{BindingSet, QueryLanguage}
import org.openrdf.repository.RepositoryConnection
import thymeflow.actors._
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.ModelDiff
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.spatial.geographic.metric.models.WGS84SphereHaversinePointMetric
import thymeflow.spatial.geographic.{Geography, Point}

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

  private val overlapMinRatio = 0.2
  private val maxLocationDistance = 1000

  private val eventsQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?event ?start ?end ?lat ?lon WHERE {
      ?event a <${SchemaOrg.EVENT}> ;
             <${SchemaOrg.START_DATE}> ?start ;
             <${SchemaOrg.END_DATE}> ?end .
      OPTIONAL {
        ?event <${SchemaOrg.LOCATION}>/<${Personal.SAME_AS}>* ?location .
        ?location a <${SchemaOrg.PLACE}> ;
                  <${SchemaOrg.GEO}> ?geo .
        ?geo <${SchemaOrg.LATITUDE}> ?lat ;
             <${SchemaOrg.LONGITUDE}> ?lon .
      }
    } ORDER BY ?start"""
  )
  private val staysQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?event ?start ?end ?lat ?lon WHERE {
      ?event a <${Personal.STAY}> ;
            <${SchemaOrg.START_DATE}> ?start ;
            <${SchemaOrg.END_DATE}> ?end ;
            <${SchemaOrg.GEO}> ?geo .
      ?geo <${SchemaOrg.LATITUDE}> ?lat ;
           <${SchemaOrg.LONGITUDE}> ?lon .
    } ORDER BY ?start"""
  )

  /**
    * Run the enrichments defined by this Enricher
    */
  override def enrich(diff: ModelDiff): Unit = {
    val events = getEvents.toBuffer

    Await.result(getStays.map(stay =>
      events
        .filter(event => event.start <= stay.end && stay.start <= event.end)
        .filter(isSharedIntervalBiggerThanRatioOfEvent(stay, _))
        .filter(isNear(stay, _))
        .map(event => {
          repositoryConnection.begin()
          repositoryConnection.add(event.resource, SchemaOrg.LOCATION, stay.resource, inferencerContext)
          repositoryConnection.commit()
          event
      }).size
    ).runFold(0)(_ + _).map(addedConnections => {
      logger.info(s"$addedConnections added between events and stay locations")
    }), Duration.Inf)
  }

  private def isSharedIntervalBiggerThanRatioOfEvent(stay: TimeIntervalResource, event: TimeIntervalResource): Boolean = {
    (Math.min(stay.end, event.end) - Math.max(stay.start, event.start)).toFloat / event.length > overlapMinRatio
  }

  private def isNear(stay: TimeIntervalResource, event: TimeIntervalResource): Boolean = {
    event.coordinates.forall(eventCoordinates =>
      stay.coordinates.forall(stayCoordinates =>
        WGS84SphereHaversinePointMetric.distance(eventCoordinates, stayCoordinates) <= maxLocationDistance
      )
    )
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
      Option(bindingSet.getValue("end").asInstanceOf[Literal]).flatMap(parseLiteralAsTime),
      Option(bindingSet.getValue("lat").asInstanceOf[Literal]).map(_.floatValue()),
      Option(bindingSet.getValue("lon").asInstanceOf[Literal]).map(_.floatValue())
      ) match {
      case (Some(resource), Some(start), Some(end), Some(lat), Some(lon)) =>
        Some(TimeIntervalResource(resource, start, end, Some(Geography.point(lon, lat))))
      case (Some(resource), Some(start), Some(end), None, None) =>
        Some(TimeIntervalResource(resource, start, end))
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

  private case class TimeIntervalResource(resource: Resource, start: Long, end: Long, coordinates: Option[Point] = None) {
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
