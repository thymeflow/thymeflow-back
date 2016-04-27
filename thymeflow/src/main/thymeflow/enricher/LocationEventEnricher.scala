package thymeflow.enricher

import java.time.{Duration, Instant}

import akka.stream.scaladsl.Source
import org.openrdf.model.{Literal, Resource}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import thymeflow.actors._
import thymeflow.enricher.LocationEventEnricher.{Event, Stay}
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.spatial.geographic.{Geography, Point}

object LocationEventEnricher {

  private case class Stay(resource: Resource,
                          from: Instant,
                          to: Instant,
                          point: Point,
                          accuracy: Double) {
    override def equals(obj: scala.Any): Boolean = {
      obj match {
        case other: Stay if other.isInstanceOf[Stay] =>
          other.resource == resource
        case _ => false
      }
    }

    override def hashCode(): Int = resource.hashCode()
  }

  private case class Event(resource: Resource,
                           from: Instant,
                           to: Instant) {
    override def equals(obj: scala.Any): Boolean = {
      obj match {
        case other: Stay if other.isInstanceOf[Stay] =>
          other.resource == resource
        case _ => false
      }
    }

    override def hashCode(): Int = resource.hashCode()
  }

}

/**
  * @author David Montoya
  */
class LocationEventEnricher(repositoryConnection: RepositoryConnection, val delay: scala.concurrent.duration.Duration) extends DelayedEnricher {

  private val valueFactory = repositoryConnection.getValueFactory
  private val inferencerContext = valueFactory.createIRI("http://thymeflow.com/personal#LocationEventEnricher")

  /**
    * Run the enrichments defined by this Enricher
    */
  override def runEnrichments(): Unit = {

    getEvents.runFold(Vector.empty[Event]) {
      case (v, u) => v :+ u
    }.map {
      case events =>
        getStays.map {
          case stay =>
            repositoryConnection.begin()
            eventsForStay(events, stay).foreach {
              case event =>
                repositoryConnection.add(event, SchemaOrg.LOCATION, stay.resource, inferencerContext)
            }
            repositoryConnection.commit()
        }
    }
  }

  def eventsForStay(events: IndexedSeq[Event], stay: Stay) = {
    import scala.collection.Searching._
    val ordering = new Ordering[Event] {
      override def compare(x: Event, y: Event): Int = {
        val d = Duration.between(x.from, y.from)
        if (d.isNegative) 1 else if (d.isZero) 0 else -1
      }
    }
    val searchValue = Event(stay.resource, stay.from, stay.from)
    events.search(searchValue)(ordering) match {
      case Found(i) =>
        val Event(a, _, ta) = events(math.max(i - 1, events.indices.min))
        val Event(b, tb, _) = events(math.min(i, events.indices.max))
        Some(Seq((a, ta), (b, tb)).map(x => (x._1, Duration.between(stay.from, x._2).abs)).minBy(_._2)._1)
      case InsertionPoint(i) if i > 0 && i <= events.indices.max =>
        val Event(a, _, ta) = events(math.max(i - 1, events.indices.min))
        val Event(b, tb, _) = events(math.min(i, events.indices.max))
        Some(Seq((a, ta), (b, tb)).map(x => (x._1, Duration.between(stay.from, x._2).abs)).minBy(_._2)._1)
      case _ =>
        None
    }
  }

  private def getEvents = {
    val eventsQuery =
      s"""
         |SELECT ?event ?from ?to
         |WHERE {
         |  ?event a <${SchemaOrg.EVENT}> ;
         |          <${SchemaOrg.START_DATE}> ?from ;
         |          <${SchemaOrg.END_DATE}> ?to .
         |} ORDER BY ?from
    """.stripMargin
    Source.fromIterator(() => repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, eventsQuery).evaluate()).map {
      case bindingSet =>
        try {
          val event = bindingSet.getValue("event").asInstanceOf[Resource]
          val from = Instant.parse(bindingSet.getValue("from").stringValue())
          val to = Instant.parse(bindingSet.getValue("to").stringValue())
          Some(Event(event, from, to))
        } catch {
          case e: NumberFormatException => None
          case e: NullPointerException => None
        }
    }.collect {
      case Some(x) => x
    }
  }

  private def getStays = {
    val staysQuery =
      s"""
         |SELECT ?stay ?from ?to ?longitude ?latitude ?uncertainty
         |WHERE {
         |  ?stay a <${Personal.STAY_EVENT}> ;
         |          <${SchemaOrg.GEO}> ?clusterGeo ;
         |          <${SchemaOrg.START_DATE}> ?from ;
         |          <${SchemaOrg.END_DATE}> ?to .
         |  ?geo    <${SchemaOrg.LONGITUDE}> ?longitude ;
         |          <${SchemaOrg.LATITUDE}> ?latitude ;
         |          <${Personal.UNCERTAINTY}> ?uncertainty .
         |} ORDER BY ?from
    """.stripMargin
    Source.fromIterator(() => repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, staysQuery).evaluate()).map {
      case bindingSet =>
        try {
          val stay = bindingSet.getValue("stay").asInstanceOf[Resource]
          val longitude = bindingSet.getValue("longitude").asInstanceOf[Literal].doubleValue()
          val latitude = bindingSet.getValue("latitude").asInstanceOf[Literal].doubleValue()
          val point = Geography.point(longitude, latitude)
          val from = Instant.parse(bindingSet.getValue("from").stringValue())
          val to = Instant.parse(bindingSet.getValue("to").stringValue())
          val uncertainty = bindingSet.getValue("uncertainty").asInstanceOf[Literal].doubleValue()
          Some(Stay(stay, from, to, point, uncertainty))
        } catch {
          case e: NumberFormatException => None
          case e: NullPointerException => None
        }
    }.collect {
      case Some(x) => x
    }
  }

}
