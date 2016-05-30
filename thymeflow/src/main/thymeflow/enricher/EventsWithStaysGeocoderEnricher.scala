package thymeflow.enricher

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.{Literal, Resource}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import thymeflow.spatial.geocoding.Geocoder
import thymeflow.spatial.geographic.Geography

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * @author Thomas Pellissier Tanon
  */
class EventsWithStaysGeocoderEnricher(repositoryConnection: RepositoryConnection, geocoder: Geocoder)
  extends AbstractEnricher(repositoryConnection) with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val featureConverter = new FeatureConverter(valueFactory)
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "EventsWithStayGeocoderEnricher")
  private val eventsWithStaysWithoutPlacesWithGeocoodinatesQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?event ?place ?placeName ?lat ?lon WHERE {
      ?event <${SchemaOrg.LOCATION}> ?stay .
      ?stay a <${Personal.STAY}> ;
            <${SchemaOrg.GEO}> ?geo .
      ?geo <${SchemaOrg.LATITUDE}> ?lat ;
           <${SchemaOrg.LONGITUDE}> ?lon .

      OPTIONAL {
        ?event <${SchemaOrg.LOCATION}> ?place .
        ?place a <${SchemaOrg.PLACE}> ;
               <${SchemaOrg.NAME}> ?placeName .

        FILTER NOT EXISTS {
          ?place <${Personal.SAME_AS}>*/<${SchemaOrg.GEO}> ?geoPlace .
        }
      }
    }"""
  )

  override def enrich(diff: ModelDiff): Unit = {
    repositoryConnection.begin()
    val model = new SimpleHashModel(valueFactory)

    eventsWithStaysWithoutPlacesWithGeocoodinatesQuery.evaluate().flatMap(bindingSet =>
      (
        Option(bindingSet.getValue("event").asInstanceOf[Resource]),
        Option(bindingSet.getValue("place").asInstanceOf[Resource]),
        Option(bindingSet.getValue("placeName")).map(_.stringValue()),
        Option(bindingSet.getValue("lat").asInstanceOf[Literal]).map(_.floatValue()),
        Option(bindingSet.getValue("lon").asInstanceOf[Literal]).map(_.floatValue())
      ) match {
        case (_, Some(place), Some(name), Some(lat), Some(lon)) =>
          Some(Right(place), geocoder.direct(name, Geography.point(lon, lat)))
        case (Some(event), None, None, Some(lat), Some(lon)) =>
          Some(Left(event), geocoder.reverse(Geography.point(lon, lat)))
        case _ => None
      }
    ).foreach(t => Await.result(t._2, Duration.Inf).headOption.foreach(feature => {
      //TODO: We keep only the first feature because Google geocoder returns also upper features ("Cachan" for "ENS Cachan"...)
      val resource = featureConverter.convert(feature, model)
      t._1 match {
        case Left(event) =>
          model.add(event, SchemaOrg.LOCATION, resource, inferencerContext)
        case Right(place) =>
          model.add(place, Personal.SAME_AS, resource, inferencerContext)
          model.add(resource, Personal.SAME_AS, place, inferencerContext)
      }
    }))

    addStatements(diff, model)
    repositoryConnection.commit()
  }
}
