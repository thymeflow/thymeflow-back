package com.thymeflow.enricher

import akka.stream.scaladsl.Source
import com.thymeflow.actors._
import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import com.thymeflow.spatial.geocoding.Geocoder
import com.thymeflow.spatial.geographic.Geography
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.{Literal, Resource}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * @author Thomas Pellissier Tanon
  */
class EventsWithStaysGeocoderEnricher(newRepositoryConnection: () => RepositoryConnection,
                                      geocoder: Geocoder,
                                      parallelism: Int = 2)
  extends AbstractEnricher(newRepositoryConnection) with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val featureConverter = new FeatureConverter(valueFactory)
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "EventsWithStayGeocoderEnricher")
  private val eventsWithStaysWithoutPlacesWithGeocoodinatesQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?event ?stay ?place ?placeName ?lat ?lon WHERE {
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

    val process = Source.fromIterator(() => eventsWithStaysWithoutPlacesWithGeocoodinatesQuery.evaluate().toVector.iterator).mapAsyncUnordered(parallelism) {
      bindingSet =>
        (
          Option(bindingSet.getValue("event").asInstanceOf[Resource]),
          Option(bindingSet.getValue("stay").asInstanceOf[Resource]),
          Option(bindingSet.getValue("place").asInstanceOf[Resource]),
          Option(bindingSet.getValue("placeName")).map(_.stringValue()),
          Option(bindingSet.getValue("lat").asInstanceOf[Literal]).map(_.floatValue()),
          Option(bindingSet.getValue("lon").asInstanceOf[Literal]).map(_.floatValue())
          ) match {
          case (_, Some(stay), Some(place), Some(name), Some(lat), Some(lon)) =>
            geocoder.direct(name, Geography.point(lon, lat)).flatMap {
              results1 =>
                geocoder.reverse(Geography.point(lon, lat)).flatMap {
                  results2 => geocoder.direct(name).map {
                    results3 => Some((Right(place), stay, results1, results2, results3))
                  }
                }
            }
          case (Some(event), Some(stay), None, None, Some(lat), Some(lon)) =>
            geocoder.reverse(Geography.point(lon, lat)).map {
              results => Some((Left(event), stay, results, Vector.empty, Vector.empty))
            }
          case _ => Future.successful(None)
        }
    }.collect {
      case Some((resource, stayResource, results1, results2, results3)) =>
        // TODO: We keep only the first feature
        results1.headOption.foreach(feature => {
          val featureResource = featureConverter.convert(feature, model)
          resource match {
            case Left(event) =>
              model.add(event, SchemaOrg.LOCATION, featureResource, stayResource)
            case Right(place) =>
              if (!isDifferentFrom(place, featureResource)) {
                model.add(place, Personal.SAME_AS, featureResource, stayResource)
                model.add(featureResource, Personal.SAME_AS, place, stayResource)
              }
          }
        })
        results2.headOption.foreach(feature => {
          val featureResource = featureConverter.convert(feature, model)
          resource match {
            case Left(event) =>
            case Right(place) =>
              if (!isDifferentFrom(place, featureResource)) {
                model.add(place, Personal.SAME_AS_TEMP, featureResource, stayResource)
                model.add(featureResource, Personal.SAME_AS_TEMP, place, stayResource)
              }
          }
        })
        results3.headOption.foreach(feature => {
          val featureResource = featureConverter.convert(feature, model)
          resource match {
            case Left(event) =>
            case Right(place) =>
              if (!isDifferentFrom(place, featureResource)) {
                val property = if (results3.size == 1) Personal.SAME_AS_TEMP_SINGLE else Personal.SAME_AS_TEMP_AMBIGUOUS
                model.add(place, property, featureResource, stayResource)
              }
          }
        })
    }
    try {
      val done = process.runForeach((x) => ())
      Await.ready(done, Duration.Inf)
    } catch {
      case e: Exception =>
        // TODO: Can we try later if this is due to an API rate limit ?
        logger.error(s"Error while running the ${this.getClass.getName}.", e)
    }
    addStatements(diff, model)
    repositoryConnection.commit()
  }
}
