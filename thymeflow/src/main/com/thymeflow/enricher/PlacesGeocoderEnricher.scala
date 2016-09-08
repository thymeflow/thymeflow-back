package com.thymeflow.enricher

import akka.stream.scaladsl.Source
import com.thymeflow.actors._
import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import com.thymeflow.spatial.geocoding.Geocoder
import com.thymeflow.spatial.geographic.{Geography, Point}
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{Literal, Resource}
import org.openrdf.repository.RepositoryConnection

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Geocodes instances of schema:Place with a schema:geo property (longitude/latitude coordinates).
  *
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  *
  *         TODO: geocode places with no name but an address
  */
class PlacesGeocoderEnricher(newRepositoryConnection: () => RepositoryConnection,
                             geocoder: Geocoder,
                             parallelism: Int = 2)
  extends AbstractEnricher(newRepositoryConnection) with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val featureConverter = new FeatureConverter(valueFactory)
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "PlacesGeocoderEnricher")

  override def enrich(diff: ModelDiff): Unit = {
    val places = diff.added.filter(null, RDF.TYPE, SchemaOrg.PLACE).subjects().asScala

    if (places.nonEmpty) {
      repositoryConnection.begin()
      val model = new SimpleHashModel(valueFactory)
      val process = Source.fromIterator(() => places.iterator).mapConcat[(Resource, Option[Point], String)] {
        placeResource =>
          if (
            !repositoryConnection.hasStatement(null, SchemaOrg.ADDRESS_COUNTRY, placeResource, true) &&
              !repositoryConnection.hasStatement(null, SchemaOrg.ADDRESS_REGION, placeResource, true) &&
              !repositoryConnection.hasStatement(null, SchemaOrg.ADDRESS_LOCALITY, placeResource, true)
          ) {
            val points = repositoryConnection.getStatements(placeResource, SchemaOrg.GEO, null, true).map(_.getObject).flatMap {
              case geoResource: Resource =>
                repositoryConnection.getStatements(geoResource, SchemaOrg.LATITUDE, null, true).map(_.getObject).flatMap {
                  case latitude: Literal =>
                    repositoryConnection.getStatements(geoResource, SchemaOrg.LONGITUDE, null, true).map(_.getObject).map {
                      case longitude: Literal => Geography.point(longitude.doubleValue(), latitude.doubleValue())
                    }
                }
            }.toVector
            val names = repositoryConnection.getStatements(placeResource, SchemaOrg.NAME, null, true)
              .map(_.getObject.stringValue()).toVector
            if (points.isEmpty) {
              names.map(name => (placeResource, None, name))
            } else {
              points.flatMap(point => names.map(name => (placeResource, Some(point), name)))
            }
          } else {
            scala.collection.immutable.Iterable.empty[(Resource, Option[Point], String)]
          }
      }.mapAsyncUnordered(parallelism) {
        case (placeResource, pointOption, name) =>
          (pointOption match {
            case Some(point) => geocoder.direct(name, point)
            case None => geocoder.direct(name)
          }).map((placeResource, _))
      }.map {
        case (placeResource, geocoderResults) =>
          if (geocoderResults.size == 1) {
            // We only add the geocoder result if there is only one result
            geocoderResults
              .map(featureConverter.convert(_, model))
              .filterNot(isDifferentFrom(_, placeResource))
              .foreach(resource => {
                model.add(placeResource, Personal.SAME_AS, resource, inferencerContext)
                model.add(resource, Personal.SAME_AS, placeResource, inferencerContext)
              })
          } else if (geocoderResults.size > 1) {
            logger.info(s"${geocoderResults.size} results: $geocoderResults")
          }
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
}
