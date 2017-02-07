package com.thymeflow.enricher

import akka.stream.scaladsl.Source
import com.thymeflow.actors._
import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.thymeflow.rdf.model.{ModelDiff, StatementSet}
import com.thymeflow.spatial.geocoding.Geocoder
import com.thymeflow.spatial.geographic.{Geography, Point}
import com.typesafe.scalalogging.StrictLogging
import org.eclipse.rdf4j.model.vocabulary.RDF
import org.eclipse.rdf4j.model.{Literal, Resource}
import org.eclipse.rdf4j.repository.RepositoryConnection

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
  private val uncertainInferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "UncertainPlacesGeocoderEnricher")

  override def enrich(diff: ModelDiff): Unit = {
    val places = diff.added.collect {
      case statement if statement.getPredicate == RDF.TYPE && statement.getObject == SchemaOrg.PLACE =>
        statement.getSubject
    }

    if (places.nonEmpty) {
      repositoryConnection.begin()
      val statements = StatementSet.empty(valueFactory)
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
          // We only add the first result.
          // If more than 1 result is returned, we consider the relation "Uncertain"
          val context =
            if (geocoderResults.size > 1) {
              uncertainInferencerContext
            } else {
              inferencerContext
            }
          geocoderResults.headOption
            .map(featureConverter.convert(_, statements))
            .filterNot(isDifferentFrom(_, placeResource))
            .foreach(resource => {
              statements.add(placeResource, Personal.SAME_AS, resource, context)
              statements.add(resource, Personal.SAME_AS, placeResource, context)
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
      addStatements(diff, statements)
      repositoryConnection.commit()
    }
  }
}
