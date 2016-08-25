package com.thymeflow.enricher

import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import com.thymeflow.spatial.geocoding.Geocoder
import com.thymeflow.spatial.geographic.Geography
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{Literal, Resource}
import org.openrdf.repository.RepositoryConnection

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * @author Thomas Pellissier Tanon
  *
  *         TODO: geocode places with no name but an address
  */
class PlacesGeocoderEnricher(newRepositoryConnection: () => RepositoryConnection, geocoder: Geocoder)
  extends AbstractEnricher(newRepositoryConnection) with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val featureConverter = new FeatureConverter(valueFactory)
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "PlacesGeocoderEnricher")

  override def enrich(diff: ModelDiff): Unit = {
    val places = diff.added.filter(null, RDF.TYPE, SchemaOrg.PLACE).subjects().asScala

    if (places.nonEmpty) {
      repositoryConnection.begin()
      val model = new SimpleHashModel(valueFactory)
      places.foreach {
        placeResource =>
          if (
            !repositoryConnection.hasStatement(null, SchemaOrg.ADDRESS_COUNTRY, placeResource, true) &&
              !repositoryConnection.hasStatement(null, SchemaOrg.ADDRESS_REGION, placeResource, true) &&
              !repositoryConnection.hasStatement(null, SchemaOrg.ADDRESS_LOCALITY, placeResource, true)
          ) {
            val coordinates = repositoryConnection.getStatements(placeResource, SchemaOrg.GEO, null, true).map(_.getObject).flatMap {
              case geoResource: Resource =>
                repositoryConnection.getStatements(geoResource, SchemaOrg.LATITUDE, null, true).map(_.getObject).flatMap {
                  case latitude: Literal =>
                    repositoryConnection.getStatements(geoResource, SchemaOrg.LONGITUDE, null, true).map(_.getObject).map {
                      case longitude: Literal => Geography.point(longitude.doubleValue(), latitude.doubleValue())
                    }
                }
            }.toTraversable

            val geocoderResults = Await.result(Future.sequence(
              repositoryConnection.getStatements(placeResource, SchemaOrg.NAME, null, true)
                .map(_.getObject.stringValue())
                .flatMap(name =>
                  if (coordinates.isEmpty) {
                    Some(geocoder.direct(name))
                  } else {
                    coordinates.map(geocoder.direct(name, _))
                  }
                )
            ), Duration.Inf).flatMap(identity).toTraversable //TODO: what if the request failed?

            if (geocoderResults.size == 1) {
              //We only add the geocoder result if there is only one result
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
      }
      addStatements(diff, model)
      repositoryConnection.commit()
    }
  }
}
