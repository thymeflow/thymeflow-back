package thymeflow.enricher

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.{OWL, RDF}
import org.openrdf.model.{Literal, Model, Resource}
import org.openrdf.repository.RepositoryConnection
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import thymeflow.spatial.geocoding.{Feature, Geocoder}
import thymeflow.spatial.geographic.impl.Point
import thymeflow.sync.converter.utils.{GeoCoordinatesConverter, PostalAddressConverter}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * @author Thomas Pellissier Tanon
  *
  *         TODO: geocode places with no name but an address
  */
class PlacesGeocoderEnricher(repositoryConnection: RepositoryConnection, geocoder: Geocoder) extends Enricher with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val geoCoordinatesConverter = new GeoCoordinatesConverter(valueFactory)
  private val postalAddressConverter = new PostalAddressConverter(valueFactory)
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "PlacesGeocoderEnricher")

  override def enrich(diff: ModelDiff): Unit = {
    repositoryConnection.begin()
    val model = new SimpleHashModel(valueFactory)

    diff.added.filter(null, RDF.TYPE, SchemaOrg.PLACE).subjects().asScala.foreach(placeResource =>
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
                  case longitude: Literal => Point(longitude.doubleValue(), latitude.doubleValue())
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
          geocoderResults.foreach(feature => {
            val resource = addFeatureToModel(feature, model)
            model.add(placeResource, OWL.SAMEAS, resource, inferencerContext)
            model.add(resource, OWL.SAMEAS, placeResource, inferencerContext)
          })
        } else if (geocoderResults.size > 1) {
          logger.info(s"${geocoderResults.size} results: $geocoderResults")
        }
      }
    )

    repositoryConnection.add(model)
    repositoryConnection.commit()
  }

  private def addFeatureToModel(feature: Feature, model: Model): Resource = {
    val placeResource = valueFactory.createIRI(feature.source.iri)

    model.add(placeResource, RDF.TYPE, SchemaOrg.PLACE, inferencerContext)
    feature.name.foreach(name =>
      model.add(placeResource, SchemaOrg.NAME, valueFactory.createLiteral(name), inferencerContext)
    )
    model.add(placeResource, SchemaOrg.ADDRESS, postalAddressConverter.convert(feature.address, model, inferencerContext), inferencerContext)
    model.add(placeResource, SchemaOrg.GEO, geoCoordinatesConverter.convert(feature.point.latitude, feature.point.longitude, None, None, model), inferencerContext)

    placeResource
  }
}
