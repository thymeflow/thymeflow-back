package thymeflow.enricher

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.{OWL, RDF}
import org.openrdf.model.{Model, Resource}
import org.openrdf.repository.RepositoryConnection
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import thymeflow.spatial.geocoding.{Feature, Geocoder}
import thymeflow.sync.converter.utils.{GeoCoordinatesConverter, PostalAddressConverter, UUIDConverter}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * @author Thomas Pellissier Tanon
  */
class GeocoderEnricher(repositoryConnection: RepositoryConnection, geocoder: Geocoder) extends Enricher with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val geoCoordinatesConverter = new GeoCoordinatesConverter(valueFactory)
  private val uuidConverter = new UUIDConverter(valueFactory)
  private val postalAddressConverter = new PostalAddressConverter(valueFactory)
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "GeocoderEnricher")

  override def enrich(diff: ModelDiff): Unit = {
    repositoryConnection.begin()
    val model = new SimpleHashModel(valueFactory)
    diff.added.filter(null, RDF.TYPE, SchemaOrg.PLACE).subjects().asScala.foreach(placeIri => {
      if (repositoryConnection.hasStatement(placeIri, SchemaOrg.ADDRESS, null, true)) {
        return
      }

      val geocoderResults = Await.result(Future.sequence(
        repositoryConnection.getStatements(placeIri, SchemaOrg.NAME, null, true)
          .map(_.getObject.stringValue())
          .filter(str => str.contains(",") || str.contains("\n")) //We geocode only places with "," the other are probably addresses components
          .map(geocoder.direct)
      ), Duration.Inf).flatMap(identity) //TODO: what if the request failed?

      if (geocoderResults.size == 1) {
        //We only add the geocoder result if there is only one result
        geocoderResults.map(addFeatureToModel(_, model))
          .map({
            model.add(placeIri, OWL.SAMEAS, _)
            model.add(_, OWL.SAMEAS, placeIri)
          })
      } else if (geocoderResults.size > 1) {
        logger.info(s"${geocoderResults.size} results: $geocoderResults")
      }
    })
    repositoryConnection.add(model)
    repositoryConnection.commit()
  }

  private def addFeatureToModel(feature: Feature, model: Model): Resource = {
    val placeResource = valueFactory.createIRI(feature.source.iri)

    model.add(placeResource, RDF.TYPE, SchemaOrg.PLACE)
    feature.name.foreach(name =>
      model.add(placeResource, SchemaOrg.NAME, valueFactory.createLiteral(name), inferencerContext)
    )
    model.add(placeResource, SchemaOrg.ADDRESS, postalAddressConverter.convert(feature.address, model)) //TODO: context
    model.add(placeResource, SchemaOrg.GEO, geoCoordinatesConverter.convert(feature.point.latitude, feature.point.longitude, None, None, model), inferencerContext)

    placeResource
  }
}
