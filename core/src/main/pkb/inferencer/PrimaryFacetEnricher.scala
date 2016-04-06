package pkb.inferencer

import org.openrdf.model.vocabulary.{OWL, RDF}
import org.openrdf.model.{Model, Resource, Value}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import pkb.rdf.Converters._
import pkb.rdf.model.ModelDiff
import pkb.rdf.model.vocabulary.Personal

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  *         Works only if the repository does symmetric inference for owl:sameAs
  *         (if owl:sameAs statements are only retrieved from InverseFunctionalPropertyInferencer it is already done)
  */
class PrimaryFacetEnricher(repositoryConnection: RepositoryConnection) extends Enricher {

  private val enricherContext = repositoryConnection.getValueFactory.createIRI("http://thomas.pellissier-tanon.fr/personal#primaryFacetEnricherOutput")
  private val equivalentFacetsOrderedByNumberOfDescriptiveTripleQuery = repositoryConnection.prepareTupleQuery(
    QueryLanguage.SPARQL,
    """SELECT ?facet WHERE {
      ?facet <http://www.w3.org/2002/07/owl#sameAs>* ?startFacet .
      ?facet ?descriptionProperty ?descriptionValue
    } GROUP BY ?facet ORDER BY DESC(COUNT(?descriptionProperty))"""
  )

  override def enrich(diff: ModelDiff): Unit = {
    repositoryConnection.begin()
    recomputePrimaryFacetsFrom(List(diff.added, diff.removed))
    repositoryConnection.commit()
  }

  private def recomputePrimaryFacetsFrom(models: Traversable[Model]): Unit = {
    models
      .map(_.filter(null, OWL.SAMEAS, null))
      .flatMap(model => model.subjects().asScala ++ model.objects().asScala)
      .foreach(recomputePrimaryFacetFrom(_))
  }

  private def recomputePrimaryFacetFrom(startFacet: Value): Unit = {
    val equivalentFacets = getEquivalentFacetsOrderedByNumberOfDescriptiveTriple(startFacet).map(_.asInstanceOf[Resource])
    equivalentFacets
      .find(_ => true)
      .foreach(repositoryConnection.add(_, RDF.TYPE, Personal.PRIMARY_FACET, enricherContext))
    //It removes the possible type from the other facets
    equivalentFacets.foreach(repositoryConnection.remove(_, RDF.TYPE, Personal.PRIMARY_FACET, enricherContext))
  }

  private def getEquivalentFacetsOrderedByNumberOfDescriptiveTriple(startFacet: Value): Iterator[Value] = {
    equivalentFacetsOrderedByNumberOfDescriptiveTripleQuery.setBinding("startFacet", startFacet)
    equivalentFacetsOrderedByNumberOfDescriptiveTripleQuery.evaluate().map(_.getBinding("facet").getValue)
  }
}
