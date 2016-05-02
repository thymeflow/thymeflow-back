package thymeflow.enricher

import org.openrdf.model.vocabulary.{OWL, RDF}
import org.openrdf.model.{Resource, Value}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.ModelDiff
import thymeflow.rdf.model.vocabulary.Personal

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  *         Works only if the repository does symmetric inference for owl:sameAs
  *         (if owl:sameAs statements are only retrieved from InverseFunctionalPropertyInferencer it is already done)
  *         TODO: will do bad things if owl:sameAs is also used for places
  */
class PrimaryFacetEnricher(repositoryConnection: RepositoryConnection) extends AbstractEnricher(repositoryConnection) {

  private val enricherContext = repositoryConnection.getValueFactory.createIRI(Personal.NAMESPACE, "primaryFacetEnricherOutput")
  private val equivalentFacetsOrderedByNumberOfDescriptiveTripleQuery = repositoryConnection.prepareTupleQuery(
    QueryLanguage.SPARQL,
    s"""SELECT ?facet WHERE {
      ?facet <${OWL.SAMEAS}>* ?startFacet .
      ?facet ?descriptionProperty ?descriptionValue
    } GROUP BY ?facet ORDER BY DESC(COUNT(?descriptionProperty))"""
  )

  override def enrich(diff: ModelDiff): Unit = {
    repositoryConnection.begin()
    List(diff.added, diff.removed)
      .map(_.filter(null, OWL.SAMEAS, null))
      .flatMap(model => model.subjects().asScala ++ model.objects().asScala)
      .foreach(recomputePrimaryFacetFrom(_, diff))
    repositoryConnection.commit()
  }

  private def recomputePrimaryFacetFrom(startFacet: Value, diff: ModelDiff): Unit = {
    val equivalentFacets = getEquivalentFacetsOrderedByNumberOfDescriptiveTriple(startFacet).map(_.asInstanceOf[Resource])
    equivalentFacets
      .find(_ => true)
      .foreach(facet =>
        addStatement(diff, repositoryConnection.getValueFactory.createStatement(facet, RDF.TYPE, Personal.PRIMARY_FACET, enricherContext))
      )
    //It removes the possible type from the other facets
    equivalentFacets.foreach(facet =>
      removeStatement(diff, repositoryConnection.getValueFactory.createStatement(facet, RDF.TYPE, Personal.PRIMARY_FACET, enricherContext))
    )
  }

  private def getEquivalentFacetsOrderedByNumberOfDescriptiveTriple(startFacet: Value): Iterator[Value] = {
    equivalentFacetsOrderedByNumberOfDescriptiveTripleQuery.setBinding("startFacet", startFacet)
    equivalentFacetsOrderedByNumberOfDescriptiveTripleQuery.evaluate().map(_.getBinding("facet").getValue)
  }
}
