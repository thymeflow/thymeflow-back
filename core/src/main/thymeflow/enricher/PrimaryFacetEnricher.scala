package thymeflow.enricher

import org.openrdf.model.vocabulary.RDF
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
      ?facet <${Personal.SAME_AS}>* ?startFacet .
      ?facet ?descriptionProperty ?descriptionValue
    } GROUP BY ?facet ORDER BY DESC(COUNT(?descriptionProperty))"""
  )

  override def enrich(diff: ModelDiff): Unit = {
    repositoryConnection.begin()
    val equivalentFacetsLists = List(diff.added, diff.removed)
      .map(_.filter(null, Personal.SAME_AS, null))
      .flatMap(model => model.subjects().asScala ++ model.objects().asScala)
      .toSet
      .map(getEquivalentFacetsOrderedByNumberOfDescriptiveTriple)

    addStatements(diff, equivalentFacetsLists
      .flatMap(_.headOption)
      .map(repositoryConnection.getValueFactory.createStatement(_, RDF.TYPE, Personal.PRIMARY_FACET, enricherContext))
      .asJavaCollection
    )
    removeStatements(diff, equivalentFacetsLists
      .flatMap(_.tail)
      .flatMap(repositoryConnection.getStatements(_, RDF.TYPE, Personal.PRIMARY_FACET, enricherContext))
      .asJavaCollection
    )
    repositoryConnection.commit()
  }

  private def getEquivalentFacetsOrderedByNumberOfDescriptiveTriple(startFacet: Value): Seq[Resource] = {
    equivalentFacetsOrderedByNumberOfDescriptiveTripleQuery.setBinding("startFacet", startFacet)
    equivalentFacetsOrderedByNumberOfDescriptiveTripleQuery.evaluate()
      .map(_.getBinding("facet").getValue.asInstanceOf[Resource]).toSeq
  }
}
