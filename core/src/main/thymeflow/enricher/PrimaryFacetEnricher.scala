package thymeflow.enricher

import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{Resource, Value}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.ModelDiff
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
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

  private val primaryFacetTypes = Set(SchemaOrg.PLACE, SchemaOrg.EVENT, Personal.AGENT)

  override def enrich(diff: ModelDiff): Unit = {
    repositoryConnection.begin()

    // list resources of type belonging to primaryFacetTypes
    val resourcesWithCandidatePrimaryFacetType = primaryFacetTypes.flatMap {
      case facetType =>
        List(diff.added, diff.removed)
          .map(_.filter(null, RDF.TYPE, facetType))
          .flatMap(model => model.subjects().asScala)
          .toSet
    }

    // list resources with personal:sameAs relationships
    val resourcesWithSameAsFacets = List(diff.added, diff.removed)
      .map(_.filter(null, Personal.SAME_AS, null))
      .flatMap(model => model.subjects().asScala ++ model.objects().asScala.collect { case r: Resource => r })
      .toSet

    // candidate resources are either resources with a type belonging to primaryFacetTypes
    // or with a personal:sameAs relationship
    val candidateResources = resourcesWithSameAsFacets ++ resourcesWithCandidatePrimaryFacetType

    val processedResourcesSet = new scala.collection.mutable.HashSet[Resource]
    val facetsWithoutPrimaryFacetBuilder = Vector.newBuilder[Resource]
    val equivalentClassesListBuilder = Vector.newBuilder[Seq[Resource]]

    // for each candidate resource, compute a list of its equivalent facets,
    // ordered by the number of descriptive triples.
    candidateResources.foreach {
      candidateResource =>
        if (!processedResourcesSet.contains(candidateResource)) {
          val equivalents = getEquivalentFacetsOrderedByNumberOfDescriptiveTriple(candidateResource)
          if (equivalents.isEmpty) {
            // if the list of equivalent facets is empty, then this means
            // that the resource is not in the KB anymore.
            facetsWithoutPrimaryFacetBuilder += candidateResource
          } else {
            equivalentClassesListBuilder += equivalents
            // mark equivalent facets as processed
            equivalents.map {
              equivalent => processedResourcesSet += equivalent
            }
          }
        }
    }

    val newPrimaryFacets = equivalentClassesListBuilder.result().map {
      case equivalenceClass =>
        // sameAsFacetClass is never empty
        facetsWithoutPrimaryFacetBuilder ++= equivalenceClass.tail
        equivalenceClass.head
    }

    addStatements(diff, newPrimaryFacets
      .map(repositoryConnection.getValueFactory.createStatement(_, RDF.TYPE, Personal.PRIMARY_FACET, enricherContext))
      .asJavaCollection
    )
    removeStatements(diff, facetsWithoutPrimaryFacetBuilder.result()
      .flatMap(repositoryConnection.getStatements(_, RDF.TYPE, Personal.PRIMARY_FACET, enricherContext))
      .asJavaCollection
    )

    repositoryConnection.commit()
  }

  private def getEquivalentFacetsOrderedByNumberOfDescriptiveTriple(startFacet: Value): Seq[Resource] = {
    equivalentFacetsOrderedByNumberOfDescriptiveTripleQuery.setBinding("startFacet", startFacet)
    equivalentFacetsOrderedByNumberOfDescriptiveTripleQuery.evaluate()
      .flatMap(row => Option(row.getBinding("facet")).map(_.getValue).collect { case r: Resource => r })
      .toSeq
  }
}
