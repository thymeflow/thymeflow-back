package com.thymeflow.enricher

import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.StatementSetDiff
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import org.eclipse.rdf4j.model.vocabulary.RDF
import org.eclipse.rdf4j.model.{Resource, Value}
import org.eclipse.rdf4j.query.QueryLanguage
import org.eclipse.rdf4j.repository.RepositoryConnection

/**
  * @author Thomas Pellissier Tanon
  *         TODO: will do bad things if owl:sameAs is also used for places
  */
class PrimaryFacetEnricher(newRepositoryConnection: () => RepositoryConnection) extends AbstractEnricher(newRepositoryConnection) {

  private val enricherContext = repositoryConnection.getValueFactory.createIRI(Personal.NAMESPACE, "primaryFacetEnricherOutput")
  private val equivalentFacetsOrderedByNumberOfDescriptiveTripleQuery = repositoryConnection.prepareTupleQuery(
    QueryLanguage.SPARQL,
    s"""SELECT ?facet WHERE {
      {
        SELECT ?facet {
          ?facet <${Personal.SAME_AS}>* ?startFacet .
        }
      }
      ?facet ?descriptionProperty ?descriptionValue .
    } GROUP BY ?facet ORDER BY DESC(COUNT(?descriptionProperty))"""
  )

  private val primaryFacetTypes = Set(SchemaOrg.PLACE, SchemaOrg.EVENT, Personal.AGENT)

  override def enrich(diff: StatementSetDiff): Unit = {
    repositoryConnection.begin()

    // list resources of type belonging to primaryFacetTypes
    val resourcesWithCandidatePrimaryFacetType = primaryFacetTypes.flatMap {
      facetType =>
        List(diff.added, diff.removed)
          .map(_.filter(statement => statement.getPredicate == RDF.TYPE && statement.getObject == facetType))
          .flatMap(statements => statements.subjects)
          .toSet
    }

    // list resources with personal:sameAs relationships
    val resourcesWithSameAsFacets = List(diff.added, diff.removed)
      .map(_.filter(_.getPredicate == Personal.SAME_AS))
      .flatMap(model => model.subjects ++ model.objects.collect { case r: Resource => r })
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
      equivalenceClass =>
        // sameAsFacetClass is never empty
        facetsWithoutPrimaryFacetBuilder ++= equivalenceClass.tail
        (equivalenceClass.head, equivalenceClass)
    }

    addStatements(diff, newPrimaryFacets
      .map(x => repositoryConnection.getValueFactory.createStatement(x._1, RDF.TYPE, Personal.PRIMARY_FACET, enricherContext)))

    newPrimaryFacets.foreach {
      case (primary, others) =>
        addStatements(diff, others.map(repositoryConnection.getValueFactory.createStatement(_, Personal.PRIMARY_FACET_PROPERTY, primary, enricherContext)))
    }

    removeStatements(diff, facetsWithoutPrimaryFacetBuilder.result()
      .flatMap(facet =>
        repositoryConnection.getStatements(facet, RDF.TYPE, Personal.PRIMARY_FACET, enricherContext) ++
          repositoryConnection.getStatements(null, Personal.PRIMARY_FACET_PROPERTY, facet, enricherContext)
      )
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
