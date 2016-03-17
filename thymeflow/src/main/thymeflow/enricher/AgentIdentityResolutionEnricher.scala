package thymeflow.enricher

import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import pkb.rdf.Converters._
import pkb.rdf.model.vocabulary.SchemaOrg

/**
  * @author David Montoya
  */
class AgentIdentityResolutionEnricher(repositoryConnection: RepositoryConnection) {

  def enrich() = {

    val query =
      s"""
         |PREFIX ex: <http://example.com/exampleOntology#>
         |SELECT ?agent ?name ?emailAddress
         |WHERE {
         |  ?agent ${SchemaOrg.NAME} ?name ;
         |         ${SchemaOrg.EMAIL} ?x .
         |  ?x ${SchemaOrg.NAME} ?emailAddress .
         |}
      """.stripMargin
    val result = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate()
    val agents = result.map {
      bindingSet =>
        (bindingSet.getValue("agent").stringValue(), bindingSet.getValue("name").stringValue(), bindingSet.getValue("emailAddress").stringValue())
    }.toVector
    val grouped = agents.groupBy(_._3).mapValues {
      case g =>
        (g.map(_._1), g.map(_._2))
    }
    grouped
  }

}
