package pkb.enricher

import org.openrdf.model.vocabulary.OWL
import org.openrdf.model.{Model, Statement}
import org.openrdf.repository.RepositoryConnection
import pkb.rdf.Converters._
import pkb.rdf.model.ModelDiff
import pkb.rdf.model.vocabulary.SchemaOrg

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  *
  *         An inferencer that uses some owl:InverseFunctionalProperty to add owl:sameAs statements
  */
class InverseFunctionalPropertyInferencer(repositoryConnection: RepositoryConnection)
  extends AbstractInferrencer(repositoryConnection) {

  private val inverseFunctionalProperties = Set(SchemaOrg.ADDRESS, SchemaOrg.TELEPHONE, SchemaOrg.EMAIL, SchemaOrg.URL)

  private val valueFactory = repositoryConnection.getValueFactory

  private val inferencerContext = valueFactory.createIRI("http://thomas.pellissier-tanon.fr/personal#inverseFunctionalInferencerOutput")

  override def enrich(diff: ModelDiff): Unit = {
    repositoryConnection.begin()

    //add
    getInferencesWithOnePremiseInWithInverseFunctionalProperty(diff.added).foreach(statement =>
      addInferredStatement(diff, statement)
    )

    //remove
    getInferencesWithOnePremiseInWithInverseFunctionalProperty(diff.removed).foreach(statement =>
      removeInferredStatement(diff, statement)
    )

    repositoryConnection.commit()
  }

  private def getInferencesWithOnePremiseInWithInverseFunctionalProperty(model: Model): Traversable[Statement] = {
    inverseFunctionalProperties.flatMap(inverseFunctionalProperty => {
      model.filter(null, inverseFunctionalProperty, null).asScala.flatMap(statement1 =>
        (
          model.filter(null, inverseFunctionalProperty, statement1.getObject).asScala
            ++
            repositoryConnection.getStatements(null, inverseFunctionalProperty, statement1.getObject, true)
          ).flatMap(statement2 =>
          Array(
            valueFactory.createStatement(statement1.getSubject, OWL.SAMEAS, statement2.getSubject, inferencerContext),
            valueFactory.createStatement(statement2.getSubject, OWL.SAMEAS, statement1.getSubject, inferencerContext)
          )
        )
      )
    })
  }
}
