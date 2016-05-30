package thymeflow.enricher

import org.openrdf.model.{Model, Statement}
import org.openrdf.repository.RepositoryConnection
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.ModelDiff
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  *
  *         An inferencer that uses some owl:InverseFunctionalProperty to add owl:sameAs statements
  */
class InverseFunctionalPropertyInferencer(repositoryConnection: RepositoryConnection)
  extends InferenceCountingInferencer(repositoryConnection) {

  private val inverseFunctionalProperties = Set(SchemaOrg.TELEPHONE, SchemaOrg.EMAIL, SchemaOrg.URL)

  private val valueFactory = repositoryConnection.getValueFactory

  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "inverseFunctionalInferencerOutput")

  override def enrich(diff: ModelDiff): Unit = {
    repositoryConnection.begin()

    //add
    getInferencesWithOnePremiseInWithInverseFunctionalProperty(diff.added).foreach(statement =>
      addStatement(diff, statement)
    )

    //remove
    getInferencesWithOnePremiseInWithInverseFunctionalProperty(diff.removed).foreach(statement =>
      removeStatement(diff, statement)
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
            valueFactory.createStatement(statement1.getSubject, Personal.SAME_AS, statement2.getSubject, inferencerContext),
            valueFactory.createStatement(statement2.getSubject, Personal.SAME_AS, statement1.getSubject, inferencerContext)
          )
        )
      )
    })
  }
}
