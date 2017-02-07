package com.thymeflow.enricher

import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.thymeflow.rdf.model.{StatementSet, StatementSetDiff}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.repository.RepositoryConnection

/**
  * @author Thomas Pellissier Tanon
  *
  *         An inferencer that uses some owl:InverseFunctionalProperty to add owl:sameAs statements
  */
class InverseFunctionalPropertyInferencer(newRepositoryConnection: () => RepositoryConnection)
  extends InferenceCountingInferencer(newRepositoryConnection) {

  private val inverseFunctionalProperties = Set(SchemaOrg.TELEPHONE, SchemaOrg.EMAIL, SchemaOrg.URL)
  private val valueFactory = repositoryConnection.getValueFactory
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "inverseFunctionalInferencerOutput")

  override def enrich(diff: StatementSetDiff): Unit = {
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

  private def getInferencesWithOnePremiseInWithInverseFunctionalProperty(statements: StatementSet): Traversable[Statement] = {
    inverseFunctionalProperties.flatMap(inverseFunctionalProperty => {
      statements.filter(_.getPredicate == inverseFunctionalProperty).flatMap(
        statement1 =>
          (statements.filter(statement => statement.getPredicate == inverseFunctionalProperty && statement.getObject == statement1.getObject) ++
            repositoryConnection.getStatements(null, inverseFunctionalProperty, statement1.getObject, true))
            .map(_.getSubject)
            .filterNot(isDifferentFrom(statement1.getSubject, _))
          .flatMap(subject2 =>
            Array(
              valueFactory.createStatement(statement1.getSubject, Personal.SAME_AS, subject2, inferencerContext),
              valueFactory.createStatement(subject2, Personal.SAME_AS, statement1.getSubject, inferencerContext)
            )
          )
      )
    })
  }
}
