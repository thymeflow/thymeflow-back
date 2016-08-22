package com.thymeflow.rdf.sail.inferencer

import com.thymeflow.rdf.Converters._
import org.openrdf.model.impl.TreeModel
import org.openrdf.model.vocabulary.{OWL, RDF, RDFS}
import org.openrdf.model.{IRI, Model}
import org.openrdf.sail.Sail
import org.openrdf.sail.inferencer.InferencerConnection
import org.openrdf.sail.inferencer.fc.AbstractForwardChainingInferencerConnection

import scala.collection.JavaConverters._

/**
  * Simple OWL inferencer supporting owl:equivalentClass, owl:equivalentProperty, owl:inverseOf, owl:SymmetricProperty,
  * owl:TransitiveProperty
  * Includes also a definition of owl:FunctionalProperty, owl:InverseFunctionalProperty and owl:sameAs
  *
  * @author Thomas Pellissier Tanon
  */
class ForwardChainingSimpleOWLInferencerConnection(sail: Sail, con: InferencerConnection)
  extends AbstractForwardChainingInferencerConnection(sail, con) {

  override def applyRules(iteration: Model): Int = {
    applyInverseOf1(iteration) + applyInverseOf2(iteration) +
      applySymmetricProperty1(iteration) + applySymmetricProperty2(iteration) +
      applyTransitiveProperty1(iteration) + applyTransitiveProperty21(iteration) + applyTransitiveProperty22(iteration)
  }

  // p owl:inverseOf p' (nt) && x p' y (t1) ---> y p x
  private def applyInverseOf1(iteration: Model): Int = {
    iteration.filter(null, OWL.INVERSEOF, null).asScala.map(inverseStatement => {
      (inverseStatement.getSubject, inverseStatement.getObject) match {
        case (property1: IRI, property2: IRI) =>
          getWrappedConnection.getStatements(null, property2, null, true).count(statement => {
            statement.getObject match {
              case obj: IRI => addInferredStatement(obj, property1, statement.getSubject)
              case _ => false
            }
          })
        case _ => 0
      }
    }).sum
  }

  // x p' y (nt) && p owl:inverseOf p' ---> y p x
  private def applyInverseOf2(iteration: Model): Int = {
    getWrappedConnection.getStatements(null, OWL.INVERSEOF, null, true).map(inverseStatement => {
      (inverseStatement.getSubject, inverseStatement.getObject) match {
        case (property1: IRI, property2: IRI) =>
          iteration.filter(null, property2, null).asScala.count(statement => {
            statement.getObject match {
              case obj: IRI => addInferredStatement(obj, property1, statement.getSubject)
              case _ => false
            }
          })
        case _ => 0
      }
    }).sum
  }

  // p rdf:type owl:SymmetricProperty (nt) && x p y (t1) ---> y p x
  private def applySymmetricProperty1(iteration: Model): Int = {
    iteration.filter(null, RDF.TYPE, OWL.SYMMETRICPROPERTY).subjects().asScala.map {
      case property: IRI => getWrappedConnection.getStatements(null, property, null, true).count(statement => {
        statement.getObject match {
          case obj: IRI => addInferredStatement(obj, property, statement.getSubject)
          case _ => false
        }
      })
    }.sum
  }

  // x p y (nt) && p rdf:type owl:SymmetricProperty ---> y p x
  private def applySymmetricProperty2(iteration: Model): Int = {
    getWrappedConnection.getStatements(null, RDF.TYPE, OWL.SYMMETRICPROPERTY, true).map(//TODO: is it clever to iterate on all properties?
      _.getSubject match {
        case property: IRI => iteration.filter(null, property, null).asScala.count(statement => {
          statement.getObject match {
            case obj: IRI => addInferredStatement(obj, property, statement.getSubject)
            case _ => false
          }
        })
        case _ => 0
      }
    ).sum
  }

  // p rdf:type owl:TransitiveProperty (nt) && y p z (t1) && x p y (t2) ---> x p z
  private def applyTransitiveProperty1(iteration: Model): Int = {
    iteration.filter(null, RDF.TYPE, OWL.TRANSITIVEPROPERTY).subjects().asScala.map {
      case property: IRI => getWrappedConnection.getStatements(null, property, null, true).map(statement1 => {
        getWrappedConnection.getStatements(null, property, statement1.getSubject, true).count(statement2 => {
          addInferredStatement(statement2.getSubject, property, statement1.getObject)
        })
      }).sum
      case _ => 0
    }.sum
  }

  // y p z (nt) && x p y (t2) && p rdf:type owl:TransitiveProperty (t1) ---> x p z
  private def applyTransitiveProperty21(iteration: Model): Int = {
    getWrappedConnection.getStatements(null, RDF.TYPE, OWL.TRANSITIVEPROPERTY, true).map(
      _.getSubject match {
        case property: IRI => iteration.filter(null, property, null).asScala.map(statement1 => {
          getWrappedConnection.getStatements(null, property, statement1.getSubject, true).count(statement2 => {
            addInferredStatement(statement2.getSubject, property, statement1.getObject)
          })
        }).sum
        case _ => 0
      }
    ).sum
  }

  // x p y (nt) && y p z (t2) && p rdf:type owl:TransitiveProperty (t1) ---> x p z
  private def applyTransitiveProperty22(iteration: Model): Int = {
    getWrappedConnection.getStatements(null, RDF.TYPE, OWL.TRANSITIVEPROPERTY, true).map(
      _.getSubject match {
        case property: IRI => iteration.filter(null, property, null).asScala.map(statement1 => {
          statement1.getObject match {
            case obj: IRI => getWrappedConnection.getStatements(obj, property, null, true).count(statement2 => {
              addInferredStatement(statement1.getSubject, property, statement2.getObject)
            })
            case _ => 0
          }
        }).sum
        case _ => 0
      }
    ).sum
  }

  override def addAxiomStatements(): Unit = {
    addInferredStatement(OWL.EQUIVALENTCLASS, RDF.TYPE, RDF.PROPERTY)
    addInferredStatement(OWL.EQUIVALENTCLASS, RDF.TYPE, OWL.SYMMETRICPROPERTY)
    addInferredStatement(OWL.EQUIVALENTCLASS, RDF.TYPE, OWL.TRANSITIVEPROPERTY)
    addInferredStatement(OWL.EQUIVALENTCLASS, RDFS.SUBPROPERTYOF, RDFS.SUBCLASSOF)
    addInferredStatement(OWL.EQUIVALENTCLASS, RDFS.DOMAIN, RDFS.CLASS)
    addInferredStatement(OWL.EQUIVALENTCLASS, RDFS.RANGE, RDFS.CLASS)

    addInferredStatement(OWL.EQUIVALENTPROPERTY, RDF.TYPE, RDF.PROPERTY)
    addInferredStatement(OWL.EQUIVALENTPROPERTY, RDF.TYPE, OWL.SYMMETRICPROPERTY)
    addInferredStatement(OWL.EQUIVALENTPROPERTY, RDF.TYPE, OWL.TRANSITIVEPROPERTY)
    addInferredStatement(OWL.EQUIVALENTPROPERTY, RDFS.SUBPROPERTYOF, RDFS.SUBPROPERTYOF)
    addInferredStatement(OWL.EQUIVALENTPROPERTY, RDFS.DOMAIN, RDF.PROPERTY)
    addInferredStatement(OWL.EQUIVALENTPROPERTY, RDFS.RANGE, RDF.PROPERTY)

    addInferredStatement(OWL.FUNCTIONALPROPERTY, RDF.TYPE, RDFS.CLASS)
    addInferredStatement(OWL.FUNCTIONALPROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY)

    addInferredStatement(OWL.INVERSEFUNCTIONALPROPERTY, RDF.TYPE, RDFS.CLASS)
    addInferredStatement(OWL.INVERSEFUNCTIONALPROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY)

    addInferredStatement(OWL.INVERSEOF, RDF.TYPE, RDF.PROPERTY)
    addInferredStatement(OWL.INVERSEOF, RDF.TYPE, OWL.SYMMETRICPROPERTY)
    addInferredStatement(OWL.INVERSEOF, RDFS.DOMAIN, RDF.PROPERTY)
    addInferredStatement(OWL.INVERSEOF, RDFS.RANGE, RDF.PROPERTY)

    addInferredStatement(OWL.SAMEAS, RDF.TYPE, RDF.PROPERTY)
    addInferredStatement(OWL.SAMEAS, RDF.TYPE, OWL.SYMMETRICPROPERTY)
    addInferredStatement(OWL.SAMEAS, RDF.TYPE, OWL.TRANSITIVEPROPERTY)
    addInferredStatement(OWL.SAMEAS, RDFS.DOMAIN, RDFS.RESOURCE)
    addInferredStatement(OWL.SAMEAS, RDFS.RANGE, RDFS.RESOURCE)

    addInferredStatement(OWL.SYMMETRICPROPERTY, RDF.TYPE, RDFS.CLASS)
    addInferredStatement(OWL.SYMMETRICPROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY)

    addInferredStatement(OWL.TRANSITIVEPROPERTY, RDF.TYPE, RDFS.CLASS)
    addInferredStatement(OWL.TRANSITIVEPROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY)
  }

  override def createModel(): Model = new TreeModel
}
