package thymeflow.rdf.model.vocabulary

import org.openrdf.model.IRI
import org.openrdf.model.impl.SimpleValueFactory
import org.openrdf.model.vocabulary.{GEO, RDF}

/**
  * @author Thomas Pellissier Tanon
  */
object Negation {

  private val notPersonalNamespace = "http://thymeflow.com/schema-not#"
  private val notSchemaOrgNamespace = "http://thymeflow.com/personal-not#"
  private val notRdfNamespace = "http://thymeflow.com/rdf-not#"
  private val notGeoNamespace = "http://thymeflow.com/geosparql-not#"
  private val valueFactory = SimpleValueFactory.getInstance()

  def not(property: IRI): IRI = {
    property match {
      case Personal.SAME_AS => Personal.DIFFERENT_FROM
      case Personal.DIFFERENT_FROM => Personal.SAME_AS
      case _ if property.getNamespace == SchemaOrg.NAMESPACE =>
        valueFactory.createIRI(notSchemaOrgNamespace, property.getLocalName)
      case _ if property.getNamespace == Personal.NAMESPACE =>
        valueFactory.createIRI(notPersonalNamespace, property.getLocalName)
      case _ if property.getNamespace == RDF.NAMESPACE =>
        valueFactory.createIRI(notRdfNamespace, property.getLocalName)
      case _ if property.getNamespace == GEO.NAMESPACE =>
        valueFactory.createIRI(notGeoNamespace, property.getLocalName)
      case _ => throw new IllegalArgumentException(s"No negation available for property $property")
    }
  }
}
