package pkb.rdf.model.document

import org.openrdf.model.{IRI, Model}
import pkb.rdf.model.SimpleHashModel

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
class Document(val iri: IRI, val triples: Model) {
  val model = {
    val editableModel = new SimpleHashModel()
    triples.iterator.asScala.foreach(triple =>
      editableModel.add(triple.getSubject, triple.getPredicate, triple.getObject, iri)
    )
    editableModel //TODO: make the model readonly
  }
}
