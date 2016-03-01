package pkb.rdf.model.document

import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.{IRI, Model}

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
class Document(val iri: IRI, val triples: Model) {
  val model = {
    val editableModel = new LinkedHashModel()
    triples.iterator.asScala.foreach(triple =>
      editableModel.add(triple.getSubject, triple.getPredicate, triple.getObject, iri)
    )
    editableModel.unmodifiable()
  }
}
