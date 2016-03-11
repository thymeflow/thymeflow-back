package pkb.rdf.model

import java.util
import java.util.Optional

import org.openrdf.model._
import org.openrdf.model.impl.SimpleValueFactory

/**
  * @author Thomas Pellissier Tanon
  */
class SimpleHashModel(valueFactory: ValueFactory = SimpleValueFactory.getInstance())
  extends util.HashSet[Statement] with Model {

  override def add(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Boolean = {
    toStatements(subj, pred, obj, contexts).map(add).reduce(_ || _)
  }

  override def getNamespaces: util.Set[Namespace] = {
    throw new UnsupportedOperationException
  }

  override def unmodifiable(): Model = {
    throw new UnsupportedOperationException
  }

  override def setNamespace(namespace: Namespace): Unit = {
    throw new UnsupportedOperationException
  }

  override def contains(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Boolean = {
    throw new UnsupportedOperationException
  }

  override def remove(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Boolean = {
    toStatements(subj, pred, obj, contexts).map(remove).reduce(_ || _)
  }

  private def toStatements(subj: Resource, pred: IRI, obj: Value, contexts: Seq[Resource]): Traversable[Statement] = {
    if (contexts.isEmpty) {
      List(valueFactory.createStatement(subj, pred, obj))
    } else {
      contexts.map(context =>
        valueFactory.createStatement(subj, pred, obj, context)
      )
    }
  }

  override def removeNamespace(prefix: String): Optional[Namespace] = {
    throw new UnsupportedOperationException
  }

  override def clear(context: Resource*): Boolean = {
    throw new UnsupportedOperationException
  }

  override def objects(): util.Set[Value] = {
    throw new UnsupportedOperationException
  }

  override def predicates(): util.Set[IRI] = {
    throw new UnsupportedOperationException
  }

  override def subjects(): util.Set[Resource] = {
    throw new UnsupportedOperationException
  }

  override def filter(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Model = {
    throw new UnsupportedOperationException
  }

  override def `match`(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): util.Iterator[Statement] = {
    throw new UnsupportedOperationException
  }

  override def getValueFactory: ValueFactory = {
    valueFactory
  }
}
