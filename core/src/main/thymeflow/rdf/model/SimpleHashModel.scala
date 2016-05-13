package thymeflow.rdf.model

import java.util
import java.util.Optional

import org.openrdf.model._
import org.openrdf.model.impl.SimpleValueFactory

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
class SimpleHashModel(valueFactory: ValueFactory = SimpleValueFactory.getInstance(), statements: util.Set[Statement] = SimpleHashModel.emptySet)
  extends util.HashSet[Statement](statements) with Model {

  override def add(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Boolean = {
    toStatements(subj, pred, obj, contexts).map(add).reduce(_ || _)
  }

  override def getNamespaces: util.Set[Namespace] = ???

  override def unmodifiable(): Model = ???

  override def setNamespace(namespace: Namespace): Unit = ???

  override def contains(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Boolean = {
    this.asScala.exists(statement =>
      (subj == null || statement.getSubject == subj) &&
        (pred == null || statement.getPredicate == pred) &&
        (obj == null || statement.getObject == obj) &&
        (contexts.isEmpty || contexts.contains(statement.getContext))
    )
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

  override def clear(context: Resource*): Boolean = ???

  override def objects(): util.Set[Value] = {
    this.asScala.map(_.getObject).asJava
  }

  override def predicates(): util.Set[IRI] = {
    this.asScala.map(_.getPredicate).asJava
  }

  override def subjects(): util.Set[Resource] = {
    this.asScala.map(_.getSubject).asJava
  }

  override def filter(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Model = {
    doFilter(subj, pred, obj, contexts)
  }

  private def doFilter(subj: Resource, pred: IRI, obj: Value, contexts: Seq[Resource]): SimpleHashModel = {
    new SimpleHashModel(valueFactory, this.asScala.filter(statement =>
      (subj == null || statement.getSubject == subj) &&
        (pred == null || statement.getPredicate == pred) &&
        (obj == null || statement.getObject == obj) &&
        (contexts.isEmpty || contexts.contains(statement.getContext))
    ).asJava)
  }

  override def `match`(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): util.Iterator[Statement] = {
    doFilter(subj, pred, obj, contexts).iterator()
  }

  override def getValueFactory: ValueFactory = {
    valueFactory
  }
}

object SimpleHashModel {
  private val emptySet = new util.HashSet[Statement]()
}