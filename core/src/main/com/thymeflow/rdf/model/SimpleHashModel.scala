package com.thymeflow.rdf.model

import java.util
import java.util.{Collections, Optional}

import org.openrdf.model._
import org.openrdf.model.impl.SimpleValueFactory

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
class SimpleHashModel(valueFactory: ValueFactory = SimpleValueFactory.getInstance(), statements: util.Collection[Statement] = Collections.emptySet())
  extends util.HashSet[Statement](statements) with Model {

  override def add(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Boolean = {
    toStatements(subj, pred, obj, contexts).map(add).reduce(_ || _)
  }

  private def toStatements(subj: Resource, pred: IRI, obj: Value, contexts: Seq[Resource]): Traversable[Statement] = {
    if (contexts.isEmpty) {
      Some(valueFactory.createStatement(subj, pred, obj))
    } else {
      contexts.map(context =>
        valueFactory.createStatement(subj, pred, obj, context)
      )
    }
  }

  override def getNamespaces: util.Set[Namespace] = {
    Collections.emptySet()
  }

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

  private def doFilter(subj: Resource, pred: IRI, obj: Value, contexts: Seq[Resource]): Model = {
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

  override def toString: String = {
    this.asScala.mkString("\n")
  }
}

object SimpleHashModel {
  val empty = new util.HashSet[Statement] with Model {
    override def filter(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Model = this
    override def subjects(): util.Set[Resource] = Collections.emptySet()
    override def predicates(): util.Set[IRI] = Collections.emptySet()
    override def objects(): util.Set[Value] = Collections.emptySet()
    override def clear(context: Resource*): Boolean = throw new UnsupportedOperationException
    override def removeNamespace(prefix: String): Optional[Namespace] = throw new UnsupportedOperationException
    override def remove(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Boolean =
      throw new UnsupportedOperationException
    override def contains(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Boolean =
      throw new UnsupportedOperationException
    override def setNamespace(namespace: Namespace): Unit = throw new UnsupportedOperationException
    override def unmodifiable(): Model = this
    override def getNamespaces: util.Set[Namespace] = Collections.emptySet()
    override def add(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Boolean =
      throw new UnsupportedOperationException
    override def `match`(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): util.Iterator[Statement] =
      Collections.emptyIterator()
    override def getValueFactory: ValueFactory = SimpleValueFactory.getInstance()
  }
}