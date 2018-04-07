package com.thymeflow.rdf.model

import org.eclipse.rdf4j.model._

import scala.collection.mutable

/**
  * @author David Montoya
  */
trait StatementSet extends mutable.Set[Statement]
  with mutable.SetLike[Statement, StatementSet] {
  def valueFactory: ValueFactory

  def contexts: Set[Resource] = this.map(_.getContext)(scala.collection.breakOut)

  def objects: Set[Value] = this.map(_.getObject)(scala.collection.breakOut)

  def predicates: Set[IRI] = this.map(_.getPredicate)(scala.collection.breakOut)

  def subjects: Set[Resource] = this.map(_.getSubject)(scala.collection.breakOut)

  def add(subj: Resource, pred: IRI, obj: Value, contexts: Resource*): Boolean = {
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

  override def empty: StatementSet = StatementSet.empty(valueFactory)

  override def toString: String = {
    this.mkString("\n")
  }
}


object StatementSet extends StatementSetFactory[StatementSet] {
  override def empty(implicit valueFactory: ValueFactory): StatementSet = StatementHashSet.empty
}