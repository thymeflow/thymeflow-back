package com.thymeflow.rdf.model

import org.eclipse.rdf4j.model._

import scala.collection.mutable


/**
  * @author David Montoya
  */
class StatementHashSet private(val valueFactory: ValueFactory)
  extends scala.collection.mutable.HashSet[Statement]
    with StatementSet
    with mutable.SetLike[Statement, StatementHashSet] {

  override def empty: StatementHashSet = StatementHashSet.empty(valueFactory)
}

object StatementHashSet extends StatementSetFactory[StatementHashSet] {
  def empty(implicit valueFactory: ValueFactory) = new StatementHashSet(valueFactory = valueFactory)
}
