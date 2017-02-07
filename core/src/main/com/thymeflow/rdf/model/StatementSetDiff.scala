package com.thymeflow.rdf.model

import org.eclipse.rdf4j.model._

/**
  * @author Thomas Pellissier Tanon
  */
class StatementSetDiff(val added: StatementSet, val removed: StatementSet) {
  def apply(diff: StatementSetDiff): Unit = {
    add(diff.added)
    remove(diff.removed)
  }

  def add(statement: Statement) {
    removed.remove(statement)
    added.add(statement)
  }

  def add(statements: Traversable[Statement]) {
    removed --= statements
    added ++= statements
  }

  def remove(statement: Statement) {
    added.remove(statement)
    removed.add(statement)
  }

  def remove(statements: Traversable[Statement]) {
    added --= statements
    removed ++= statements
  }

  def contexts(): Set[Resource] = {
    val contexts = added.contexts
    contexts ++ removed.contexts
  }

  def filter(f: (Statement) => Boolean): StatementSetDiff = {
    implicit val valueFactory = added.valueFactory
    new StatementSetDiff(
      added.filter(f),
      removed.filter(f)
    )
  }

  def isEmpty: Boolean = {
    added.isEmpty && removed.isEmpty
  }

  override def toString: String = {
    (this.added.view.map("+ " + _.toString) ++ this.removed.view.map("- " + _.toString)).mkString("\n")
  }
}

object StatementSetDiff {
  def merge(diffs: StatementSetDiff*)(implicit valueFactory: ValueFactory): StatementSetDiff = {
    val diff = new StatementSetDiff(StatementSet.empty, StatementSet.empty)
    diffs.foreach(diff.apply)
    diff
  }
}
