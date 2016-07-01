package thymeflow.rdf.model

import java.util

import org.openrdf.model._

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
class ModelDiff(val added: Model, val removed: Model) {
  def apply(diff: ModelDiff): Unit = {
    add(diff.added)
    remove(diff.removed)
  }

  def add(statement: Statement) {
    removed.remove(statement)
    added.add(statement)
  }

  def add(statements: util.Collection[Statement]) {
    removed.removeAll(statements)
    added.addAll(statements)
  }

  def remove(statement: Statement) {
    added.remove(statement)
    removed.add(statement)
  }

  def remove(statements: util.Collection[Statement]) {
    added.removeAll(statements)
    removed.addAll(statements)
  }

  def contexts(): util.Set[Resource] = {
    val contexts = added.contexts()
    contexts.addAll(removed.contexts())
    contexts
  }

  def filter(subj: Resource, pred: IRI, obj: Value, contexts: Resource = null): ModelDiff = {
    new ModelDiff(
      added.filter(subj, pred, obj, contexts),
      removed.filter(subj, pred, obj, contexts)
    )
  }

  def isEmpty: Boolean = {
    added.isEmpty && removed.isEmpty
  }

  override def toString: String = {
    (this.added.asScala.map("+ " + _.toString) ++ this.removed.asScala.map("- " + _.toString)).mkString("\n")
  }
}

object ModelDiff {
  def merge(diffs: ModelDiff*): ModelDiff = {
    val diff = new ModelDiff(new SimpleHashModel(), new SimpleHashModel())
    diffs.foreach(diff.apply)
    diff
  }
}
