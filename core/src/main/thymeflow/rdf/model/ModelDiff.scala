package thymeflow.rdf.model

import java.util

import org.openrdf.model.{Model, Statement}

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
}
