package thymeflow.rdf.model

import org.openrdf.model.Model

/**
  * @author Thomas Pellissier Tanon
  */
class ModelDiff(val added: Model, val removed: Model) {
  def apply(diff: ModelDiff): Unit = {
    added.removeAll(diff.removed)
    added.addAll(diff.added)

    removed.removeAll(diff.added)
    removed.addAll(diff.removed)
  }
}
