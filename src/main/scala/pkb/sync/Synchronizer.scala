package pkb.sync

import pkb.rdf.model.document.Document

/**
  * @author Thomas Pellissier Tanon
  */
trait Synchronizer {

  /**
    * @return documents modified since the last synchronization
    */
  def synchronize(): Traversable[Document]
}
