package pkb.inferencer

import pkb.rdf.model.ModelDiff

/**
  * @author Thomas Pellissier Tanon
  */
trait Inferencer {

  /**
    * @return adds inference to the repository the Inferencer is linked to based on the diff
    */
  def infer(diff: ModelDiff): Unit
}
