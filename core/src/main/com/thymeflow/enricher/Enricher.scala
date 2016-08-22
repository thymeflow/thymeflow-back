package com.thymeflow.enricher

import com.thymeflow.rdf.model.ModelDiff

/**
  * @author Thomas Pellissier Tanon
  */
trait Enricher {

  /**
    * @return enrichs the repository the Enricher is linked to based on the diff
    */
  def enrich(diff: ModelDiff): Unit
}
