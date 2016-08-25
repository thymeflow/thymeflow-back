package com.thymeflow.enricher

import com.thymeflow.rdf.model.ModelDiff
import org.openrdf.repository.RepositoryConnection

/**
  * @author Thomas Pellissier Tanon
  */
trait Enricher {

  protected val newRepositoryConnection: () => RepositoryConnection

  protected val repositoryConnection = newRepositoryConnection()

  /**
    * @return enrichs the repository the Enricher is linked to based on the diff
    */
  def enrich(diff: ModelDiff): Unit
}
