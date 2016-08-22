package com.thymeflow.text.search

import scala.concurrent.Future

/**
  * @author  David Montoya
  */
trait FullTextSearch[ENTITY] {

  /**
    * Lookup for entities matching some text query.
    *
    * @param query the lookup text
    * @param matchPercent the match percent (between 0 and 100)
    * @return a sequence of matching entities, their matching text, andthe match score
    */
  def matchQuery(query: String, matchPercent: Int = 100): Future[Seq[(ENTITY, String, Float)]]

}
