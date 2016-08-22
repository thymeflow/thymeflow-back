package com.thymeflow.text.search

import scala.concurrent.Future

/**
  * @author  David Montoya
  */

trait PartialTextMatcher[ENTITY] {
  /**
    * Lookup for entities partially matching the query.
    *
    * @param query the query content.
    * @param searchDepth the maximum search depth.
    * @param clearDuplicateNestedResults return only the best match if some matches are nested
    * @return
    */
  def partialMatchQuery(query: IndexedSeq[String], searchDepth: Int, clearDuplicateNestedResults: Boolean): Future[(Traversable[(ContentPosition, Traversable[(ENTITY, String, Float)])])]
}
