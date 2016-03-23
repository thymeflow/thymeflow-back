package thymeflow.text.search

import scala.concurrent.Future

/**
  * @author  David Montoya
  */

trait PartialTextMatcher[ENTITY] {
  /**
    * Lookup for entities partially matching the query.
    * @param query the query content.
    * @param searchDepth the maximum search depth.
    * @param clearDuplicateNestedResults
    * @return
    */
  def partialMatchQuery(query: Seq[String], searchDepth: Int, clearDuplicateNestedResults: Boolean): Future[(Traversable[(ContentPosition, Traversable[(ENTITY, String, Float)])])]
}
