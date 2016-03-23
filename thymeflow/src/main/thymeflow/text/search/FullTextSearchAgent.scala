package thymeflow.text.search

import scala.concurrent.Future

/**
  * @author  David Montoya
  */
trait FullTextSearchAgent[T] extends FullTextSearch[T] {
  /**
    * Closes the TextSearchAgent, freeing up resources.
    * @return
    */
  def close(): Future[Unit]
}
