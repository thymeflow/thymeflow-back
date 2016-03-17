package thymeflow.text.search

import scala.concurrent.Future

/**
 * @author  David Montoya
 */
trait TextSearchAgent[T] extends TextSearch[T] {

  def close(): Future[Unit]
}
