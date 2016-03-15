package thymeflow.textsearch

import scala.concurrent.Future

/**
 * @author  David Montoya
 */
trait TextSearch[T] {

  def search(query: String, matchPercent: Int = 100): Future[Seq[(T,Float)]]

}
