package thymeflow.text.search.elasticsearch

import org.elasticsearch.action.{ActionListener, ListenableActionFuture}

import scala.concurrent.Promise
import scala.language.implicitConversions
import scala.util.{Failure, Success}

/**
 * @author  David Montoya
 */
object ListenableActionFutureExtensions {

  implicit def listenableActionFutureAsScalaFuture[T](listenableActionFuture: ListenableActionFuture[T]): ListenableActionFutureAsScalaFuture[T]
  = new ListenableActionFutureAsScalaFuture(listenableActionFuture)

  class ListenableActionFutureAsScalaFuture[T](listenableActionFuture: ListenableActionFuture[T]){
    def future = {
      val promise = Promise[T]()
      listenableActionFuture.addListener(new ActionListener[T]{
        override def onFailure(e: Throwable): Unit = {
          promise.complete(Failure(e))
        }

        override def onResponse(response: T): Unit = {
          promise.complete(Success(response))
        }
      })
      promise.future
    }
  }
}
