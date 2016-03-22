package pkb.sync.publisher

import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * @author David Montoya
  */
trait ScrollDocumentPublisher[DOCUMENT, SCROLL] extends ActorPublisher[DOCUMENT] with StrictLogging {
  protected var currentScrollOption: Option[SCROLL] = None
  protected var noMoreResults = false
  protected var processing = false
  protected var buf = Vector.empty[DOCUMENT]

  def receive = {
    case Result(scrollOption, hits) =>
      processing = false
      if (scrollOption.isEmpty) {
        noMoreResults = true
      } else {
        currentScrollOption = scrollOption
      }
      if (buf.isEmpty && isActive && totalDemand > 0)
        hits foreach onNext
      else {
        buf ++= hits
        deliverBuf()
      }
      if (!(buf.isEmpty && noMoreResults)) {
        if (isActive && totalDemand > 0) {
          nextResults()
        }
      }
    case Failure(failure) =>
      onError(failure)
      processing = false
    case Request(cnt) =>
      deliverBuf()
      if (isActive) {
        if (!noMoreResults && totalDemand > 0) {
          nextResults()
        }
      }
    case Cancel =>
      context.stop(self)
    case _ =>
  }

  protected def queryBuilder: (SCROLL) => Future[Result]

  protected def nextResults(): Unit = {
    try {
      if (!processing) {
        currentScrollOption match {
          case Some(currentScroll) =>
            processing = true
            val future = queryBuilder(currentScroll)
            future.foreach {
              case result => this.self ! result
            }
            future.onFailure {
              case t => this.self ! Failure(t)
            }
          case None =>
          // No results to query
        }
      }
    } catch {
      case t: Exception =>
        this.self ! Failure(t)
    }
  }

  @tailrec final protected def deliverBuf(): Unit =
    if (isActive && totalDemand > 0) {
      /*
       * totalDemand is a Long and could be larger than
       * what buf.splitAt can accept
       */
      if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        buf = keep
        use foreach onNext
      } else {
        val (use, keep) = buf.splitAt(Int.MaxValue)
        buf = keep
        use foreach onNext
        deliverBuf()
      }
    }

  protected case class Result(scroll: Option[SCROLL], hits: Traversable[DOCUMENT])

  protected case class Failure(throwable: Throwable)

}

