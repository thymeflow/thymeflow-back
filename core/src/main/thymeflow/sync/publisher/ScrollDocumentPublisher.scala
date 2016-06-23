package thymeflow.sync.publisher

import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import com.typesafe.scalalogging.StrictLogging
import thymeflow.utilities.ExceptionUtils

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * @author David Montoya
  */
trait ScrollDocumentPublisher[DOCUMENT, SCROLL] extends ActorPublisher[DOCUMENT] with StrictLogging {
  private var scrollQueue: Vector[SCROLL] = Vector.empty
  private var processing = false
  private var buf = Vector.empty[DOCUMENT]

  def receive = {
    case Result(scrollOption, hits) =>
      processing = false
      scrollQueue = scrollOption.map(_ +: scrollQueue).getOrElse(scrollQueue)
      queueDocuments(hits)
      if (!(buf.isEmpty && scrollQueue.isEmpty)) {
        nextResults(totalDemand)
      }
    case Failure(failure) =>
      // Necessary to log this error because of https://github.com/akka/akka/issues/18359
      logger.error(ExceptionUtils.getUnrolledStackTrace(failure))
      onError(failure)
      processing = false
    case Request(requestCount) =>
      deliverBuf()
      if (scrollQueue.nonEmpty) {
        nextResults(requestCount)
      }
    case Cancel =>
      context.stop(self)
  }

  protected def queueIsEmpty = scrollQueue.isEmpty && !processing

  protected def queue(scroll: SCROLL) = {
    scrollQueue :+= scroll
    nextResults(totalDemand)
  }

  private def nextResults(requestCount: Long): Unit = {
    try {
      if (isActive && totalDemand > 0 && !processing) {
        scrollQueue match {
          case head +: tail =>
            processing = true
            scrollQueue = tail
            val future = queryBuilder(head, requestCount)
            future.foreach {
              case result => this.self ! result
            }
            future.onFailure {
              case t => this.self ! Failure(t)
            }
          case _ =>
          // no scroll to query
        }
      }
    } catch {
      case t: Exception =>
        this.self ! Failure(t)
    }
  }

  protected def queueDocuments(documents: Traversable[DOCUMENT]) = {
    buf ++= documents
    deliverBuf()
  }

  protected def queryBuilder: (SCROLL, Long) => Future[Result]

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

