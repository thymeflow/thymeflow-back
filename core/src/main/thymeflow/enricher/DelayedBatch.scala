package thymeflow.enricher

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/**
  * @author Thomas Pellissier Tanon
  */
case class DelayedBatch[In, Out](seed: In => Out, aggregate: (Out, In) => Out, delay: FiniteDuration)
                                (implicit system: ActorSystem, executionContext: ExecutionContext)
  extends GraphStage[FlowShape[In, Out]] with StrictLogging {

  val in = Inlet[In]("DelayedBatch.in")
  val out = Outlet[Out]("DelayedBatch.out")
  override val shape: FlowShape[In, Out] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    lazy val decider = inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

    private var agg: Option[Out] = None
    private var lastPullTime: Long = 0
    private var tickScheduler: Option[Cancellable] = None

    override def preStart() = {
      if (tickScheduler.isEmpty) {
        tickScheduler = Some(system.scheduler.schedule(delay, delay, getStageActor {
          case (actorRef, DelayedBatch.Tick) => flush()
        }.ref, DelayedBatch.Tick))
      }

      pull(in)
    }

    override def postStop() = {
      tickScheduler.foreach(_.cancel())
    }

    setHandler(in, new InHandler {

      override def onPush(): Unit = {
        val elem = grab(in)
        lastPullTime = System.nanoTime()

        agg match {
          case Some(aggValue) =>
            try {
              agg = Some(aggregate(aggValue, elem))
            } catch {
              case NonFatal(ex) => decider(ex) match {
                case Supervision.Stop => failStage(ex)
                case Supervision.Restart => restartState()
                case Supervision.Resume =>
              }
            }
          case None =>
            try {
              agg = Some(seed(elem))
            } catch {
              case NonFatal(ex) => decider(ex) match {
                case Supervision.Stop => failStage(ex)
                case Supervision.Restart => restartState()
                case Supervision.Resume =>
              }
            }
        }

        if (isAvailable(out)) {
          flush()
        }
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        flush()
      }
    })

    setHandler(out, new OutHandler {

      override def onPull(): Unit = {
        flush()
        if (!isClosed(in) && !hasBeenPulled(in)) {
          pull(in)
        }
      }
    })

    private def flush(): Unit = {
      if (isAvailable(out)) {
        agg.foreach(aggValue => {
          //Only executed if there is something to push so if a pull have been done and lastPullTime set to a meaningful value
          if (System.nanoTime() - lastPullTime >= delay.toNanos) {
            push(out, aggValue)
            agg = None
          }
        })
      }

      //We see if we should close the stage
      if (isClosed(in) && agg.isEmpty) {
        completeStage()
      }
    }

    private def restartState(): Unit = {
      agg = None
    }
  }
}

object DelayedBatch {
  def apply[T](aggregate: (T, T) => T, delay: FiniteDuration)
              (implicit system: ActorSystem, executionContext: ExecutionContext): DelayedBatch[T, T] =
    DelayedBatch[T, T](identity, aggregate, delay)(system, executionContext)

  private object Tick

}
