package thymeflow.enricher

import akka.actor.{Actor, Props}
import thymeflow.actors._
import thymeflow.rdf.model.ModelDiff

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author David Montoya
  */
trait DelayedEnricher extends Enricher {
  private val tickActor = thymeflow.actors.system.actorOf(Props(new Actor {
    var previousDiffTimeOption: Option[Long] = None
    var previousEnrichmentOption: Option[Long] = None

    def receive = {
      case Diff =>
        previousDiffTimeOption = Some(System.nanoTime())
      case Tick =>
        previousDiffTimeOption match {
          case Some(previousDiffTime) =>
            if (Duration.fromNanos(System.nanoTime() - previousDiffTime) >= delay) {
              val runEnrichment = previousEnrichmentOption match {
                case Some(previousEnrichment) if previousDiffTime != previousEnrichment => true
                case None => true
                case _ => false
              }
              if (runEnrichment) {
                previousEnrichmentOption = Some(previousDiffTime)
                runEnrichments()
              }
            }
          case None =>
        }
    }
  }))

  /**
    * @return duration to wait for before running the Enricher
    */
  def delay: Duration

  /**
    * Run the enrichments defined by this Enricher
    */
  def runEnrichments(): Unit

  override def enrich(diff: ModelDiff): Unit = {
    tickActor ! Diff
  }

  thymeflow.actors.system.scheduler.schedule(1 second, 10 seconds, tickActor, Tick)

  private object Tick
  private object Diff
}
