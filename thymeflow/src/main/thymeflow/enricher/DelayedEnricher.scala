package thymeflow.enricher

import akka.actor.{Actor, Props}
import pkb.actors._
import pkb.inferencer.Inferencer
import pkb.rdf.model.ModelDiff

import scala.concurrent.duration._
import scala.languageFeature.postfixOps

/**
  * @author David Montoya
  */
trait DelayedEnricher extends Inferencer {
  val tickActor = pkb.actors.system.actorOf(Props(new Actor {
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
                run()
              }
            }
          case None =>
        }
    }
  }))

  def delay: Duration

  def run(): Unit

  override def infer(diff: ModelDiff): Unit = {
    tickActor ! Diff
  }
  pkb.actors.system.scheduler.schedule(1 second, 10 seconds, tickActor, Tick)

  object Tick

  object Diff
}
