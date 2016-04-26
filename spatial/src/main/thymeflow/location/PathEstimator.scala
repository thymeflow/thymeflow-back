package thymeflow.location

import java.time.Duration

import com.typesafe.scalalogging.StrictLogging
import thymeflow.utilities.TimeExecution

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/**
  * @author David Montoya
  */
trait PathEstimator[OBSERVATION] extends StrictLogging {
  def distance(from: OBSERVATION, to: OBSERVATION): Double

  def duration(from: OBSERVATION, to: OBSERVATION): Duration

  def withinSpeedBounds(distance: Double, duration: Duration): Boolean

  def isWithinLookupBounds(observation: OBSERVATION, reference: OBSERVATION): Boolean

  def estimate(observations: Traversable[OBSERVATION]): IndexedSeq[(OBSERVATION, IndexedSeq[OBSERVATION])] = {
    val resultBuilder = new ArrayBuffer[(OBSERVATION, IndexedSeq[OBSERVATION])]
    val reverseResultMap = new scala.collection.mutable.OpenHashMap[OBSERVATION, Vector[OBSERVATION]]
    var candidateClusters = Vector[OBSERVATION]()
    val length = observations.size * 2
    logger.info(s"[path-estimator] Estimating paths from ${observations.size} observations...")
    TimeExecution.timeProgressStep("path-estimator", length, logger, {
      case reportProgress =>
        def computeConnectedObservations(sequence: Traversable[OBSERVATION],
                                         outputResults: ((OBSERVATION, IndexedSeq[OBSERVATION])) => Unit) = {
          for (observation <- sequence) {
            val (withinBounds, _) = candidateClusters.partition(isWithinLookupBounds(_, observation))
            val closeObservations =
              if (withinBounds.nonEmpty) {
                val minimalDistanceObservation = withinBounds.map {
                  candidateObservation => (candidateObservation, distance(candidateObservation, observation), duration(candidateObservation, observation))
                }.filter(x => withinSpeedBounds(x._2, x._3))
                if (minimalDistanceObservation.nonEmpty) {
                  Vector(minimalDistanceObservation.minBy {
                    case (_, distance, duration) => (distance, duration)
                  }._1)
                } else {
                  Vector()
                }
              } else {
                Vector()
              }
            val connectedObservations = if (closeObservations.isEmpty) {
              candidateClusters.takeRight(1).filter(x => withinSpeedBounds(distance(x, observation), duration(x, observation)))
            } else {
              closeObservations
            }

            candidateClusters = withinBounds :+ observation
            outputResults((observation, connectedObservations))
            reportProgress()
          }
        }
        computeConnectedObservations(observations, resultBuilder += _)
        val result = resultBuilder.result()
        computeConnectedObservations(result.reverse.map(_._1), {
          case (observation, connectedObservations) =>
            connectedObservations.foreach {
              connectedObservation =>
                reverseResultMap += connectedObservation -> (reverseResultMap.getOrElse(connectedObservation, Vector()) :+ observation)
            }
        })

        result.map {
          case (observation, connectedObservations) =>
            (observation, connectedObservations ++ reverseResultMap.getOrElse(observation, Vector()))
        }
    })

  }

  def max(estimation: Traversable[(OBSERVATION, IndexedSeq[OBSERVATION])]): Option[IndexedSeq[OBSERVATION]] = {
    var parentMap = new scala.collection.mutable.OpenHashMap[OBSERVATION, (OBSERVATION, Double)]
    var lastObservationOption: Option[OBSERVATION] = None
    val length = estimation.size
    logger.info(s"[path-estimator] Maximizing path from $length observations...")
    TimeExecution.timeProgressStep("path-estimator", length, logger, {
      case reportProgress =>
        for ((observation, connectedObservations) <- estimation) {
          if (connectedObservations.nonEmpty) {
            val (observationParent, observationDistance, _) = connectedObservations.map {
              connectedObservation =>
                val d = parentMap.get(connectedObservation).map(_._2).getOrElse(0.0d)
                (connectedObservation, d + distance(connectedObservation, observation), duration(connectedObservation, observation).negated())
            }.maxBy(x => (x._2, x._3))
            parentMap += observation ->(observationParent, observationDistance)
          }
          lastObservationOption = Some(observation)
          reportProgress()
        }
        @tailrec
        def unfold(head: OBSERVATION, accumulated: Vector[OBSERVATION] = Vector()): Vector[OBSERVATION] = {
          val newAccumulated = head +: accumulated
          parentMap.get(head) match {
            case Some(parent) => unfold(parent._1, newAccumulated)
            case None => newAccumulated
          }
        }

        lastObservationOption.map {
          lastObservation =>
            unfold(lastObservation)
        }
    })
  }
}
