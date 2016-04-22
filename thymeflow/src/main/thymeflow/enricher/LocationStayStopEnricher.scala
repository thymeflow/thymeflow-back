package thymeflow.enricher

import java.time.{Duration, Instant}

import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.repository.RepositoryConnection
import pkb.actors._
import thymeflow.location.Clustering
import thymeflow.location.model.Observation
import thymeflow.spatial.geographic.{Geography, Point}

import scala.concurrent.Future

/**
  * @author David Montoya
  */
abstract class LocationStayStopEnricher(repositoryConnection: RepositoryConnection, val delay: scala.concurrent.duration.Duration)
  extends DelayedEnricher with StrictLogging {

  def deleteStays(): Future[Unit]

  def getLocations[Unit](): Source[(Instant, Point, Double), Unit]

  def saveStays(stays: Traversable[(Instant, Instant, Point, Double)]): Future[Unit]

  override def runEnrichments(): Unit = {
    implicit val format = org.json4s.DefaultFormats
    val clustering = new Clustering {}
    val minimumStayDuration = Duration.ofMinutes(15)
    val observationEstimatorDuration = Duration.ofMinutes(60)
    val movementEstimatorDuration = Duration.ofMinutes(120)
    val movementObservationEstimatorDuration = Duration.ofMinutes(0)
    val lambda = 0.95
    deleteStays().flatMap {
      _ =>
        getLocations().runFold((0, Vector[Observation]())) {
          case ((index, observations), (time, point, accuracy)) =>
            val observation = Observation(index = index, time = time, accuracy = accuracy, point = point)
            (index + 1, observations :+ observation)
        }.flatMap {
          case (_, observations) =>
            Future {
              val (observationAndStays, stays) = clustering.extractStaysFromObservations(minimumStayDuration, observationEstimatorDuration, lambda)(observations)
              val locationStays = clustering.estimateMovement(movementEstimatorDuration, movementObservationEstimatorDuration, lambda)(observationAndStays, stays).map {
                case (movementStays, _) =>
                  movementStays.map {
                    case pointCluster =>
                      (
                        pointCluster.observations.minBy(_.time).time,
                        pointCluster.observations.maxBy(_.time).time,
                        Geography.point(pointCluster.mean.longitude, pointCluster.mean.latitude),
                        math.sqrt(pointCluster.variance)
                        )
                  }
              }.getOrElse(Vector())
              locationStays
            }.flatMap {
              case locationStays =>
                Source.fromIterator(() => locationStays.iterator).grouped(500).mapAsync(1) {
                  g => saveStays(g)
                }.runForeach(_ => ())
            }
        }
    }
  }
}
