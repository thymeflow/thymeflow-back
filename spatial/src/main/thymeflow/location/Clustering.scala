package thymeflow.location

import java.time.{Duration, Instant}

import com.typesafe.scalalogging.StrictLogging
import thymeflow.location.cluster.{ClusterSettings, MaxLikelihoodCluster, TimeSequentialClusterEstimator}
import thymeflow.location.treillis.{MovingPosition, SamePosition}
import thymeflow.mathematics.HellingerDistance
import thymeflow.spatial.geographic.Point
import thymeflow.spatial.geographic.metric.models.{WGS84GeographyLinearMetric, WGS84SphereHaversinePointMetric}
import thymeflow.spatial.metric.Metric
import thymeflow.utilities.time.Implicits._

/**
  * @author David Montoya
  */

private case class MovementObservation[OBSERVATION <: treillis.Observation](index: Int, observation: OBSERVATION) extends treillis.Observation {

  override def point: Point = observation.point

  override def time: Instant = observation.time

  override def accuracy: Double = observation.accuracy
}

trait Clustering extends StrictLogging {

  def observationWindowCountDistribution[OBSERVATION <: treillis.Observation](observationSequence: Array[OBSERVATION]) = {
    // compute the distribution of the number of distinct observations over 10-minute periods.
    val period = Duration.ofMinutes(10)
    val uniqObservationSequence = observationSequence.distinct
    val startTime = uniqObservationSequence.head.time
    val endTime = uniqObservationSequence.last.time
    var time = startTime
    var index = 0
    val observationCountsBuilder = Vector.newBuilder[Int]
    while (time < endTime) {
      val nextTime = time.plus(period)
      // find the index of the next observation whose time is greater than or equal to nextTime
      val nextIndex = (index to uniqObservationSequence.indices.last).collectFirst {
        case i if uniqObservationSequence(i).time >= nextTime => i
      }.getOrElse(uniqObservationSequence.indices.last + 1)
      observationCountsBuilder += nextIndex - index
      index = nextIndex
      time = nextTime
    }
    val observationCounts = observationCountsBuilder.result()
    val counts = observationCounts.groupBy(identity).map {
      case (observationCount, v) => observationCount -> v.size
    }.toVector.sortBy(_._1)
    logger.info(s"Average Observation count over $period-periods: ${observationCounts.sum / observationCounts.size}, distribution: $counts")
  }

  def splitMovement[OBSERVATION <: treillis.Observation, CLUSTER_OBSERVATION <: treillis.ClusterObservation](movementEstimatorDuration: Duration,
                                                                                                             lambda: Double)(out: (IndexedSeq[(OBSERVATION, Option[CLUSTER_OBSERVATION])]) => Unit) = {
    val movementEstimator = new treillis.StateEstimator {
      override def distance(from: Point, to: Point): Double = {
        WGS84SphereHaversinePointMetric.distance(from, to)
      }
    }
    movementEstimator.findSingleNodes(movementEstimatorDuration, out)
  }

  def estimateMovement[OBSERVATION <: treillis.Observation, CLUSTER_OBSERVATION <: treillis.ClusterObservation]
  (movementEstimatorDuration: Duration)
  (observationsAndStays: IndexedSeq[(OBSERVATION, Option[CLUSTER_OBSERVATION])]) = {
    val movementEstimator = new treillis.StateEstimator {
      override def distance(from: Point, to: Point): Double = {
        WGS84SphereHaversinePointMetric.distance(from, to)
      }
    }

    val movementEstimationOption = movementEstimator.estimate(observationsAndStays, movementEstimatorDuration)
    movementEstimationOption.map {
      case movementEstimation =>
        movementEstimation.collect {
          case MovingPosition(_, observation, _) => observation
          case SamePosition(_, observation) => observation
        }
    }
  }

  def extractStaysFromObservations[OBSERVATION <: treillis.Observation](minimumStayDuration: Duration,
                                                                        observationEstimatorDuration: Duration,
                                                                        lambda: Double)(out: (MaxLikelihoodCluster[OBSERVATION, Instant]) => Unit,
                                                                                           state: Set[MaxLikelihoodCluster[OBSERVATION, Instant]] = Set[MaxLikelihoodCluster[OBSERVATION, Instant]]()) = {
    val estimator = getObservationEstimator[OBSERVATION](lambda, observationEstimatorDuration, metric, (state.iterator.map(_.index) ++ Iterator(-1)).max + 1)
    var i, j = 0
    val (estimatorOnObservation, estimatorOnFinish, estimatorGetState) = estimator.observationProcessor({
      case (cluster) =>
        val to = cluster.observations.last.time
        val from = cluster.observations.head.time
        if (Duration.between(from, to).abs.compareTo(minimumStayDuration) >= 0) {
          out(cluster)
          j += 1
        }
        i += 1
    }, state)
    def onObservation(observation: OBSERVATION) = {
      estimatorOnObservation(observation)
    }
    def onFinish() = {
      estimatorOnFinish()
      logger.info(s"[location-stay-extraction] - Extracted $j long-enough stays amongst $i candidate clusters {lambda=$lambda,observationEstimatorDuration=$observationEstimatorDuration}.")
    }
    def getState = {
      estimatorGetState()
    }
    (onObservation _, onFinish _, getState _)
  }

  // metric to compute geographic distances from.
  def metric = WGS84GeographyLinearMetric

  def getObservationEstimator[OBSERVATION <: treillis.Observation](lambda: Double,
                                                                   lookupDuration: Duration,
                                                                   metric: Metric[Point, Double],
                                                                   nextClusterIndex: Int) = {
    getMaxLikelihoodEstimator[OBSERVATION](lambda,
      lookupDuration,
      metric,
      _.point,
      _.accuracy,
      x => x.accuracy * x.accuracy, _ => 1,
      _.time,
      nextClusterIndex)
  }

  def getMaxLikelihoodEstimator[OBSERVATION](lambda: Double,
                                             lookupDuration: Duration,
                                             metric: Metric[Point, Double],
                                             observationCenter: (OBSERVATION) => Point,
                                             observationAccuracy: (OBSERVATION) => Double,
                                             observationVariance: (OBSERVATION) => Double,
                                             observationWeight: (OBSERVATION) => Double,
                                             _observationTime: (OBSERVATION) => Instant,
                                             nextClusterIndex: Int = 0) = {
    val _lambda = lambda
    val settings = ClusterSettings(metric = metric,
      observationMean = observationCenter,
      observationAccuracy = observationAccuracy,
      observationVariance = observationVariance,
      observationWeight = observationWeight)
    var clusterIndex = nextClusterIndex - 1
    new TimeSequentialClusterEstimator[OBSERVATION, Point, Instant, MaxLikelihoodCluster[OBSERVATION, Instant]] {
      val ordering = new Ordering[MaxLikelihoodCluster[OBSERVATION, Instant]] {
        override def compare(x: MaxLikelihoodCluster[OBSERVATION, Instant],
                             y: MaxLikelihoodCluster[OBSERVATION, Instant]): Int = {
          val c = y.t.compareTo(x.t)
          if (c == 0) {
            y.index - x.index
          } else {
            c
          }
        }
      }

      override def lambda: Double = _lambda

      override def distributionDistance: (Double, Double, Double) => Double = HellingerDistance.normal

      override def distance: (Point, Point) => Double = metric.distance

      override val asDistribution = (x: OBSERVATION) => (observationCenter(x), observationVariance(x))
      override val observationTime = _observationTime

      override def isWithinLookupBounds: (Instant, Instant) => Boolean = {
        case (time1, time2) => lookupDuration.compareTo(Duration.between(time1, time2).abs()) >= 0
      }

      override def clusterWithNewObservation(cluster: MaxLikelihoodCluster[OBSERVATION, Instant], lastTime: Instant, newObservation: OBSERVATION): MaxLikelihoodCluster[OBSERVATION, Instant] = {
        clusterIndex += 1
        MaxLikelihoodCluster(cluster, lastTime, newObservation, clusterIndex)
      }

      override def newCluster(lastTime: Instant, observation: OBSERVATION): MaxLikelihoodCluster[OBSERVATION, Instant] = {
        clusterIndex += 1
        MaxLikelihoodCluster(lastTime, observation, settings, clusterIndex)
      }
    }
  }
}
