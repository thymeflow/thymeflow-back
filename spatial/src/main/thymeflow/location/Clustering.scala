package thymeflow.location

import java.time.{Duration, Instant}

import com.typesafe.scalalogging.StrictLogging
import thymeflow.location.cluster.{ClusterSettings, MaxLikelihoodCluster, TimeSequentialClusterEstimator}
import thymeflow.location.model._
import thymeflow.location.treillis.{MovingPosition, SamePosition}
import thymeflow.mathematics.HellingerDistance
import thymeflow.spatial.geographic.Point
import thymeflow.spatial.geographic.metric.models.{WGS84GeographyLinearMetric, WGS84SphereHaversinePointMetric}
import thymeflow.spatial.metric.Metric
import thymeflow.utilities.time.Implicits._

/**
  * @author David Montoya
  */


trait Clustering extends StrictLogging {

  def observationWindowCountDistribution(observationSequence: Array[Observation]) = {
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

  def flattenGroupSequence[A, B, X, I](bSequence: IndexedSeq[B], aSequenceWithB: IndexedSeq[X])
                                      (xToA: X => A, xToBs: X => IndexedSeq[B], bIndex: B => I) = {
    flattenGroupSequenceGeneric(bSequence, aSequenceWithB, xToA, xToBs, bIndex, (indexToAMap: Map[I, A]) => indexToAMap.apply)
  }

  def flattenGroupSequenceGeneric[A, B, X, I, T](bSequence: IndexedSeq[B],
                                                 aSequenceWithB: IndexedSeq[X],
                                                 xToA: X => A,
                                                 xToBs: X => IndexedSeq[B],
                                                 bIndex: B => I,
                                                 aRetrieve: (Map[I, A]) => I => T) = {
    val bIndexToAMap = aSequenceWithB.flatMap {
      case (x) =>
        xToBs(x).map {
          case b =>
            bIndex(b) -> xToA(x)
        }
    }.toMap

    val retrieve = aRetrieve(bIndexToAMap)

    val bSequenceWithA = bSequence.map {
      case b => (b, retrieve(bIndex(b)))
    }
    bSequenceWithA
  }

  def estimateMovement(movementEstimatorDuration: Duration,
                       movementObservationEstimatorDuration: Duration,
                       lambda: Double)
                      (observationsAndStays: IndexedSeq[(Observation, Option[ClusterObservation])],
                       stays: IndexedSeq[ClusterObservation]) = {
    val movementEstimator = new treillis.StateEstimator {
      override def distance(from: Point, to: Point): Double = {
        WGS84SphereHaversinePointMetric.distance(from, to)
      }
    }

    val movementEstimationOption = movementEstimator.estimate(observationsAndStays, stays, movementEstimatorDuration)

    val movementEstimationTravelerClustersAndObservationsOption = movementEstimationOption.map {
      case movementEstimation =>
        val movementEstimationTravelerObservations = movementEstimation.collect {
          case m: MovingPosition => m.observation.asInstanceOf[Observation]
          case same: SamePosition => same.observation.asInstanceOf[Observation]
        }
        val movementObservationEstimator = getObservationEstimator(lambda, movementObservationEstimatorDuration, metric)
        val movementClusters = movementObservationEstimator.estimate(movementEstimationTravelerObservations)

        (movementClusters,
          flattenGroupSequenceOption(movementEstimationTravelerObservations, movementClusters)(identity, _.observations, _.index).collect {
            case (a, Some(cluster)) => (a, cluster)
          })
    }

    val counts = movementEstimationOption.map {
      case movementEstimation => (movementEstimation.count(_.isInstanceOf[MovingPosition]), movementEstimation.count(_.isInstanceOf[SamePosition]))
    }
    logger.info(s"[movement-estimation] Found $counts states {lambda=$lambda,movementEstimatorDuration=$movementEstimatorDuration,movementObservationEstimatorDuration=$movementObservationEstimatorDuration}.")

    movementEstimationTravelerClustersAndObservationsOption
  }

  // metric to compute geographic distances from.
  def metric = WGS84GeographyLinearMetric

  def getObservationEstimator(lambda: Double, lookupDuration: Duration, metric: Metric[Point, Double]) = {
    getMaxLikelihoodEstimator[Observation](lambda,
      lookupDuration,
      metric,
      _.point,
      _.accuracy,
      x => x.accuracy * x.accuracy, _ => 1,
      _.time, _.index)
  }

  def getMaxLikelihoodEstimator[OBSERVATION](lambda: Double,
                                             lookupDuration: Duration,
                                             metric: Metric[Point, Double],
                                             observationCenter: (OBSERVATION) => Point,
                                             observationAccuracy: (OBSERVATION) => Double,
                                             observationVariance: (OBSERVATION) => Double,
                                             observationWeight: (OBSERVATION) => Double,
                                             _observationTime: (OBSERVATION) => Instant,
                                             observationIndex: (OBSERVATION) => Int) = {
    val _lambda = lambda
    val settings = ClusterSettings(metric = metric,
      observationMean = observationCenter,
      observationAccuracy = observationAccuracy,
      observationVariance = observationVariance,
      observationWeight = observationWeight)
    new TimeSequentialClusterEstimator[OBSERVATION, Point, Instant, MaxLikelihoodCluster[OBSERVATION, Instant]] {
      implicit val ordering = new Ordering[MaxLikelihoodCluster[OBSERVATION, Instant]] {
        override def compare(x: MaxLikelihoodCluster[OBSERVATION, Instant], y: MaxLikelihoodCluster[OBSERVATION, Instant]): Int = {
          val c = y.t.compareTo(x.t)
          if (c == 0) {
            val r = observationIndex(y.observations.last) - observationIndex(x.observations.last)
            r
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
        MaxLikelihoodCluster(cluster, lastTime, newObservation)
      }

      override def newCluster(lastTime: Instant, observation: OBSERVATION): MaxLikelihoodCluster[OBSERVATION, Instant] = {
        MaxLikelihoodCluster(lastTime, observation, settings)
      }
    }
  }

  def flattenGroupSequenceOption[A, B, X, I](bSequence: IndexedSeq[B],
                                             aSequenceWithB: IndexedSeq[X])
                                            (xToA: X => A, xToBs: X => IndexedSeq[B], bIndex: B => I) = {
    flattenGroupSequenceGeneric(bSequence, aSequenceWithB, xToA, xToBs, bIndex, (indexToAMap: Map[I, A]) => indexToAMap.get)
  }

  def extractStaysFromObservations(minimumStayDuration: Duration,
                                   observationEstimatorDuration: Duration,
                                   lambda: Double)(observationSequence: IndexedSeq[Observation]) = {

    val estimator = getObservationEstimator(lambda, observationEstimatorDuration, metric)
    val candidateStays = estimator.estimate(observationSequence)
    logger.info(s"[stay-extraction] Found ${candidateStays.size} candidate stays {lambda=$lambda,observationEstimatorDuration=$observationEstimatorDuration}.")

    val longEnoughStays = candidateStays.filter {
      case cluster =>
        val to = cluster.observations.last.time
        val from = cluster.observations.head.time
        Duration.between(from, to).abs.compareTo(minimumStayDuration) >= 0
    }
    logger.info(s"[stay-extraction] Found ${longEnoughStays.size} long enough stays {lambda=$lambda,minimumStayDuration=$minimumStayDuration}.")

    val staysAndObservations = longEnoughStays.zipWithIndex.map {
      case (cluster, index) =>
        val to = cluster.observations.last.time
        val from = cluster.observations.head.time
        val clusterObservation = ClusterObservation(index = index, point = cluster.mean, from = from, to = to, accuracy = math.sqrt(cluster.variance))
        (clusterObservation, cluster.observations)
    }

    val observationsAndStays = flattenGroupSequenceOption(observationSequence, staysAndObservations)(_._1, _._2, _.index)
    (observationsAndStays, staysAndObservations.map(_._1))

  }
}
