package thymeflow.location.cluster

import com.typesafe.scalalogging.StrictLogging
import thymeflow.utilities.TimeExecution

import scala.collection.SortedSet
import scala.collection.mutable.ArrayBuffer

/**
  * @author David Montoya
  */
trait TimeSequentialClusterEstimator[OBSERVATION, SPACE, TIME, CLUSTER <: Cluster[OBSERVATION, SPACE, TIME]] extends StrictLogging {
  implicit val ordering: Ordering[CLUSTER]

  def lambda: Double

  def observationTime: OBSERVATION => TIME

  def asDistribution: OBSERVATION => (SPACE, Double)

  def clusterWithNewObservation(cluster: CLUSTER, lastTime: TIME, newObservation: OBSERVATION): CLUSTER

  def newCluster(lastTime: TIME, observation: OBSERVATION): CLUSTER

  def distributionDistance: (Double, Double, Double) => Double

  def distance: (SPACE, SPACE) => Double

  def isWithinLookupBounds: (TIME, TIME) => Boolean

  def estimate(observations: Traversable[OBSERVATION]): IndexedSeq[CLUSTER] = {
    val resultClustersBuilder = new ArrayBuffer[CLUSTER]
    var candidateClusters: SortedSet[CLUSTER] = SortedSet()
    val length = observations.size
    val processName = "sequential-cluster-estimator"
    logger.debug(s"[$processName] Estimating clusters from $length observations {lambda=$lambda}...")
    TimeExecution.timeProgressStep(processName, length, logger, {
      case reportProgress =>
        var i = 0
        for (observation <- observations) {
          val time = observationTime(observation)
          var clustersWithinLookupBounds = new scala.collection.mutable.TreeSet[CLUSTER]
          var clustersOutsideLookupBounds = new ArrayBuffer[CLUSTER]()

          candidateClusters.foreach {
            candidateCluster =>
              if (isWithinLookupBounds(candidateCluster.t, time)) {
                clustersWithinLookupBounds += candidateCluster
              } else {
                clustersOutsideLookupBounds += candidateCluster
              }
          }
          // if there are no clusters within lookup bounds, use the most recent candidate cluster (possibly very old) as only candidate.
          if (clustersWithinLookupBounds.isEmpty && candidateClusters.nonEmpty) {
            clustersWithinLookupBounds += candidateClusters.head
            resultClustersBuilder ++= clustersOutsideLookupBounds.tail.result()
          } else {
            resultClustersBuilder ++= clustersOutsideLookupBounds.result()
          }
          candidateClusters = clustersWithinLookupBounds
          val iterator = candidateClusters.iterator
          var foundClusterToMergeWith = false

          while (!foundClusterToMergeWith && iterator.hasNext) {

            val candidateClusterToMergeWith = iterator.next()
            val mergedCluster = clusterWithNewObservation(candidateClusterToMergeWith, time, observation)
            def isObservationClose(candidateClusterObservation: OBSERVATION) = {
              val (candidateClusterObservationCenter, candidateClusterObservationVariance) = asDistribution(candidateClusterObservation)
              val d = distance(candidateClusterObservationCenter, mergedCluster.mean)
              val distrDistance = distributionDistance(d, mergedCluster.variance, candidateClusterObservationVariance)
              distrDistance <= lambda
            }
            // eagerly check the closeness of the new observation (in an attempt to improve the performance of this overall check.
            val isClose = isObservationClose(observation) && candidateClusterToMergeWith.observations.forall(isObservationClose)
            if (isClose) {
              candidateClusters -= candidateClusterToMergeWith
              candidateClusters += mergedCluster
              foundClusterToMergeWith = true
            } else {

            }
          }
          // if this observation could not be merged into a previous cluster, create a new one.
          if (!foundClusterToMergeWith) {
            candidateClusters += newCluster(time, observation)
          }
          reportProgress()
        }
    })
    resultClustersBuilder ++= candidateClusters.toSeq.reverseIterator
    resultClustersBuilder.result()
  }
}
