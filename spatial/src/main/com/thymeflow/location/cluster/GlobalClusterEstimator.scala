package com.thymeflow.location.cluster

import com.thymeflow.utilities.TimeExecution
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable.ArrayBuffer

/**
  * @author David Montoya
  */
trait GlobalClusterEstimator[OBSERVATION, SPACE, T, CLUSTER <: Cluster[OBSERVATION, SPACE, T]] extends StrictLogging {
  def lambda: Double

  def asDistribution(observation: OBSERVATION): (SPACE, Double)

  def clusterWithNewObservation(cluster: CLUSTER, newObservation: OBSERVATION): CLUSTER

  def newCluster(observation: OBSERVATION): CLUSTER

  def distributionDistance(spaceDistance: Double, clusterVariance: Double, observationVariance: Double): Double

  def distance(from: SPACE, to: SPACE): Double

  /**
    * LookupRadius takes a Variance, returns a lookup radius
    *
    * @return
    */
  def lookupRadius(variance: Double): Double

  def estimate2(observations: Traversable[OBSERVATION], nearbyObservations: (SPACE, Double) => Traversable[(OBSERVATION, Double)]): IndexedSeq[CLUSTER] = {
    val resultClustersBuilder = new ArrayBuffer[CLUSTER]
    val clusteredObservationMap = new scala.collection.mutable.OpenHashMap[OBSERVATION, CLUSTER]
    val length = observations.size
    val processName = "global-cluster-estimator"
    logger.debug(s"[$processName] Estimating clusters from $length observations {lambda=$lambda}...")
    val candidates = TimeExecution.timeProgressStep(processName, length, logger, {
      case reportProgress =>
        observations.flatMap {
          case observation =>
            val (observationMean, observationVariance) = asDistribution(observation)
            clusteredObservationMap.put(observation, newCluster(observation))
            reportProgress()
            nearbyObservations(observationMean, lookupRadius(observationVariance)).toIndexedSeq.map {
              case (observation2, geoDistance) => (observation, observation2, distributionDistance(geoDistance, observationVariance, asDistribution(observation2)._2))
            }
        }(scala.collection.breakOut[Traversable[OBSERVATION], (OBSERVATION, OBSERVATION, Double), IndexedSeq[(OBSERVATION, OBSERVATION, Double)]]).sortBy(_._3)
    })
    TimeExecution.timeProgressStep(processName, candidates.size, logger, {
      case reportProgress =>
        for ((o1, o2, d) <- candidates) {
          def isObservationClose(candidateCluster: CLUSTER)(candidateClusterObservation: OBSERVATION) = {
            val (candidateClusterObservationCenter, candidateClusterObservationVariance) = asDistribution(candidateClusterObservation)
            val d = distance(candidateClusterObservationCenter, candidateCluster.mean)
            val distrDistance = distributionDistance(d, candidateCluster.variance, candidateClusterObservationVariance)
            distrDistance <= lambda
          }
          val cluster1 = clusteredObservationMap(o1)
          val cluster2 = clusteredObservationMap(o2)
          if (cluster1 != cluster2) {
            val (clusterA, clusterB) = if (cluster2.size < cluster1.size) {
              (cluster1, cluster2)
            } else {
              (cluster2, cluster1)
            }
            val mergedCandidateCluster = clusterB.observations.foldLeft(clusterA) {
              case (c, o) => clusterWithNewObservation(c, o)
            }
            val isClose = clusterB.observations.forall(isObservationClose(mergedCandidateCluster)) && clusterA.observations.forall(isObservationClose(mergedCandidateCluster))
            if (isClose) {
              clusterA.observations.foreach(clusteredObservationMap.put(_, mergedCandidateCluster))
              clusterB.observations.foreach(clusteredObservationMap.put(_, mergedCandidateCluster))
            }
          }
          reportProgress()
        }
        resultClustersBuilder ++= clusteredObservationMap.values.toSet
    })
    resultClustersBuilder.result()
  }

  def estimate(observations: Traversable[OBSERVATION], nearbyObservations: (SPACE, Double) => Traversable[(OBSERVATION, Double)]): IndexedSeq[CLUSTER] = {
    val resultClustersBuilder = new ArrayBuffer[CLUSTER]
    val clusteredObservations = new scala.collection.mutable.HashSet[OBSERVATION]
    val length = observations.size
    val processName = "global-cluster-estimator"
    logger.debug(s"[$processName] Estimating clusters from $length observations {lambda=$lambda}...")
    TimeExecution.timeProgressStep(processName, length, logger, {
      case reportProgress =>
        for (observation <- observations) {
          if (!clusteredObservations.contains(observation)) {
            var candidateCluster = newCluster(observation)
            clusteredObservations += observation
            val candidateObservations = nearbyObservations(candidateCluster.mean, lookupRadius(candidateCluster.variance)).toIndexedSeq.sortBy {
              case (x, d) => distributionDistance(d, candidateCluster.variance, asDistribution(x)._2)
            }
            for ((candidateObservation, _) <- candidateObservations) {
              if (!clusteredObservations.contains(candidateObservation)) {
                val mergedCandidateCluster = clusterWithNewObservation(candidateCluster, candidateObservation)
                def isObservationClose(candidateClusterObservation: OBSERVATION) = {
                  val (candidateClusterObservationCenter, candidateClusterObservationVariance) = asDistribution(candidateClusterObservation)
                  val d = distance(candidateClusterObservationCenter, mergedCandidateCluster.mean)
                  val distrDistance = distributionDistance(d, mergedCandidateCluster.variance, candidateClusterObservationVariance)
                  distrDistance <= lambda
                }
                val isClose = isObservationClose(candidateObservation) && candidateCluster.observations.forall(isObservationClose)
                if (isClose) {
                  candidateCluster = mergedCandidateCluster
                  clusteredObservations += candidateObservation
                }
              }
            }
            resultClustersBuilder += candidateCluster
          }
          reportProgress()
        }
    })
    resultClustersBuilder.result()
  }
}
