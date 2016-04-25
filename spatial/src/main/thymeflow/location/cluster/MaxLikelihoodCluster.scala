package thymeflow.location.cluster

import thymeflow.mathematics.LogNum
import thymeflow.spatial
import thymeflow.spatial.geographic.Point
import thymeflow.spatial.mapping.surfaceprojection.{SimpleLocalCartesianProjection, SurfaceProjection}

/**
  * @author David Montoya
  */

class MaxLikelihoodCluster[OBSERVATION, T] private(val cartesianMapping: SurfaceProjection,
                                                   val t: T,
                                                   val observations: IndexedSeq[OBSERVATION],
                                                   val totalWeight: Double,
                                                   val mean: Point,
                                                   val xMeanNumerator: LogNum,
                                                   val yMeanNumerator: LogNum,
                                                   val varianceNormalizer: LogNum,
                                                   val varianceProduct: LogNum,
                                                   val variance: Double,
                                                   val settings: ClusterSettings[OBSERVATION],
                                                   val index: Int)
  extends PointCluster[OBSERVATION, T] {

  def accuracy = Math.sqrt(variance)
  override def toString = s"MaxLikelihoodCluster($t, $observations, $mean, $variance, $totalWeight)"
}

object MaxLikelihoodCluster {

  def apply[OBSERVATION, T](t: T,
                            observation: OBSERVATION,
                            settings: ClusterSettings[OBSERVATION], index: Int): MaxLikelihoodCluster[OBSERVATION, T] = {

    apply(t,
      observation,
      settings,
      cartesianMappingOption = None,
      previousObservations = Vector(),
      previousVarianceNormalizer = LogNum.zero,
      previousVarianceProduct = LogNum.one,
      previousTotalWeight = 0d,
      previousXMeanNumerator = LogNum.zero,
      previousYMeanNumerator = LogNum.zero,
      index = index)
  }

  /**
    *
    *
    * combinedVariances = (v_2 \cdots v_n, v_1 v_3 \cdots v_n, \ldots, v_1 \cdots v_{n_1})
    * varianceNormalizer = \sum{ combinedVariances }
    * varianceProduct =  v_1 \ldots v_n / varianceNormalizer
    * xMean = \sum { (x_1, \ldots, x_n). combinedVariances }
    * yMean = \sum { (y_1, \ldots, y_n). combinedVariances }
    *
    * @param t
    * @param observation
    * @param settings
    * @param cartesianMappingOption
    * @param previousObservations
    * @param previousTotalWeight
    * @param previousXMeanNumerator
    * @param previousYMeanNumerator
    * @param previousVarianceNormalizer
    * @param previousVarianceProduct
    * @tparam OBSERVATION
    * @return
    */
  def apply[OBSERVATION, T](t: T,
                            observation: OBSERVATION,
                            settings: ClusterSettings[OBSERVATION],
                            cartesianMappingOption: Option[SurfaceProjection],
                            previousObservations: IndexedSeq[OBSERVATION],
                            previousTotalWeight: Double,
                            previousXMeanNumerator: LogNum,
                            previousYMeanNumerator: LogNum,
                            previousVarianceNormalizer: LogNum,
                            previousVarianceProduct: LogNum,
                            index: Int): MaxLikelihoodCluster[OBSERVATION, T] = {
    val observationMean = settings.observationMean(observation)
    val cartesianMapping = cartesianMappingOption.getOrElse(SimpleLocalCartesianProjection(observationMean, settings.metric))
    val observationVariance = settings.observationVariance(observation)
    val observations = previousObservations :+ observation
    val observationWeight = settings.observationWeight(observation)
    val observationCartesian = cartesianMapping.map(observationMean)
    val observationFactor = LogNum(observationVariance) ** observationWeight
    val observationVarianceProduct = if (observationWeight == 1d) previousVarianceProduct else previousVarianceProduct * (LogNum(observationVariance) ** (observationWeight - 1d))
    val xMeanNumerator = (previousXMeanNumerator * observationFactor) + observationCartesian.x * observationVarianceProduct * observationWeight
    val yMeanNumerator = (previousYMeanNumerator * observationFactor) + observationCartesian.y * observationVarianceProduct * observationWeight
    val varianceNormalizer = (previousVarianceNormalizer * observationFactor) + observationVarianceProduct * observationWeight
    val varianceProduct = previousVarianceProduct * observationFactor
    val xMean = xMeanNumerator / varianceNormalizer
    val yMean = yMeanNumerator / varianceNormalizer
    val newClusterMean = cartesianMapping.inverse(spatial.cartesian.Geometry.point(xMean.toDouble, yMean.toDouble))
    val totalWeight = previousTotalWeight + observationWeight
    val newClusterVariance = (varianceProduct * (totalWeight / varianceNormalizer)).toDouble
    new MaxLikelihoodCluster(
      cartesianMapping = cartesianMapping,
      t = t,
      observations = observations, totalWeight = totalWeight,
      mean = newClusterMean,
      xMeanNumerator = xMeanNumerator,
      yMeanNumerator = yMeanNumerator,
      varianceNormalizer = varianceNormalizer,
      varianceProduct = varianceProduct,
      variance = newClusterVariance,
      settings = settings,
      index = index
    )
  }

  def apply[OBSERVATION, T](cluster: MaxLikelihoodCluster[OBSERVATION, T],
                            t: T,
                            observation: OBSERVATION, index: Int): MaxLikelihoodCluster[OBSERVATION, T] = {
    apply(t,
      observation,
      cluster.settings,
      cartesianMappingOption = Some(cluster.cartesianMapping),
      previousObservations = cluster.observations,
      previousVarianceNormalizer = cluster.varianceNormalizer,
      previousVarianceProduct = cluster.varianceProduct,
      previousTotalWeight = cluster.totalWeight,
      previousXMeanNumerator = cluster.xMeanNumerator,
      previousYMeanNumerator = cluster.yMeanNumerator,
      index = index)
  }

}
