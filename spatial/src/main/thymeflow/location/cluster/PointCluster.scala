package thymeflow.location.cluster

import thymeflow.spatial.mapping.surfaceprojection.SimpleLocalCartesianProjection
import thymeflow.spatial.{cartesian, geographic}

/**
  * @author David Montoya
  */
trait PointCluster[OBSERVATION, T] extends Cluster[OBSERVATION, geographic.Point, T] {

  def settings: ClusterSettings[OBSERVATION]

  override def weightedMean: geographic.Point = {
    val center = observations.minBy(settings.observationAccuracy)
    val mapping = SimpleLocalCartesianProjection(settings.observationMean(center), settings.metric)
    val baseAccuracy = settings.observationAccuracy(center)
    val accuracySum = observations.map(observation => settings.observationWeight(observation) * baseAccuracy / settings.observationAccuracy(observation)).sum
    mapping.inverse(cartesian.Geometry.point(observations.map {
      case observation => mapping.map(settings.observationMean(observation)).scale((settings.observationWeight(observation) * baseAccuracy / settings.observationAccuracy(observation)) / accuracySum)
    }.reduce(_.plus(_))))
  }

}
