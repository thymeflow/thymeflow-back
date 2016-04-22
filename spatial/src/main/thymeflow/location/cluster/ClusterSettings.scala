package thymeflow.location.cluster

import thymeflow.spatial.geographic.Point
import thymeflow.spatial.metric.Metric

/**
  * @author David Montoya
  */
case class ClusterSettings[OBSERVATION](metric: Metric[Point, Double],
                                        observationMean: OBSERVATION => Point,
                                        observationAccuracy: OBSERVATION => Double,
                                        observationVariance: OBSERVATION => Double,
                                        observationWeight: OBSERVATION => Double = (_: OBSERVATION) => 1d)
