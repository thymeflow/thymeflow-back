package com.thymeflow.location.cluster

import com.thymeflow.spatial.geographic.Point
import com.thymeflow.spatial.metric.Metric

/**
  * @author David Montoya
  */
case class ClusterSettings[OBSERVATION](metric: Metric[Point, Double],
                                        observationMean: OBSERVATION => Point,
                                        observationAccuracy: OBSERVATION => Double,
                                        observationVariance: OBSERVATION => Double,
                                        observationWeight: OBSERVATION => Double = (_: OBSERVATION) => 1d)
