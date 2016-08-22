package com.thymeflow.mathematics

/**
  * @author David Montoya
  */
object HellingerDistance {

  /**
    * Hellinger distance between two normal distributions P ~ N(u1,v1) and Q ~ N(u2,v2)
    *
    * @param meanDifference u1 - u2
    * @param variance1      v1
    * @param variance2      v2
    * @return H(P,Q)
    */
  def normal(meanDifference: Double, variance1: Double, variance2: Double) = {
    math.sqrt(normalSquared(meanDifference, variance1, variance2))
  }

  /**
    * Squared hellinger distance between two normal distributions P ~ N(u1,v1) and Q ~ N(u2,v2)
    *
    * @param meanDifference u1 - u2
    * @param variance1      v1
    * @param variance2      v2
    * @return H(P,Q)**2
    */
  def normalSquared(meanDifference: Double, variance1: Double, variance2: Double) = {
    val z = math.sqrt(variance1 * variance2) * 2d * math.exp(-0.25d * meanDifference * meanDifference / (variance1 + variance2)) / (variance1 + variance2)
    if (z <= 0) {
      1.0
    } else if (z >= 1) {
      0.0
    } else {
      1 - z
    }
  }

}
