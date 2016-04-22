package thymeflow.spatial.metric

/**
  * @author David Montoya
  */
trait Metric[-SPACE, @specialized(Double) W] {
  /**
    * Given two points on a unit sphere, calculate their distance apart in radians.
    *
    * @param from first point
    * @param to   second point
    * @return distance between points, in radians
    */
  def distance(from: SPACE, to: SPACE): W
}
