package thymeflow.spatial.metric

/**
  * @author David Montoya
  */
trait LinearMetric[-SPACE, -LINEAR, @specialized(Double) W] extends Metric[SPACE, W] {
  def length(o: LINEAR): W
}
