package thymeflow.spatial.metric

import com.typesafe.scalalogging.StrictLogging
import thymeflow.utilities.Memoize

/**
  * @author David Montoya
  */
trait CachedLinearMetric[-SPACE, -LINEAR, @specialized(Double) W] extends LinearMetric[SPACE, LINEAR, W] with StrictLogging {
  private val lengthCachedFunction = Memoize.concurrentFifoCache[LINEAR, W](cacheSize, innerMetric.length)
  private val distanceCachedFunction = Memoize.concurrentFifoCache[SPACE, SPACE, W](cacheSize, innerMetric.distance)

  def innerMetric: LinearMetric[SPACE, LINEAR, W]

  def cacheSize = 1000

  override def length(o: LINEAR) = lengthCachedFunction(o)

  override def distance(from: SPACE, to: SPACE) = distanceCachedFunction(from, to)

}
