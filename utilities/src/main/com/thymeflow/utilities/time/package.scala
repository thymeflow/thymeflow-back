package com.thymeflow.utilities

import java.time._

import scala.language.implicitConversions

/**
  * @author David Montoya
  */
package object time {
  def now() = {
    Instant.now()
  }

  def ofEpochSeconds(time: Long, nanoOffset: Long = 0) = {
    Instant.ofEpochSecond(time).plusNanos(0)
  }

  def ofEpochMilliseconds(time: Long, nanoOffset: Long = 0) = {
    Instant.ofEpochMilli(time).plusNanos(nanoOffset)
  }

  trait DurationNumeric extends Numeric[Duration] {
    override def plus(x: Duration, y: Duration): Duration = x.plus(y)

    override def toFloat(x: Duration): Float = toDouble(x).toFloat

    override def toDouble(x: Duration): Double = toLong(x) + x.getNano.toDouble / 1000000000.0d

    override def toInt(x: Duration): Int = toLong(x).toInt

    override def toLong(x: Duration): Long = x.getSeconds

    override def negate(x: Duration): Duration = x.negated()

    override def fromInt(x: Int): Duration = Duration.ofSeconds(x)

    override def times(x: Duration, y: Duration): Duration = throw new IllegalArgumentException("Duration.times is not a valid operation.")

    override def minus(x: Duration, y: Duration): Duration = x.minus(y)

    override def compare(x: Duration, y: Duration): Int = x.compareTo(y)
  }

  class DurationWithToSecondsDouble(val d: Duration) {
    def toSecondsDouble: Double = {
      d.getSeconds.toDouble + d.getNano.toDouble / 1000000000.0d
    }
  }

  class DoubleWithToDurationAsSeconds(val d: Double) {
    def toDurationAsSeconds: Duration = {
      val t = d.toLong
      val remainder = ((d - t) * 1000000000.0d).toLong
      Duration.ofSeconds(t, remainder)
    }
  }


  object Implicits {
    val durationOrdering: Ordering[Duration] = implicitly[Ordering[Duration]]
    val instantOrdering: Ordering[Instant] = implicitly[Ordering[Instant]]

    implicit def durationWithToSecondsDouble(d: Duration): DurationWithToSecondsDouble = new DurationWithToSecondsDouble(d)
    implicit def doubleWithToDurationAsSeconds(d: Double): DoubleWithToDurationAsSeconds = new DoubleWithToDurationAsSeconds(d)

    implicit def durationOrderingOps(lhs: Duration): durationOrdering.Ops = new durationOrdering.Ops(lhs)
    implicit def instantOrderingOps(lhs: Instant): instantOrdering.Ops = new instantOrdering.Ops(lhs)

    implicit object DurationIsNumeric extends DurationNumeric
  }

}
