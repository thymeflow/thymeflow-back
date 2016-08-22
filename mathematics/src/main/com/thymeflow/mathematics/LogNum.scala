package com.thymeflow.mathematics

import com.typesafe.scalalogging.StrictLogging

import scala.collection.SeqLike
import scala.collection.generic.CanBuildFrom
import scala.language.{higherKinds, implicitConversions, postfixOps}
import scala.math._

final class LogNum(val logValue: Double, val positive: Boolean = true) extends Ordered[LogNum] {

  import LogNum.extract

  def +[N](other: N)(implicit num: Numeric[N]): LogNum = {
    this + LogNum(other)
  }

  def unary_- : LogNum = {
    LogNum.zero - this
  }

  def -(other: LogNum): LogNum = {
    this + LogNum.fromLog(other.logValue, !other.positive)
  }

  def +(other: LogNum): LogNum = {
    val oLogValue = other.logValue
    val oPositive = other.positive
    if (logValue == Double.NegativeInfinity)
      other
    else if (oLogValue == Double.NegativeInfinity)
      this
    else if (logValue >= oLogValue)
      if (oPositive == positive) {
        new LogNum(logValue + log1p(exp(oLogValue - logValue)), oPositive)
      } else if (positive) {
        new LogNum(logValue + log1p(-exp(oLogValue - logValue)), true)
      } else {
        // negative
        new LogNum(logValue + log1p(-exp(oLogValue - logValue)), false)
      }
    else {
      if (oPositive == positive) {
        new LogNum(oLogValue + log1p(exp(logValue - oLogValue)), oPositive)
      } else if (oPositive) {
        new LogNum(oLogValue + log1p(-exp(logValue - oLogValue)), true)
      } else {
        // negative
        new LogNum(oLogValue + log1p(-exp(logValue - oLogValue)), false)
      }
    }
  }

  def -[N](other: N)(implicit num: Numeric[N]): LogNum = {
    this - LogNum(other)
  }

  def *[N](other: N)(implicit num: Numeric[N]): LogNum = {
    this * LogNum(other)
  }

  def *(other: LogNum): LogNum = {
    LogNum.fromLog(logValue + other.logValue, if (other.positive == positive) true else false)
  }

  def /[N](other: N)(implicit num: Numeric[N]): LogNum = {
    this / LogNum(other)
  }

  def /(other: LogNum): LogNum = {
    LogNum.fromLog(logValue - other.logValue, if (other.positive == positive) true else false)
  }

  def **(pow: LogNum): LogNum = {
    if (positive) {
      new LogNum(pow.toDouble * logValue, true)
    } else {
      throw new IllegalArgumentException(s"cannot raise a negative value to a power: $this^$pow")
    }
  }

  def **[N](pow: N)(implicit num: Numeric[N]): LogNum = {
    if (positive) {
      new LogNum(num.toDouble(pow) * logValue, true)
    } else {
      throw new IllegalArgumentException(s"cannot raise a negative value to a power: $this^$pow")
    }
  }

  override def compare(that: LogNum) = {
    if (positive == that.positive) {
      logValue.compare(that.logValue) * (if (positive) 1 else -1)
    } else if (positive) {
      1
    } else {
      -1
    }
  }

  def max[N](other: N)(implicit num: Numeric[N]): LogNum = {
    val (oLogValue, oPositive) = extract(other)
    if (positive && oPositive) {
      if (logValue >= oLogValue) {
        this
      } else {
        new LogNum(oLogValue, oPositive)
      }
    } else if (positive && !oPositive) {
      this
    } else if (!positive && oPositive) {
      new LogNum(oLogValue, oPositive)
    } else {
      if (logValue >= oLogValue) {
        new LogNum(oLogValue, oPositive)
      } else {
        this
      }
    }
  }

  def min[N](other: N)(implicit num: Numeric[N]): LogNum = {
    val (oLogValue, oPositive) = extract(other)
    if (positive && oPositive) {
      if (logValue >= oLogValue) {
        new LogNum(oLogValue, oPositive)
      } else {
        this
      }
    } else if (positive && !oPositive) {
      new LogNum(oLogValue, oPositive)
    } else if (!positive && oPositive) {
      this
    } else {
      if (logValue >= oLogValue) {
        this
      } else {
        new LogNum(oLogValue, oPositive)
      }
    }
  }

  def toInt = toDouble.toInt * (if (positive) 1 else -1)

  def toLong = toDouble.toLong * (if (positive) 1L else -1L)

  def toFloat = toDouble.toFloat * (if (positive) 1.0f else -1.0f)

  def toDouble = exp(logValue) * (if (positive) 1.0d else -1.0d)

  def abs = new LogNum(logValue, true)

  override def toString = "LogNum(%s,%s)".format(logValue, positive)

  override def hashCode(): Int = (logValue, positive).hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: LogNum =>
      if (logValue == o.logValue) {
        if (logValue == Double.NegativeInfinity) {
          true
        } else {
          positive == o.positive
        }
      } else {
        false
      }
    case o => o == exp(logValue) * (if (positive) 1.0d else -1.0d)
  }
}

object LogNum extends StrictLogging {

  val zero = new LogNum(Double.NegativeInfinity, true)
  val one = new LogNum(0.0, true)

  /**
    * Constructs a LogNum from a Numeric
    *
    * @param n   numeric value
    * @param num Numeric implicit
    * @tparam N Numeric's inner type
    * @return LogNum
    */
  def apply[N](n: N)(implicit num: Numeric[N]): LogNum = {
    n match {
      case o: LogNum => o
      case _ =>
        if (num.signum(n) >= 0) {
          new LogNum(log(num.toDouble(n)), true)
        } else {
          new LogNum(log(-num.toDouble(n)), false)
        }
    }
  }

  /**
    * Constructs a LogNum from a log-value and its sign
    *
    * @param logValue the logarithmic value
    * @param positive true if the value is positive
    * @return LogNum
    */
  def fromLog(logValue: Double, positive: Boolean = true): LogNum = {
    new LogNum(logValue, positive)
  }

  implicit def enrichNumeric[N: Numeric](self: N): EnrichedNumeric[N] = new EnrichedNumeric(self)

  implicit def pairRightLogNum[T](x: (T, LogNum)): LogNum = x._2

  implicit def pairRightLogNumUpdate[T](x: (T, LogNum), n: LogNum): (T, LogNum) = (x._1, n)

  implicit def pairLeftLogNum[T](x: (LogNum, T)): LogNum = x._1

  implicit def pairLeftLogNumUpdate[T](x: (LogNum, T), n: LogNum): (LogNum, T) = (n, x._2)

  implicit def simpleLogNum[T](x: (LogNum)): LogNum = x

  implicit def simpleLogNumUpdate[T](x: (LogNum), n: LogNum): (LogNum) = n

  implicit def seqToEnrichedSeq[X, SEQ[X] <: SeqLike[X, SEQ[X]]](self: SEQ[X])
                                                                (implicit logNum: X => LogNum, updateLogNum: (X, LogNum) => X, bf: CanBuildFrom[SEQ[X], X, SEQ[X]]): EnrichedSeq[X, SEQ] =
    new EnrichedSeq[X, SEQ](self)

  def pow[N: Numeric](n: LogNum, pow: N): LogNum = n ** pow

  def pow[N: Numeric](n: N, pow: LogNum): LogNum = n ** pow

  private def extract[N](other: N)(implicit num: Numeric[N]): (Double, Boolean) = {
    other match {
      case o: LogNum => (o.logValue, o.positive)
      case _ =>
        if (num.signum(other) >= 0) {
          (log(num.toDouble(other)), true)
        } else {
          (log(-num.toDouble(other)), false)
        }
    }
  }

  trait LogNumOrdering extends Ordering[LogNum] {
    override def compare(a: LogNum, b: LogNum) = a compare b
  }

  trait LogNumIsFractional extends Fractional[LogNum] {
    def plus(x: LogNum, y: LogNum): LogNum = x + y

    def minus(x: LogNum, y: LogNum): LogNum = x - y

    def times(x: LogNum, y: LogNum): LogNum = x * y

    def div(x: LogNum, y: LogNum): LogNum = x / y

    def negate(x: LogNum): LogNum = -x

    def fromInt(x: Int): LogNum = LogNum(x)

    def toInt(x: LogNum): Int = x.toInt

    def toLong(x: LogNum): Long = x.toLong

    def toFloat(x: LogNum): Float = x.toFloat

    def toDouble(x: LogNum): Double = x.toDouble

    override def zero = LogNum.zero

    override def one = LogNum.one

    override def abs(x: LogNum): LogNum = new LogNum(x.logValue, true)

    override def signum(x: LogNum): Int =
      if (x.logValue == Double.NegativeInfinity) {
        0
      } else if (x.positive) {
        1
      } else {
        -1
      }
  }

  class EnrichedNumeric[N](self: N)(implicit num: Numeric[N]) {
    def +(n: LogNum) = toLogNum + n

    def -(n: LogNum) = toLogNum - n

    def *(n: LogNum) = toLogNum * n

    def toLogNum =
      if (num.signum(self) >= 0) {
        new LogNum(log(num.toDouble(self)), true)
      } else {
        new LogNum(log(-num.toDouble(self)), false)
      }

    def /(n: LogNum) = toLogNum / n

    def **(n: LogNum) = toLogNum ** n
  }

  class EnrichedSeq[X, SEQ[X] <: SeqLike[X, SEQ[X]]](self: SEQ[X])(implicit logNum: X => LogNum, updateLogNum: (X, LogNum) => X, bf: CanBuildFrom[SEQ[X], X, SEQ[X]]) {

    def maxNormalize: SEQ[X] = {
      val maxLogNum = logNum(self.maxBy(logNum))
      if (maxLogNum.logValue.isNaN) {
        throw new IllegalArgumentException("LogNum max normalization cannot proceed : some values are equal to NaN")
      }
      if (maxLogNum.logValue.isInfinite) {
        logger.warn("LogNum max normalization : max LogNum is infinite, using normalization factor of 1")
        self.map { case (x) => x }(bf)
      } else {
        self.map { case (x) => updateLogNum(x, logNum(x) / maxLogNum) }(bf)
      }
    }

    def reduceSum: LogNum = {
      val negatives = Array.newBuilder[Double]
      val positives = Array.newBuilder[Double]
      self.foreach {
        case x =>
          val l = logNum(x)
          if (l.positive) positives += l.logValue
          else negatives += l.logValue
      }
      def reduceSameSign(values: Array[Double], positive: Boolean): LogNum = {
        if (values.isEmpty) {
          LogNum.zero
        } else {
          val m = values.max
          if (m.isInfinite) {
            LogNum.fromLog(m, positive)
          } else {
            val maxLogValue = m
            LogNum.fromLog(maxLogValue + log(values.foldLeft(0.0) {
              case (accum, n) =>
                accum + exp(n - maxLogValue)
            }), positive)
          }
        }
      }
      reduceSameSign(positives.result(), positive = true) + reduceSameSign(negatives.result(), positive = false)
    }

    def normalize: SEQ[X] = {
      assert(self.forall(logNum(_).positive))
      val sorted = self.sortBy(logNum)
      val max = logNum(sorted.last)
      if (max.logValue.isInfinite) {
        throw new IllegalArgumentException(s"LogNum normalization cannot proceed: maximum is zero or infinite: ${max.logValue}")
      } else {
        val maxLogValue = max.logValue
        val sum = LogNum.fromLog(maxLogValue + log(self.foldLeft(0.0) {
          case (accum, n) =>
            accum + exp(logNum(n).logValue - maxLogValue)
        }), positive = true)
        self.map { case (x) => updateLogNum(x, logNum(x) / sum) }(bf)
      }
    }
  }

  implicit object LogNumIsFractional extends LogNumIsFractional with LogNumOrdering

}