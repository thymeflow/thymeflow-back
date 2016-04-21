package pkb.utilities

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.{Logger, StrictLogging}

/**
  * @author David Montoya
  */
object TimeExecution extends StrictLogging {

  def timeDebug[R](processName: String, block: => R): R = {
    timeDebug(processName, logger, block)
  }

  def timeDebug[R](processName: String, logger: Logger, block: => R): R = {
    time(processName, (x) => {
      logger.debug(x)
    }, block)
  }

  def timeInfo[R](processName: String, block: => R): R = {
    timeInfo(processName, logger, block)
  }

  def timeInfo[R](processName: String, logger: Logger, block: => R): R = {
    time(processName, (x) => {
      logger.info(x)
    }, block)
  }

  def time[R](processName: String, log: String => Unit, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    log(s"[$processName] - Elapsed time: ${Duration.ofNanos(t1 - t0)}")
    result
  }

  def timeProgressStep[R](processName: String, target: Long, logger: Logger, block: (() => Unit) => R, step: Long = 1): R = {
    timeProgress(processName, target, logger,
      (progress) => {
        val p = new AtomicLong(0)
        val progressImplicit = () => {
          progress(p.addAndGet(step))
        }
        block(progressImplicit)
      })
  }

  def timeProgress[R](processName: String, target: Long, logger: Logger, block: (Long => Unit) => R): R = {
    var lastProgress = (0L, System.nanoTime())
    var done = false
    val beginTime = System.nanoTime()
    val period = Duration.ofSeconds(5)
    val progress = (p: Long) => {
      val percent = (p.toDouble / target.toDouble) * 100.0
      val currentTime = System.nanoTime()
      val b = if (!done && p == target) {
        done = true
        true
      } else {
        false
      }
      if (b || Duration.ofNanos(currentTime - lastProgress._2).compareTo(period) >= 0) {
        val processDuration = Duration.ofNanos(currentTime - beginTime)
        val averageSpeed = 1d / durationToSecondsDouble(processDuration.dividedBy(p))
        if (b) {
          logger.info(f"[$processName] - $percent%.2f%% (averageSpeed=$averageSpeed%.2f ops/s, time=$processDuration}).")
        } else {
          val currentSpeed = 1d / durationToSecondsDouble(Duration.ofNanos(currentTime - lastProgress._2).dividedBy(p - lastProgress._1))
          val speed = 0.5 * averageSpeed + 0.5 * currentSpeed
          val eta = secondsDoubleToDuration(((target - p).toDouble / speed).ceil)
          logger.info(f"[$processName] - $percent%.2f%% ($speed%.2f ops/s, eta=$eta}.")
        }
        lastProgress = (p, currentTime)
      }
    }
    block(progress)
  }

  private def durationToSecondsDouble(d: Duration): Double = {
    d.getSeconds.toDouble + d.getNano.toDouble / 1000000000.0d
  }

  private def secondsDoubleToDuration(time: Double): Duration = {
    val t = time.toLong
    val remainder = ((time - t) * 1000000000.0d).toLong
    Duration.ofSeconds(t, remainder)
  }

}
