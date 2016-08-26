package com.thymeflow.utilities

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

import com.thymeflow.utilities.time.Implicits._
import com.typesafe.scalalogging.{Logger, StrictLogging}

/**
  * @author David Montoya
  */
object TimeExecution extends StrictLogging {

  def time[R](processName: String, log: String => Unit, block: => R, processInfo: String = ""): R = {
    log(s"[$processName] - Started${if (processInfo.nonEmpty) ": " else ""}$processInfo.")
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    log(s"[$processName] - Elapsed time: ${Duration.ofNanos(t1 - t0)}")
    result
  }

  def timeDebug[R](processName: String, logger: Logger, block: => R, processInfo: String = ""): R = {
    time(processName, (x) => {
      logger.debug(x)
    }, block, processInfo)
  }

  def timeInfo[R](processName: String, logger: Logger, block: => R, processInfo: String = ""): R = {
    time(processName, (x) => {
      logger.info(x)
    }, block, processInfo)
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
      if (p > 0 && (b || Duration.ofNanos(currentTime - lastProgress._2) >= period)) {
        val processDuration = Duration.ofNanos(currentTime - beginTime)
        val averageSpeed = 1d / processDuration.dividedBy(p).toSecondsDouble
        if (b) {
          logger.info(f"[$processName] - $percent%.2f%% {averageSpeed=$averageSpeed%.2f ops/s, time=$processDuration}.")
        } else {
          val steps = p - lastProgress._1
          if (steps > 0) {
            val timePerStep = Duration.ofNanos(currentTime - lastProgress._2).dividedBy(steps).toSecondsDouble
            if (timePerStep > 0d) {
              val currentSpeed = 1d / timePerStep
              val speed = 0.5 * averageSpeed + 0.5 * currentSpeed
              val eta = ((target - p).toDouble / speed).ceil.toDurationAsSeconds
              logger.info(f"[$processName] - $percent%.2f%% {$speed%.2f ops/s, eta=$eta}.")
            }
          }
        }
        lastProgress = (p, currentTime)
      }
    }
    block(progress)
  }

}
