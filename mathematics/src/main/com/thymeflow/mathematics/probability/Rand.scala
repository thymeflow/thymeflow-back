package com.thymeflow.mathematics.probability

import scala.util.Random

/**
  * @author David Montoya
  */
trait Rand[T] {
  def draw(): T
}

trait DiscreteRand[T] extends Rand[T] {
  def all(): Traversable[T]
}

object Rand {

  def nextLong(bound: Long)(implicit random: Random) = {
    if (bound <= 0) throw new IllegalArgumentException("bound must be positive")
    def randomBits() = {
      // unsigned long
      (random.nextLong << 1) >>> 1
    }
    var r: Long = randomBits()
    val m: Long = bound - 1
    if ((bound & m) == 0) // i.e., bound is a power of 2
      r = r % bound
    else {
      var u: Long = r
      def a() = {
        r = u % bound
        r
      }
      while (u - a() + m < 0) {
        u = randomBits()
      }
    }
    r
  }

  def shuffleTwoWeightedOrdered[T](weightedElements: IndexedSeq[(T, Long)])(implicit random: Random): (DiscreteRand[(T, T)], Long) = {
    if (weightedElements.size < 2) throw new IllegalArgumentException("there should be two or more elements")
    val adjustedWeightElements = weightedElements.indices.map {
      i =>
        val (e1, s1) = weightedElements(i)
        val s = ((i + 1) to weightedElements.indices.last).map {
          j => s1 * weightedElements(j)._2
        }.sum
        (e1, s)
    }
    val adjustedCumulativeRand = Rand.cumulative(adjustedWeightElements)
    val cumulativeRand = Rand.cumulative(weightedElements)
    (new DiscreteRand[(T, T)] {
      def draw() = {
        // sample from 0 to n - 2
        val (e1, i) = adjustedCumulativeRand.drawWithIndex()
        // sample from 0 to n - 1 without i
        val (e2, _) = cumulativeRand.drawAfter(i)
        (e1, e2)
      }

      def all() = {
        weightedElements.indices.flatMap {
          i =>
            val e1 = weightedElements(i)._1
            (i + 1 to weightedElements.indices.last).map {
              j => (e1, weightedElements(j)._1)
            }
        }
      }
    }, adjustedCumulativeRand.size)
  }

  def cumulative[T](weightedElements: IndexedSeq[(T, Long)])(implicit random: Random) = {
    new CumulativeRand[T](weightedElements)
  }

  def shuffleTwoOrdered[T](elements: IndexedSeq[T])(implicit random: Random): Rand[(T, T)] = {
    if (elements.size < 2) throw new IllegalArgumentException("there should be two or more elements")
    new Rand[(T, T)] {
      def draw() = {
        // sample from 0 to n - 1
        val i = random.nextInt(elements.size)
        // sample from 0 to n - 1 without i
        var j = random.nextInt(elements.size - 1)
        if (i != elements.size - 1) {
          if (j >= i) {
            j += 1
          }
        }
        if (i < j) {
          (elements(i), elements(j))
        } else {
          (elements(j), elements(i))
        }
      }
    }
  }

  def uniform[T](elements: IndexedSeq[T])(implicit random: Random) = {
    new UniformRand[T](elements)
  }
}