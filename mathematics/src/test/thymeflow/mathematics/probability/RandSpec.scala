package thymeflow.mathematics.probability

import org.scalatest._

/**
  * @author David Montoya
  */
class RandSpec extends FlatSpec with Matchers {
  implicit val random = scala.util.Random

  "CumulativeRand" should "size should be equal to the sum of weights" in {
    val elements = Vector((1, 10L), (2, 5L), (3, 15L))
    Rand.cumulative(elements).size should equal(elements.map(_._2).sum)
  }

  "CumulativeRand" should "sample cumulative distributions" in {
    val elements = Vector((1, 10L), (2, 5L), (3, 15L))
    val rand = Rand.cumulative(elements)
    val expectedDistribution = elements.map {
      case (element, weight) => element -> weight / rand.size.toDouble
    }.toMap
    val sampleSize = 1000000
    val distribution = (for (i <- 1 to sampleSize) yield {
      rand.draw()
    }).groupBy(identity).map {
      case (element, e) => element -> e.size.toDouble / sampleSize.toDouble
    }
    val meanError = distribution.map {
      case (element, v) => Math.abs(expectedDistribution.getOrElse(element, 0d) - v)
    }.sum / elements.size
    meanError should be <= 0.01
  }

  "Rand" should "shuffleTwoWeightedOrdered" in {
    val elements = Vector((1, 10L), (2, 5L), (3, 15L))
    val expected = elements.indices.flatMap {
      i =>
        val s1 = elements(i)._2
        (i + 1 to elements.indices.last).map {
          j => ((elements(i)._1, elements(j)._1), s1 * elements(j)._2)
        }
    }
    val expectedCount = expected.map(_._2).sum.toDouble
    val expectedDistribution = expected.map {
      case (e1, c) => (e1, c.toDouble / expectedCount)
    }.toMap
    val (rand, _) = Rand.shuffleTwoWeightedOrdered(elements)
    val sampleSize = 1000000
    val distribution = (for (i <- 1 to sampleSize) yield {
      rand.draw()
    }).groupBy(identity).map {
      case (element, e) => element -> e.size.toDouble / sampleSize.toDouble
    }
    val meanError = distribution.map {
      case (element, v) => Math.abs(expectedDistribution.getOrElse(element, 0d) - v)
    }.sum / elements.size
    meanError should be <= 0.01
  }

}
