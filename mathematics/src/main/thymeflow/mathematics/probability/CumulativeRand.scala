package thymeflow.mathematics.probability

import scala.util.Random

/**
  * @author David Montoya
  */
class CumulativeRand[T] private[probability](valueProbabilities: IndexedSeq[(T, Long)])(implicit random: Random) extends DiscreteRand[T] {

  private val (cumulativeCounts, count) = {
    val cumulativeCountsBuilder = IndexedSeq.newBuilder[Long]
    val s = valueProbabilities.foldLeft(0L) {
      case (cumulativeProbability, (element, probability)) =>
        val sum = cumulativeProbability + probability
        cumulativeCountsBuilder += sum
        sum
    }
    if (s == 0L) throw new IllegalArgumentException("sum is equal to zero")
    (cumulativeCountsBuilder.result(), s)
  }

  def drawAfter(index: Int) = {
    val offset = cumulativeCounts(index)
    search(offset + Rand.nextLong(count - offset))
  }

  def drawBefore(index: Int) = {
    val before = cumulativeCounts(index - 1)
    search(Rand.nextLong(before))
  }

  private def search(offset: Long) = {
    import scala.collection.Searching._
    val index =
      new SearchImpl(cumulativeCounts).search(offset) match {
        case InsertionPoint(insertionPoint) => insertionPoint
        case Found(i) => i + 1
      }
    (valueProbabilities(index)._1, index)
  }

  def size = count

  override def draw(): T = {
    search(Rand.nextLong(count))._1
  }

  def drawWithIndex(): (T, Int) = {
    search(Rand.nextLong(count))
  }

  def all() = valueProbabilities.map(_._1)
}

