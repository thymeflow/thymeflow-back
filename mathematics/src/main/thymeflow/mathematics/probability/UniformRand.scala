package thymeflow.mathematics.probability

import scala.util.Random

/**
  * @author David Montoya
  */
class UniformRand[T] private[probability](val elements: IndexedSeq[T])(implicit random: Random) extends Rand[T] {
  def draw() = elements(random.nextInt(elements.size))
}
