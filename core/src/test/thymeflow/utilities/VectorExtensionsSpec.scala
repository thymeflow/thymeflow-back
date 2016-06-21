package thymeflow.utilities

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author David Montoya
  */
class VectorExtensionsSpec extends FlatSpec with Matchers with StrictLogging {

  "reduceLeftTree" should "have the same result as reduce" in {
    val s1 = Vector(List(1), List(2), List(3), List(4), List(5), List(6), List(7), List(8), List(9), List(10))
    for (i <- 1 to s1.size) {
      val s1Cut = s1.take(i)
      s1Cut.reduce(_ ++ _) should be(VectorExtensions.reduceLeftTree(s1Cut)(_ ++ _))
    }
  }

  "reduceLeftTree" should "perform a balanced reduce" in {
    val s1 = Vector(List(1), List(2), List(3), List(4), List(5), List(6), List(7), List(8), List(9), List(10))
    def depth(v: Any): Int = {
      v match {
        case Vector(x, y) => Math.max(depth(x), depth(y)) + 1
        case _ => 0
      }
    }
    for (i <- 1 to s1.size) {
      val s1Cut = s1.take(i)
      val expectedDepth = 32 - Integer.numberOfLeadingZeros(i - 1)
      depth(VectorExtensions.reduceLeftTree(s1Cut.map(x => x: Any))((x1, x2) => Vector(x1, x2))) should be(expectedDepth)
    }
  }

  "reduceLeftTree" should "throw exception on empty input" in {
    intercept[UnsupportedOperationException] {
      VectorExtensions.reduceLeftTree[List[Int]](Vector.empty)(_ ++ _)
    }
  }
}
