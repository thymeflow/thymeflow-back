package thymeflow.graph

import org.scalatest._

/**
  * @author David Montoya
  */
class FlowAlgorithmsSpec extends FlatSpec with Matchers {

  "minCostMaxFlow" should "solve min-cost max-flow in a simple trellis" in {
    val cap = Array(
      Array(0, 1, 0, 0, 0),
      Array(0, 0, 1, 1, 0),
      Array(0, 0, 0, 0, 1),
      Array(0, 0, 0, 0, 1),
      Array(0, 0, 0, 0, 0))

    val cost = Array(
      Array(0.0, 0.0, 0.0, 0.0, 0.0),
      Array(0.0, 0.0, 4.0, 10.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0, 1.0),
      Array(0.0, 0.0, 0.0, 0.0, 3.0),
      Array(0.0, 0.0, 0.0, 0.0, 0.0))


    val r1 = FlowAlgorithms.minCostMaxFlow[Int](cap.indices, { case (from, to) => cap(from)(to) }, { case (from, to) => cost(from)(to) },
      0, 4)


    r1._1 should be(1)
    r1._2 should be(5)
  }
}
