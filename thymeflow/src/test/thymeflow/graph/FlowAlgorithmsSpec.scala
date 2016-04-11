package thymeflow.graph

import org.scalatest._

/**
  * @author David Montoya
  */
class FlowAlgorithmsSpec extends FlatSpec with Matchers {

  "minCostMaxFlow" should "solve min-cost max-flow in a simple trellis" in {
    val edges = Vector(
      (0, 1, 1.0, 0.0),
      (1, 2, 1.0, 4.0), (1, 3, 1.0, 10.0),
      (2, 4, 1.0, 1.0),
      (3, 4, 1.0, 3.0)
    )

    val (totalFlow, totalCost, flow) = FlowAlgorithms.minCostMaxFlow[Int](edges, 0, 4)

    totalFlow should be(1)
    totalCost should be(5)
  }

  "minCostMaxFlow" should "solve max-flow in a simple trellis with null costs" in {
    val edges = Vector(
      (0, 1, 5.0, 0.0), (0, 2, 15.0, 0.0),
      (1, 3, 5.0, 0.0), (1, 4, 5.0, 0.0),
      (2, 3, 5.0, 0.0), (2, 4, 5.0, 0.0),
      (3, 5, 15.0, 0.0),
      (4, 5, 5.0, 0.0)
    )


    val (totalFlow, totalCost, flow) = FlowAlgorithms.minCostMaxFlow[Int](edges, 0, 5)

    totalFlow should be(15d)
    totalCost should be(0d)
    flow(0, 2) should be(10d)
    flow(0, 1) should be(5d)
    flow(3, 5) should be(10d)
    flow(4, 5) should be(5d)
  }
}
