package thymeflow.graph

import scala.collection.mutable

/**
  * @author David Montoya
  */
object FlowAlgorithms {

  final val INF = Double.PositiveInfinity

  /**
    * Solves the min-cost max-flow problem using potentials.
    * Cannot handle negative edge costs.
    *
    * @param edges  the edges of this network, (from, to, capacity, cost)
    * @param source the flow's source
    * @param sink   the flow's sink
    * @tparam NODE the node type
    * @return (totalFlow, totalCost, flow) where flow maps an edge (from, to) to its flow value
    */
  def minCostMaxFlow[NODE](edges: Traversable[(NODE, NODE, Double, Double)],
                           source: NODE,
                           sink: NODE,
                           maxFlow: Double = Double.PositiveInfinity) = {
    val priority = new mutable.HashMap[NODE, Double].withDefaultValue(INF)
    val currentFlow = new mutable.HashMap[NODE, Double].withDefaultValue(INF)
    val flow = new mutable.HashMap[(NODE, NODE), Double].withDefaultValue(0d)
    val parent = new mutable.HashMap[NODE, (NODE, Double)]
    val potential = new mutable.HashMap[NODE, Double].withDefaultValue(0d)
    val positiveNeighbors = edges.groupBy(_._1).mapValues {
      case g => g.map {
        case e@(_, v, capacity, cost) =>
          if (cost < 0d) throw new IllegalArgumentException(s"Expected non-negative edge costs. Found ($e).")
          (v, capacity, cost)
      }
    }
    val negativeNeighbors = edges.groupBy(_._2).mapValues {
      case g => g.map {
        case e@(u, _, _, cost) =>
          (u, 0d, -cost)
      }
    }
    val neighborMap = (positiveNeighbors.keySet ++ negativeNeighbors.keySet).map {
      case (node) => node -> (positiveNeighbors.getOrElse(node, Vector.empty) ++ negativeNeighbors.getOrElse(node, Vector.empty)).toVector
    }.toMap
    val neighbors = neighborMap.getOrElse(_: NODE, Vector.empty)
    var totalFlow = 0d
    var totalCost = 0d
    val ordering = new Ordering[(NODE, Double)] {
      override def compare(x: (NODE, Double), y: (NODE, Double)): Int = {
        y._2.compareTo(x._2)
      }
    }
    val q = new mutable.PriorityQueue[(NODE, Double)]()(ordering)
    var b = true
    while (b && totalFlow < maxFlow) {
      q.clear()
      priority.clear()
      q.enqueue((source, 0d))
      priority(source) = 0d
      val finished = new mutable.HashSet[NODE]
      currentFlow(source) = INF
      while (!finished.contains(sink) && q.nonEmpty) {
        val (u, uPriority) = q.dequeue()
        if (uPriority == priority(u)) {
          finished += u
          for ((v, capacity, cost) <- neighbors(u) if !finished.contains(v)) {
            val f = flow((u, v))
            if (f < capacity) {
              // due to numerical precision loss, and since costs are non-negative,
              // we explicitly ensure that newPriority is >= uPriority
              val newPriority = Math.max(uPriority + (cost + (potential(u) - potential(v))), uPriority)
              if (priority(v) > newPriority) {
                priority(v) = newPriority
                q.enqueue((v, newPriority))
                parent(v) = (u, cost)
                currentFlow(v) = Math.min(currentFlow(u), capacity - f)
              }
            }
          }
        }
      }
      if (priority(sink) != INF) {
        for (k <- finished) {
          potential(k) += priority(k) - priority(sink)
        }
        val dFlow = Math.min(currentFlow(sink), maxFlow - totalFlow)
        totalFlow += dFlow
        var v = sink
        while (v != source) {
          val (u, cost) = parent(v)
          flow((u, v)) += dFlow
          flow((v, u)) -= dFlow
          totalCost += dFlow * cost
          v = u
        }
      } else {
        b = false
      }
    }
    (totalFlow, totalCost, flow.getOrElse(_: (NODE, NODE), 0d))
  }


}
