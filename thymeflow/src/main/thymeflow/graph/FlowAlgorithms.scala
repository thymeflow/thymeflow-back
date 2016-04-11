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
    * @tparam T the node type
    * @return (totalFlow, totalCost, flow) where flow maps an edge (from, to) to its flow value
    */
  def minCostMaxFlow[T](edges: Traversable[(T, T, Double, Double)],
                        source: T,
                        sink: T,
                        maxFlow: Double = Double.PositiveInfinity) = {
    val priority = new mutable.HashMap[T, Double].withDefaultValue(INF)
    val currentFlow = new mutable.HashMap[T, Double].withDefaultValue(INF)
    val flow = new mutable.HashMap[(T, T), Double].withDefaultValue(0d)
    val parent = new mutable.HashMap[T, (T, Double)]
    val potential = new mutable.HashMap[T, Double].withDefaultValue(0d)
    val positiveNeighbors = edges.groupBy(_._1).mapValues {
      case g => g.map {
        case e@(u, v, capacity, cost) =>
          if (cost < 0d) throw new IllegalArgumentException(s"Expected non-negative edge costs. Found ($e).")
          (v, capacity, cost)
      }
    }.getOrElse(_: T, Vector.empty)
    val negativeNeighbors = edges.groupBy(_._2).mapValues {
      case g => g.map {
        case e@(u, v, _, cost) =>
          (u, 0d, -cost)
      }
    }.getOrElse(_: T, Vector.empty)
    def allNeighbors(node: T) = {
      positiveNeighbors(node).view ++ negativeNeighbors(node).seq
    }
    var totalFlow = 0d
    var totalCost = 0d
    val ordering = new Ordering[(T, Double)] {
      override def compare(x: (T, Double), y: (T, Double)): Int = {
        y._2.compareTo(x._2)
      }
    }
    val q = new mutable.PriorityQueue[(T, Double)]()(ordering)
    var b = true
    while (b && totalFlow < maxFlow) {
      q.clear()
      priority.clear()
      q.enqueue((source, 0d))
      priority(source) = 0d
      val finished = new mutable.HashSet[T]
      currentFlow(source) = INF
      while (!finished.contains(sink) && q.nonEmpty) {
        val (u, uPriority) = q.dequeue()
        if (uPriority == priority(u)) {
          finished += u
          for ((v, capacity, cost) <- allNeighbors(u)) {
            val f = flow((u, v))
            if (f < capacity) {
              val newPriority = uPriority + cost + potential(u) - potential(v)
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
    (totalFlow, totalCost, flow.getOrElse(_: (T, T), 0d))
  }


}
