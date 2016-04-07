package thymeflow.graph

import scala.annotation.tailrec

/**
  * @author David Montoya
  */
object FlowAlgorithms {

  final val INF = Double.PositiveInfinity


  /**
    * Solves the Min-cost max-flow algorithm
    * For details http://courses.csail.mit.edu/6.854/06/scribe/s12-minCostFlowAlg.pdf
    *
    * @param nodes    a traversable over the nodes in the network
    * @param capacity the capacity of an edge given a pair of from -> to nodes
    * @param cost     the cost of an edge given a pair of from -> to nodes
    * @param source   the flow's source
    * @param sink     the flow's sink
    * @tparam T the node type
    * @return
    */
  def minCostMaxFlow[T](nodes: Traversable[T],
                        capacity: (T, T) => Double,
                        cost: (T, T) => Double,
                        source: T,
                        sink: T) = {

    val flow = new scala.collection.mutable.HashMap[(T, T), Double].withDefaultValue(0d)
    val dist = new scala.collection.mutable.HashMap[T, Double].withDefaultValue(INF)
    val parent = new scala.collection.mutable.HashMap[T, T]
    val pi = new scala.collection.mutable.HashMap[T, Double].withDefaultValue(0d)

    def search(source: T, sink: T) = {
      val found = new scala.collection.mutable.HashSet[T]
      @tailrec
      def rec(optionU: Option[T]) {
        optionU match {
          case Some(u) =>
            var optionBest: Option[T] = None
            found(u) = true
            for (k <- nodes) {
              if (!found(k)) {
                if (flow((k, u)) != 0d) {
                  dist(u) match {
                    case INF =>
                    case uDist =>
                      val v = uDist + pi(u) - pi(k) - cost(k, u)
                      if (dist(k) > v) {
                        dist(k) = v
                        parent(k) = u
                      }
                  }
                }
                if (flow((u, k)) < capacity(u, k)) {
                  dist(u) match {
                    case INF =>
                    case uDist =>
                      val v = uDist + pi(u) - pi(k) + cost(u, k)
                      if (dist(k) > v) {
                        dist(k) = v
                        parent(k) = u
                      }
                  }

                }
                val distBest = optionBest.map(dist).getOrElse(INF)
                if (dist(k) < distBest) optionBest = Some(k)
              }
            }
            rec(optionBest)
          case None =>
        }
      }
      dist.clear()
      dist(source) = 0d
      rec(Some(source))
      for (k <- nodes) {
        pi(k) = Math.min(pi(k) + dist(k), INF)
      }
      found(sink)
    }

    var totalFlow = 0d
    var totalCost = 0d
    while (search(source, sink)) {
      var amt = INF
      var x = sink
      while (x != source) {
        amt = Math.min(amt, if (flow((x, parent(x))) != 0d) flow((x, parent(x))) else capacity(parent(x), x) - flow((parent(x), x)))
        x = parent(x)
      }
      x = sink
      while (x != source) {
        if (flow((x, parent(x))) != 0d) {
          flow((x, parent(x))) -= amt
          totalCost -= amt * cost(x, parent(x))
        } else {
          flow((parent(x), x)) += amt
          totalCost += amt * cost(parent(x), x)
        }
        x = parent(x)
      }
      totalFlow += amt
    }

    (totalFlow, totalCost, flow)
  }

}
