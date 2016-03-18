package thymeflow.graph

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * @author  David Montoya
  */
object ConnectedComponents {
  def compute[NODE](nodes: Traversable[NODE], neighbors: NODE => Traversable[NODE]): Seq[Set[NODE]] = {
    val processed = new java.util.HashSet[NODE].asScala
    val components = mutable.Queue.empty[Set[NODE]]


    // Breadth-first search
    def bfs(start: NODE): Unit = {
      val component = new java.util.HashSet[NODE].asScala
      val bfsStack = mutable.Stack[NODE]()
      processed += start
      bfsStack push start
      while (bfsStack.nonEmpty) {
        val u = bfsStack.pop()
        component += u
        for (v <- neighbors(u); if !processed.contains(v)) {
          processed += v
          bfsStack push v
        }
      }
      components += component.toSet
    }

    // perform BFS from every vertex in the graph.
    for {
      u <- nodes if !(processed contains u)
    } bfs(u)
    components.toSeq
  }

}
