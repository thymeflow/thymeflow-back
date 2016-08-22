package com.thymeflow.graph

import scala.collection.mutable

/**
  * @author  David Montoya
  */
object ConnectedComponents {
  def compute[NODE](nodes: Traversable[NODE], neighbors: NODE => Traversable[NODE]): IndexedSeq[Set[NODE]] = {
    val processed = mutable.HashSet.empty[NODE]
    val components = mutable.Queue.empty[Set[NODE]]


    // Breadth-first search
    def bfs(start: NODE): Unit = {
      val component = mutable.HashSet.empty[NODE]
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
    components.toIndexedSeq
  }

}
