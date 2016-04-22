package thymeflow.graph

import com.typesafe.scalalogging.StrictLogging
import thymeflow.utilities.pqueue.FiedlerFibonacciHeap

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection._

/**
  * @author David Montoya
  */
private[graph] trait ShortestPathLike[NODE, EDGE_OPTIONAL, @specialized(Int, Double) W] extends StrictLogging {
  /**
    * StopCondition provides a way to terminate the algorithm at a certain
    * point, e.g.: When target becomes settled.
    */
  type StopCondition[T] = ShortestPath.StopCondition[NODE, W, T]

  def outgoing: NODE => Traversable[(EDGE_OPTIONAL, W, NODE)]

  def maximumLookupCostMinimumFactor: Option[W]

  def num: Numeric[W]

  def maximumLookupDistance: Option[W]

  def heuristic: Seq[NODE] => (NODE) => W

  def shortestPathTreeConditional[T](start: Seq[NODE], targetCondition: NODE => (Boolean, Boolean), targetsHint: Seq[NODE], getParent: (EDGE_OPTIONAL, NODE) => T) = {
    var targetsSet = new java.util.HashSet[NODE]().asScala
    val newStopCondition: StopCondition[T] = maximumLookupCostMinimumFactor match {
      case Some(factor) =>
        var lookupDistance: Option[W] = None
        (node: NODE, cost: W, settled: Set[NODE], distance: Map[NODE, W], path: Map[NODE, T]) =>
          lookupDistance match {
            case Some(max) if num.gt(cost, max) => true
            case _ =>
              val (isTarget, done) = targetCondition(node)
              if (isTarget) {
                targetsSet += node
                if (lookupDistance.isEmpty) {
                  lookupDistance = Some(num.times(cost, factor))
                }
              }
              done
          }
      case None =>
        (node: NODE, cost: W, settled: Set[NODE], distance: Map[NODE, W], path: Map[NODE, T]) =>
          val (isTarget, done) = targetCondition(node)
          if (isTarget) {
            targetsSet += node
          }
          done
    }
    val (distanceMap, spt) = shortestPathTreeBase(start, newStopCondition, getParent, targetsHint)
    (distanceMap, spt, targetsSet)
  }

  def shortestPathTree[T](start: Seq[NODE], targets: Seq[NODE], getParent: (EDGE_OPTIONAL, NODE) => T) = {
    if (targets.isEmpty) {
      (Map.empty[NODE, W], Map.empty[NODE, T])
    } else {
      var targetsSet = new java.util.HashSet[NODE](targets.asJava).asScala
      val newStopCondition: StopCondition[T] = maximumLookupCostMinimumFactor match {
        case Some(factor) =>
          var lookupDistance: Option[W] = None
          (node: NODE, cost: W, settled: Set[NODE], distance: Map[NODE, W], path: Map[NODE, T]) =>
            lookupDistance match {
              case Some(max) if num.gt(cost, max) => true
              case _ =>
                if (targetsSet.contains(node)) {
                  targetsSet -= node
                  if (lookupDistance.isEmpty) {
                    lookupDistance = Some(num.times(cost, factor))
                  }
                  targetsSet.isEmpty
                } else {
                  false
                }
            }
        case None =>
          (node: NODE, cost: W, settled: Set[NODE], distance: Map[NODE, W], path: Map[NODE, T]) =>
            if (targetsSet.contains(node)) {
              targetsSet -= node
              targetsSet.isEmpty
            } else {
              false
            }
      }
      val (distanceMap, spt) = shortestPathTreeBase(start, newStopCondition, getParent, targets)
      (distanceMap, spt)
    }
  }

  def shortestPathDistance(start: Seq[NODE], target: NODE): Option[W] = {
    val (distance, _) = shortestPathTree(start, target, (_: EDGE_OPTIONAL, from: NODE) => from)
    distance.get(target)
  }

  def shortestPathTree[T](start: Seq[NODE], target: NODE, getParent: (EDGE_OPTIONAL, NODE) => T) = {
    val newStopCondition: StopCondition[T] = (node, cost, settled, distanceMap, path) => target.equals(node)
    val (distanceMap, spt) = shortestPathTreeBase[T](start, newStopCondition, getParent, Vector(target))
    (distanceMap, spt)
  }

  def shortestPath(start: NODE, target: NODE): Option[(Path[NODE, EDGE_OPTIONAL], W)] = {
    shortestPath(List(start), target)
  }

  def shortestPath(start: Seq[NODE], targets: Seq[NODE]): Map[NODE, Option[(Path[NODE, EDGE_OPTIONAL], W)]] = {
    val (distanceMap, spt) = shortestPathTree(start, targets, (edge: EDGE_OPTIONAL, from: NODE) => (from, edge))
    targets.map {
      target =>
        target -> unfoldPath(distanceMap, target, spt)
    }(scala.collection.breakOut)
  }

  def shortestPathConditional(start: Seq[NODE], targetCondition: NODE => (Boolean, Boolean), targetsHint: Seq[NODE] = Vector()): Map[NODE, Option[(Path[NODE, EDGE_OPTIONAL], W)]] = {
    val (distanceMap, spt, targets) = shortestPathTreeConditional(start, targetCondition, targetsHint, (edge: EDGE_OPTIONAL, from: NODE) => (from, edge))
    targets.map {
      target =>
        target -> unfoldPath(distanceMap, target, spt)
    }(scala.collection.breakOut)
  }

  def shortestPath(start: Seq[NODE], target: NODE): Option[(Path[NODE, EDGE_OPTIONAL], W)] = {
    val (distanceMap, spt) = shortestPathTree(start, target, (edge: EDGE_OPTIONAL, from: NODE) => (from, edge))
    unfoldPath(distanceMap, target, spt)
  }

  def shortestNodePath(start: NODE, target: NODE): Option[(Path[NODE, (NODE, NODE)], W)] = {
    shortestNodePath(List(start), target)
  }

  def shortestNodePath(start: Seq[NODE], targets: Seq[NODE]): Map[NODE, Option[(Path[NODE, (NODE, NODE)], W)]] = {
    val (distanceMap, spt) = shortestPathTree(start, targets, (_: EDGE_OPTIONAL, from: NODE) => from)
    targets.map {
      target =>
        target -> unfoldNodePath(distanceMap, target, spt)
    }(scala.collection.breakOut)
  }

  def shortestNodePathConditional(start: Seq[NODE], targetCondition: NODE => (Boolean, Boolean), targetsHint: Seq[NODE] = Vector()): Map[NODE, Option[(Path[NODE, (NODE, NODE)], W)]] = {
    val (distanceMap, spt, targets) = shortestPathTreeConditional(start, targetCondition, targetsHint, (_: EDGE_OPTIONAL, from: NODE) => from)
    targets.map {
      target =>
        target -> unfoldNodePath(distanceMap, target, spt)
    }(scala.collection.breakOut)
  }

  def shortestNodePath(start: Seq[NODE], target: NODE): Option[(Path[NODE, (NODE, NODE)], W)] = {
    val (distanceMap, spt) = shortestPathTree(start, target, (_: EDGE_OPTIONAL, from: NODE) => from)
    unfoldNodePath(distanceMap, target, spt)
  }

  /**
    * By default the Dijkstra algorithm processes all nodes reachable from
    * <code>start</code> given to <code>compute()</code>.
    */
  protected def defaultStopCondition[T]: StopCondition[T] = (_, _, _, _, _) => false

  protected def maximumLookupDistanceStopCondition[T](lookupDistance: W): StopCondition[T] = (_, cost, _, _, _) => {
    // do not search any further if cost > maximumLookupDistance. Note that we do not use gCost here since we assume an ADMISSIBLE heuristic
    num.gt(cost, lookupDistance)
  }

  protected def shortestPathTreeBase[T](start: Seq[NODE],
                                        stopCondition: StopCondition[T] = defaultStopCondition,
                                        getParent: (EDGE_OPTIONAL, NODE) => T,
                                        heuristicTargets: Seq[NODE] = Vector()): (Map[NODE, W], Map[NODE, T])

  @tailrec
  private def unfold[PARENT, RESULT](resultVector: Vector[RESULT],
                                     vertex: NODE,
                                     spt: Map[NODE, PARENT], getResult: (PARENT, NODE) => (NODE, RESULT)): (NODE, IndexedSeq[RESULT]) = {
    spt.get(vertex) match {
      case Some(parent) =>
        val (from, result) = getResult(parent, vertex)
        unfold(result +: resultVector, from, spt, getResult)
      case None => (vertex, resultVector)
    }
  }

  private def unfoldPath(distanceMap: Map[NODE, W], target: NODE, spt: Map[NODE, (NODE, EDGE_OPTIONAL)]): Option[(Path[NODE, EDGE_OPTIONAL], W)] = {
    distanceMap.get(target).map {
      d => unfold(Vector(), target, spt, (parent: (NODE, EDGE_OPTIONAL), to: NODE) => (parent._1, (parent._2, to))) match {
        case (start, path) => (Path(start, path), d)
      }
    }
  }

  private def unfoldNodePath(distanceMap: Map[NODE, W], target: NODE, spt: Map[NODE, NODE]): Option[(Path[NODE, (NODE, NODE)], W)] = {
    distanceMap.get(target).map {
      d => unfold(Vector(), target, spt, (parent: NODE, to: NODE) => (parent, to)) match {
        case (start, path) => (Path(start +: path), d)
      }
    }
  }

}


class ShortestPath[NODE, EDGE_OPTIONAL, @specialized(Int, Double) W] private
(val outgoing: NODE => Traversable[(EDGE_OPTIONAL, W, NODE)],
 val maximumLookupDistance: Option[W],
 val maximumLookupCostMinimumFactor: Option[W],
 val num: Numeric[W] with Ordering[W],
 val heuristic: Seq[NODE] => (NODE) => W) extends ShortestPathLike[NODE, EDGE_OPTIONAL, W] {

  protected def shortestPathTreeBase[T](startNodes: Seq[NODE],
                                        stopCondition: StopCondition[T] = defaultStopCondition, getParent: (EDGE_OPTIONAL, NODE) => T,
                                        heuristicTargets: Seq[NODE] = Vector()):
  (Map[NODE, W], Map[NODE, T]) = {
    implicit val ordering = num
    val heuristic1 = if (heuristicTargets.isEmpty) {
      (_: NODE) => num.zero
    } else {
      heuristic(heuristicTargets)
    }
    // initialize PriorityQueue
    val priorityQueue = new FiedlerFibonacciHeap[NODE, W]
    // set of NODES that have been visited, we assume the heuristic is MONOTONIC
    val visited = new java.util.HashSet[NODE]().asScala
    // NODE -> Distance Map
    val distanceMap = new mutable.OpenHashMap[NODE, W]()
    // NODE -> PARENT_NODE Map in Shortest-Path Tree
    val parent = new mutable.OpenHashMap[NODE, T]()

    val priorityQueueGraphNodeToPQueueNodeMap = new mutable.OpenHashMap[NODE, FiedlerFibonacciHeap.Node[NODE, W]]()

    startNodes.foreach {
      startNode =>
        val realCost = num.zero
        distanceMap.update(startNode, realCost)
        val heuristicCost = num.plus(num.zero, heuristic1(startNode))
        priorityQueueGraphNodeToPQueueNodeMap.put(startNode, priorityQueue.insert(startNode, heuristicCost))
    }

    val stopConditionWithLookupDistance: StopCondition[T] = maximumLookupDistance match {
      case Some(lookupDistance) =>
        val lookupDistanceStopCondition = maximumLookupDistanceStopCondition[T](lookupDistance)
        (s1, s2, s3, s4, s5) => stopCondition(s1, s2, s3, s4, s5) || lookupDistanceStopCondition(s1, s2, s3, s4, s5)
      case None => stopCondition
    }

    while (!priorityQueue.isEmpty) {
      // pop NODE with minimal cost
      val state = priorityQueue.removeMin().get
      // NODE to visit = state.v
      val currentNode = state.data
      val currentNodeCost = state.key
      val currentNodeRealCost = distanceMap(currentNode)
      // remove u from the priority queue map
      priorityQueueGraphNodeToPQueueNodeMap.remove(currentNode)
      // add current node to set of visited nodes
      visited += currentNode
      // stop search if stopCondition is true.
      if (stopConditionWithLookupDistance(currentNode, currentNodeCost, visited, distanceMap, parent)) {
        return (distanceMap, parent)
      }
      for ((edge, weight, childNode) <- outgoing(currentNode) if !visited.contains(childNode)) {
        val childNodeRealCost = num.plus(currentNodeRealCost, weight)
        val (lowerThanPreviousOne, isNew) = distanceMap.get(childNode) match {
          case Some(d) => (num.gt(d, childNodeRealCost), false)
          case None => (true, true)
        }
        if (lowerThanPreviousOne) {
          distanceMap.update(childNode, childNodeRealCost)
          parent.update(childNode, getParent(edge, currentNode))
          val childNodeHeuristicCost = num.plus(childNodeRealCost, heuristic1(childNode))
          if (isNew) {
            priorityQueueGraphNodeToPQueueNodeMap.put(childNode, priorityQueue.insert(childNode, childNodeHeuristicCost))
          } else {
            val pQueueNode = priorityQueueGraphNodeToPQueueNodeMap(childNode)
            priorityQueue.decreaseKey(pQueueNode, childNodeHeuristicCost)
          }
        }
      }
    }
    (distanceMap, parent)
  }
}


object ShortestPath {

  type StopCondition[NODE, WEIGHT, T] = (NODE, WEIGHT, Set[NODE], Map[NODE, WEIGHT], Map[NODE, T]) => Boolean

  def allPairsShortestPaths[NODE, WEIGHT: Numeric](nodes: Traversable[NODE],
                                                   maximumLookupDistance: Option[WEIGHT] = None)(neighbors: NODE => Traversable[(NODE, WEIGHT)])
                                                  (stopCondition: (NODE) => StopCondition[NODE, WEIGHT, NODE]) = {
    val shortestPath = ShortestPath.explicit[NODE, Unit, WEIGHT](from => neighbors(from).map { case (to, weight) => ((), weight, to) }, maximumLookupDistance = maximumLookupDistance)
    nodes.view.map {
      case node =>
        (node, shortestPath.shortestPathTreeBase[NODE](Vector(node), stopCondition = stopCondition(node), getParent = (x: Unit, y: NODE) => y))
    }
  }

  def explicit[NODE, EDGE, WEIGHT](outgoing: NODE => Traversable[(EDGE, WEIGHT, NODE)],
                                   maximumLookupDistance: Option[WEIGHT] = None, maximumLookupCostMinimumFactor: Option[WEIGHT] = None,
                                   heuristic: Option[Seq[NODE] => ((NODE) => WEIGHT)] = None)(implicit num: Numeric[WEIGHT]) = {
    new ShortestPath(outgoing, maximumLookupDistance, maximumLookupCostMinimumFactor, num, heuristic.getOrElse((_: Seq[NODE]) => (_: NODE) => num.zero))
  }

}