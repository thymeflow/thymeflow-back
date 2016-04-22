package thymeflow.location.treillis

import java.time.Duration

import com.typesafe.scalalogging.StrictLogging
import thymeflow.graph.ShortestPath
import thymeflow.spatial.geographic
import thymeflow.utilities.TimeExecution

import scala.collection.JavaConverters._
import scala.collection.{Traversable, mutable}

/**
  * @author David Montoya
  */

trait StateEstimator extends StrictLogging {

  def distance(from: geographic.Point, to: geographic.Point): Double

  def estimate(observations: IndexedSeq[(Observation, Option[ClusterObservation])],
               clusters: IndexedSeq[ClusterObservation],
               lookupDuration: Duration) = {
    implicit val observationDeserializer = (index: Int) => observations(index)._1
    implicit val clusterDeserializer = (index: Int) => clusters(index)
    implicit val observationSerializer = (observation: Observation) => observation.index
    implicit val clusterSerializer = (clusterObservation: ClusterObservation) => clusterObservation.index
    val lastIndex = observations.indices.last
    def outgoingLambda(progress: Long => Unit): ((Int, Long)) => Traversable[(Unit, Double, State)] = {
      case (stateSerialized@(index: Int, _: Long)) =>
        val fromState = State.deserialize(stateSerialized)
        val nextIndex = index + 1
        progress(nextIndex)
        if (nextIndex > lastIndex) {
          Vector.empty
        } else {
          val (nextObservation, nextObservationCluster) = observations(nextIndex)
          val previousObservationCluster = observations(index)._2
          StateGenerator.generator(fromState,
            previousObservationCluster,
            nextObservation,
            nextObservationCluster,
            distance,
            lookupDuration
          )
        }
    }

    val initial = State.serialize(SamePosition(observations.head._1))

    val singleNodes = TimeExecution.timeProgress("state-estimator-simplification", observations.length, logger, {
      case progress =>
        val outgoing = outgoingLambda(progress)(_: (Int, Long)).view.map {
          case (edge, weight, state) => state.serialize
        }
        findSingleNodes(initial, outgoing, (x: (Int, Long)) => x._1)
    })

    val targetNodes = if (singleNodes.last == lastIndex) singleNodes else singleNodes :+ lastIndex

    TimeExecution.timeProgress("state-estimator", observations.length, logger, {
      case progress =>
        val outgoing = outgoingLambda(progress)(_: (Int, Long)).view.map {
          case (edge, weight, state) => (edge, weight, state.serialize)
        }
        val sp = ShortestPath.explicit[(Int, Long), Unit, Double](outgoing = outgoing)
        targetNodes.foldLeft(Some(Vector(initial)): Option[Vector[(Int, Long)]]) {
          case (Some(pathAccumulator), targetNodeIndex) =>
            val result = sp.shortestNodePathConditional(Vector(pathAccumulator.last), x => {
              val isTarget = x._1 == targetNodeIndex
              (isTarget, isTarget)
            }).headOption.flatMap(_._2).map {
              case (p, _) => p.nodes.tail
            }
            if (result.isEmpty) {
              logger.warn(s"Found empty path from ${pathAccumulator.last} to $targetNodeIndex")
            }
            result.map(pathAccumulator ++ _)
          case _ => None
        }.map(_.map(State.deserialize))
    })
  }

  def findSingleNodes[N](startingNode: N,
                         neighbors: N => Traversable[N],
                         exploredCounter: N => Int): Vector[Int] = {
    val explored = new java.util.HashSet[N]().asScala
    val toExplore = new mutable.Queue[N]()
    toExplore += startingNode
    explored.add(startingNode)
    var counter = exploredCounter(startingNode)
    var i = 0
    val singleNodesBuilder = Vector.newBuilder[Int]
    while (toExplore.nonEmpty) {
      val currentNode = toExplore.dequeue()
      val currentCounter = exploredCounter(currentNode)
      if (currentCounter > counter) {
        if (i == 1) {
          singleNodesBuilder += counter
        }
        counter = currentCounter
        explored --= explored.collect {
          case c if exploredCounter(c) < counter => c
        }
        i = 0
      }
      i += 1
      neighbors(currentNode).foreach {
        (neighbor) =>
          if (!explored.contains(neighbor)) {
            explored.add(neighbor)
            toExplore += neighbor
          }
      }
    }
    val singleNodes = singleNodesBuilder.result()
    logger.info(s"Found ${singleNodes.size} single nodes (counter=$counter), max=${
      singleNodes.sliding(2).map {
        case Vector(s1, s2) => s2 - s1
        case _ => 0
      }.max
    }")
    singleNodes
  }
}
