package thymeflow.location.treillis

import java.time.Duration

import com.typesafe.scalalogging.StrictLogging
import thymeflow.graph.ShortestPath
import thymeflow.spatial.geographic

import scala.collection.Traversable

/**
  * @author David Montoya
  */

trait StateEstimator extends StrictLogging {

  def distance(from: geographic.Point, to: geographic.Point): Double

  def estimate[OBSERVATION <: Observation,
  CLUSTER_OBSERVATION <: ClusterObservation](observations: IndexedSeq[(OBSERVATION, Option[CLUSTER_OBSERVATION])],
                                             lookupDuration: Duration) = {
    val clusterToIndexMap = new scala.collection.mutable.HashMap[CLUSTER_OBSERVATION, Int]
    val clustersBuilder = IndexedSeq.newBuilder[CLUSTER_OBSERVATION]
    var clusterIndex = 0
    observations.foreach {
      case (_, Some(cluster)) =>
        if (!clusterToIndexMap.contains(cluster)) {
          clusterToIndexMap += cluster -> clusterIndex
          clustersBuilder += cluster
          clusterIndex += 1
        }
      case _ =>
    }
    val clusters = clustersBuilder.result()
    implicit val observationDeserializer = (index: Int) => observations(index)._1
    implicit val clusterDeserializer = (index: Int) => clusters(index)
    implicit val clusterSeerializer = (cluster: CLUSTER_OBSERVATION) => clusterToIndexMap(cluster)
    val lastIndex = observations.indices.last
    val outgoingLambda = (stateSerialized: (Int, Long)) => {
      val fromState = State.deserialize(stateSerialized)
      val index = fromState.observationIndex
      val nextIndex = index + 1
      if (nextIndex > lastIndex) {
        Traversable.empty
      } else {
        val (nextObservation, nextObservationCluster) = observations(nextIndex)
        val previousObservationCluster = observations(index)._2
        StateGenerator.generator(fromState,
          previousObservationCluster,
          nextIndex,
          nextObservation,
          nextObservationCluster,
          distance,
          lookupDuration
        )
      }
    }

    val initial = State.serialize(SamePosition[OBSERVATION, CLUSTER_OBSERVATION](0, observations.head._1))
    val targetNodeIndex = lastIndex


    val outgoing = outgoingLambda(_: (Int, Long)).view.map {
      case (edge, weight, state) => (edge, weight, state.serialize)
    }
    val sp = ShortestPath.explicit[(Int, Long), Unit, Double](outgoing = outgoing)
    val result = sp.shortestNodePathConditional(Vector(initial), x => {
      val isTarget = x._1 == targetNodeIndex
      (isTarget, isTarget)
    }).headOption.flatMap(_._2).map {
      case (p, _) => p.nodes.tail
    }
    if (result.isEmpty) {
      logger.warn(s"[location-single-nodes] - Found empty path to $targetNodeIndex")
    }
    result.map(_.map(State.deserialize(_)(observationDeserializer, clusterDeserializer)))
  }


  def findSingleNodes[OBSERVATION <: Observation,
  CLUSTER_OBSERVATION <: ClusterObservation](lookupDuration: Duration, out: (IndexedSeq[(OBSERVATION, Option[CLUSTER_OBSERVATION])]) => Unit) = {
    def initial(x: (OBSERVATION, Option[CLUSTER_OBSERVATION])) = SamePosition[OBSERVATION, CLUSTER_OBSERVATION](0, x._1)
    def neighbors(state: State[OBSERVATION, CLUSTER_OBSERVATION],
                  previousObservation: (OBSERVATION, Option[CLUSTER_OBSERVATION]),
                  observation: (OBSERVATION, Option[CLUSTER_OBSERVATION])) = {
      StateGenerator.generator(state,
        previousObservation._2,
        state.observationIndex + 1,
        observation._1,
        observation._2,
        distance,
        lookupDuration
      ).map(_._3)
    }
    findSingleNodesBase(out, initial, neighbors)
  }

  private def findSingleNodesBase[OBSERVATION, STATE](out: IndexedSeq[OBSERVATION] => Unit,
                                                      initial: OBSERVATION => STATE,
                                                      neighbors: (STATE, OBSERVATION, OBSERVATION) => Traversable[STATE]) = {
    var inputs: IndexedSeq[OBSERVATION] = IndexedSeq.empty
    var currentNodes: Set[STATE] = Set()
    var nodesCounter = 0
    var singleNodesCounter = 0
    var maxLengthBetweenSingleNodes = 0
    def flushNodes() = {
      if (inputs.nonEmpty) {
        out(inputs)
        maxLengthBetweenSingleNodes = Math.max(inputs.size, maxLengthBetweenSingleNodes)
        inputs = IndexedSeq(inputs.last)
        singleNodesCounter += 1
      }
    }
    def onInput(input: OBSERVATION) = {
      inputs.lastOption match {
        case Some(previousInput) =>
          if (currentNodes.size == 1) {
            flushNodes()
          }
          currentNodes = currentNodes.flatMap {
            case node =>
              neighbors(node, previousInput, input)
          }
        case None =>
          currentNodes = Set(initial(input))
      }
      if (currentNodes.isEmpty) {
        logger.warn(s"[location-single-nodes] - found no nodes at $input")
        flushNodes()
      } else {
        inputs = inputs :+ input
      }
      nodesCounter += 1
    }
    def onFinish() = {
      flushNodes()
      logger.debug(s"[location-single-nodes] - Found $singleNodesCounter single nodes {totalNodes=$nodesCounter, maxLengthBetweenSingleNodes=$maxLengthBetweenSingleNodes}.")
    }
    (onInput _, onFinish _)
  }
}
