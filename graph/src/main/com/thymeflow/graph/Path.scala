package com.thymeflow.graph

import scala.collection.AbstractSeq

/**
  * @author David Montoya
  */
trait Path[NODE, EDGE]
  extends AbstractSeq[(NODE, EDGE, NODE)]
    with IndexedSeq[(NODE, EDGE, NODE)] {
  def nodes: IndexedSeq[NODE]

  def edges: IndexedSeq[EDGE]

  def startNode: NODE

  def endNode: NODE

  def concat(otherPath: Path[NODE, EDGE]): Path[NODE, EDGE]

  override def take(n: Int): Path[NODE, EDGE] = {
    Path(startNode, super.take(n).map(x => (x._2, x._3)))
  }

  override def drop(n: Int): Path[NODE, EDGE] = {
    val tail = super.drop(n).map(x => (x._2, x._3))
    Path(nodes(n), tail)
  }
}

protected class NodePath[NODE](val nodes: IndexedSeq[NODE]) extends Path[NODE, (NODE, NODE)] {
  override def startNode: NODE = nodes.head

  override def edges: IndexedSeq[(NODE, NODE)] = {
    nodes.sliding(2).flatMap {
      case Seq(from, to) => Some((from, to))
      case Seq(single) => None
    }.toIndexedSeq
  }

  override def concat(otherPath: Path[NODE, (NODE, NODE)]): Path[NODE, (NODE, NODE)] = {
    require(endNode == otherPath.startNode)
    Path(nodes ++ otherPath.nodes.tail)
  }

  override def endNode: NODE = nodes.last

  override def length: Int = nodes.length - 1

  override def apply(idx: Int): (NODE, (NODE, NODE), NODE) = {
    val from = nodes.apply(idx)
    val to = nodes.apply(idx + 1)
    (from, (from, to), to)
  }

  override def take(n: Int): Path[NODE, (NODE, NODE)] = {
    Path(nodes.take(n + 1))
  }

  override def drop(n: Int): Path[NODE, (NODE, NODE)] = {
    Path(nodes.drop(n))
  }
}

protected class NodeEdgePath[NODE, EDGE](val startNode: NODE, protected val innerSeq: IndexedSeq[(EDGE, NODE)]) extends Path[NODE, EDGE] {

  override def length: Int = innerSeq.length

  def nodes: IndexedSeq[NODE] = {
    startNode +: innerSeq.map(_._2)
  }

  def edges: IndexedSeq[EDGE] = {
    innerSeq.map(_._1)
  }

  override def apply(idx: Int): (NODE, EDGE, NODE) = {
    innerSeq.apply(idx) match {
      case (e, to) =>
        val from = if (idx == 0) {
          startNode
        } else {
          innerSeq.apply(idx - 1)._2
        }
        (from, e, to)
    }
  }

  override def take(n: Int): Path[NODE, EDGE] = {
    Path(startNode, innerSeq.take(n))
  }

  override def drop(n: Int): Path[NODE, EDGE] = {
    val newStartNode = if (n == 0) {
      startNode
    } else {
      innerSeq(n - 1)._2
    }
    Path(newStartNode, innerSeq.drop(n))
  }

  def concat(otherPath: Path[NODE, EDGE]) = {
    require(endNode == otherPath.startNode)
    otherPath match {
      case other: NodeEdgePath[NODE, EDGE] => Path(startNode, innerSeq ++ other.innerSeq)
      case _ => Path(startNode, innerSeq ++ otherPath.map { x => (x._2, x._3) })
    }
  }

  def endNode: NODE = if (innerSeq.isEmpty) startNode else innerSeq.last._2
}

protected class ZeroPath[NODE, EDGE](protected val single: NODE) extends Path[NODE, EDGE] {
  override def startNode = single

  override def nodes: IndexedSeq[NODE] = Vector(single)

  override def edges: IndexedSeq[EDGE] = Vector[EDGE]()

  override def concat(otherPath: Path[NODE, EDGE]): Path[NODE, EDGE] = {
    require(endNode == otherPath.startNode)
    otherPath
  }

  override def endNode = single

  override def length: Int = 0

  override def apply(idx: Int): (NODE, EDGE, NODE) = throw new IndexOutOfBoundsException(idx.toString)

  override def take(n: Int): Path[NODE, EDGE] = {
    Path.zero(single)
  }

  override def drop(n: Int): Path[NODE, EDGE] = {
    if (n == 0) {
      Path.zero(single)
    } else {
      throw new IllegalArgumentException(s"can't drop $n edges from zero length path")
    }
  }
}


object Path {

  def apply[NODE](nodes: IndexedSeq[NODE]) = {
    require(nodes.nonEmpty)
    if (nodes.length == 1) {
      new ZeroPath[NODE, (NODE, NODE)](nodes.head)
    } else {
      new NodePath[NODE](nodes)
    }
  }

  def apply[NODE, EDGE](startNode: NODE, innerSeq: IndexedSeq[(EDGE, NODE)]) = {
    if (innerSeq.isEmpty) {
      new ZeroPath[NODE, EDGE](startNode)
    } else {
      new NodeEdgePath[NODE, EDGE](startNode, innerSeq)
    }

  }

  /** A walk of zero length that is a single node. */
  def zero[NODE, EDGE](node: NODE) =
    new ZeroPath[NODE, EDGE](node)
}
