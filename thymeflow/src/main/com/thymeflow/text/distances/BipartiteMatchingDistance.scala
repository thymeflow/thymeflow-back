package com.thymeflow.text.distances

import com.thymeflow.graph.BipartiteMatching

/**
  * @author  David Montoya
  */
class BipartiteMatchingDistance(distance: (String, String) => Double,
                                distanceThreshold: Double) {

  def getDistance(s1Tokens: IndexedSeq[String], s2Tokens: IndexedSeq[String]): Seq[(Seq[String], Seq[String], Double)] = {
    matchIndices(s1Tokens, s2Tokens).map {
      case (indices1, indices2, d) => (indices1.map(s1Tokens), indices2.map(s2Tokens), d)
    }
  }

  def matchIndices(s1Tokens: Seq[String], s2Tokens: Seq[String]): Seq[(Seq[Int], Seq[Int], Double)] = {
    if (s1Tokens.nonEmpty && s2Tokens.nonEmpty) {
      val distanceMatrix = s1Tokens.map {
        case u =>
          s2Tokens.map {
            case v =>
              distance(u, v)
          }
      }
      val (matching, _) = BipartiteMatching.extractMatching(distanceMatrix)
      val resultBuilder = IndexedSeq.newBuilder[(Seq[Int], Seq[Int], Double)]
      val matchedIndexes1Builder = IndexedSeq.newBuilder[Int]
      val matchedIndexes2Builder = IndexedSeq.newBuilder[Int]
      matching.zipWithIndex.collect {
        case (v, u) if v != -1 && (distanceMatrix(u)(v) <= distanceThreshold) =>
          matchedIndexes1Builder += u
          matchedIndexes2Builder += v
          resultBuilder += ((IndexedSeq(u), IndexedSeq(v), distanceMatrix(u)(v)))
        case _ =>
      }
      val matchedIndexes1 = matchedIndexes1Builder.result()
      if (matchedIndexes1.size < math.min(s1Tokens.size, s2Tokens.size)) {
        // if not all indexes have been matched
        // build a sorted seq of indexes that have not been matched
        val remaining1 = (s1Tokens.indices.toSet -- matchedIndexes1).toIndexedSeq.sorted
        val remaining2 = (s2Tokens.indices.toSet -- matchedIndexes2Builder.result).toIndexedSeq.sorted
        // compute distance between remaining terms when combined into a single one
        val remainingDistance = distance(remaining1.map(s1Tokens).mkString(" "), remaining2.map(s2Tokens).mkString(" "))
        if (remainingDistance <= distanceThreshold) {
          resultBuilder += ((remaining1, remaining2, remainingDistance))
        }
      }
      resultBuilder.result()
    } else {
      IndexedSeq()
    }
  }
}
