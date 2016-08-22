package com.thymeflow.enricher.entityresolution

import java.util.Locale

import com.thymeflow.text.distances.BipartiteMatchingDistance
import com.thymeflow.utilities.Memoize
import com.thymeflow.utilities.text.Normalization
import org.apache.lucene.search.spell.{JaroWinklerDistance, LevensteinDistance}

/**
  * @author David Montoya
  */
trait EntityResolution {

  protected val normalizeTerm = Memoize.concurrentFifoCache(1000, uncachedNormalizeTerm _)
  /**
    * term sequence matching functions, used to match "Alice Wonders" with "Wondrs Alice"
    */
  protected val (getMatchingTermSimilarities, getMatchingTermIndicesWithSimilarities, getMatchingTermIndices) = {
    val bipartiteMatchingDistance = new BipartiteMatchingDistance((s1, s2) => 1d - termSimilarity(s1, s2), matchDistanceThreshold)
    def getIndicesWithSimilarities(terms1: IndexedSeq[String], terms2: IndexedSeq[String]) = {
      bipartiteMatchingDistance.matchIndices(terms1, terms2).map {
        case (subTerms1, subTerms2, distance) => (subTerms1, subTerms2, 1d - distance)
      }
    }
    def getSimilarities(terms1: IndexedSeq[String], terms2: IndexedSeq[String]) = {
      bipartiteMatchingDistance.getDistance(terms1, terms2).map {
        case (subTerms1, subTerms2, distance) => (subTerms1, subTerms2, 1d - distance)
      }
    }
    def getIndices(terms1: IndexedSeq[String], terms2: IndexedSeq[String]) = {
      bipartiteMatchingDistance.matchIndices(terms1, terms2).map {
        case (subTerms1, subTerms2, distance) => (subTerms1, subTerms2)
      }
    }
    (getSimilarities _, getIndicesWithSimilarities _, getIndices _)
  }
  // \u2022 is the bullet character
  private val tokenSeparator =
    """[\p{Punct}\s\u2022]+""".r

  protected def baseStringSimilarity: EntityResolution.StringSimilarity

  protected def matchDistanceThreshold: Double

  /**
    *
    * @param terms1                     term sequence 1
    * @param terms2                     term sequence 2
    * @param termIDFs                   term IDFs
    * @param termSimilarityMatchIndices a function for matching term sequences
    * @return the equality probability of the two term sequences
    */
  protected def getNameTermsEqualityProbability(terms1: IndexedSeq[(String, Double)],
                                                terms2: IndexedSeq[(String, Double)],
                                                termIDFs: String => Double,
                                                termSimilarityMatchIndices: (IndexedSeq[String], IndexedSeq[String]) => Seq[(Seq[Int], Seq[Int], Double)]) = {
    if (terms1.nonEmpty && terms2.nonEmpty) {
      val similarityIndices = termSimilarityMatchIndices(terms1.map(_._1), terms2.map(_._1))
      def termsTFIDFs(terms: IndexedSeq[(String, Double)]) = (index: Int) => {
        val (term, weight) = terms(index)
        weight * termIDFs(term)
      }
      normalizedSoftTFIDF(termsTFIDFs(terms1), termsTFIDFs(terms2))(terms1.indices, terms2.indices, similarityIndices)
    } else {
      0d
    }
  }

  /**
    *
    * @param text1TFIDF tf-idf for the first text
    * @param text2TFIDF tf-idf for the second text
    * @tparam T the term type
    * @return the distance between two text sequences using cosine similarity over their TFIDF space
    */
  private def normalizedSoftTFIDF[T](text1TFIDF: T => Double, text2TFIDF: T => Double) = (text1: Seq[T], text2: Seq[T], similarities: Seq[(Seq[T], Seq[T], Double)]) => {
    val denominator = text1.map(text1TFIDF).sum + text2.map(text2TFIDF).sum
    if (denominator == 0d) {
      0d
    } else {
      val numerator = similarities.map {
        case (terms1, terms2, similarity) =>
          (terms1.map(text1TFIDF).sum + terms2.map(text2TFIDF).sum) * similarity
      }.sum
      Math.min(numerator / denominator, 1d)
    }
  }

  protected def getNamesEqualityProbability[RESOURCE](names1: Traversable[(String, Double)],
                                                      names2: Traversable[(String, Double)],
                                                      termIDFs: String => Double,
                                                      termSimilarityMatch: (IndexedSeq[String], IndexedSeq[String]) => Seq[(Seq[String], Seq[String], Double)]) = {
    var weight = 0d
    var normalization = 0d
    names1.foreach {
      case (name1, name1Weight) =>
        names2.foreach {
          case (name2, name2Weight) =>
            val terms1 = extractTerms(name1)
            val terms2 = extractTerms(name2)
            if (terms1.nonEmpty && terms2.nonEmpty) {
              val similarities = termSimilarityMatch(terms1, terms2)
              val maxWeight = normalizedSoftTFIDF(termIDFs, termIDFs)(terms1, terms2, similarities)
              weight += (name1Weight * name2Weight) * maxWeight
              normalization += (name1Weight * name2Weight)
            }
        }
    }
    val equalityProbability = if (normalization != 0.0) {
      Math.min(weight / normalization, 1d)
    } else {
      0.0
    }
    equalityProbability
  }

  /**
    *
    * @param content to extract terms from
    * @return a list of extracted terms, in their order of appearance
    */
  protected def extractTerms(content: String) = {
    tokenSeparator.split(content).toIndexedSeq.filter(_.nonEmpty)
  }

  /**
    * Computes term inverse document frequencies (IDFs) from a collection of documents
    *
    * @param documents a collection of documents, each document is given by a list of terms, and their respective appearance probability (between 0 and 1)
    * @return a TERM -> IDF map
    */
  protected def computeTermIDFs(documents: Traversable[Traversable[(String, Double)]]) = {
    val n = documents.size
    val idfs = documents.flatten.groupBy(_._1).map {
      case (term, terms) =>
        term -> math.log(n / terms.map(_._2).sum)
    }
    idfs
  }

  /**
    * Parses a token list out of some text by splitting at separators
    *
    * @param content the text to parse
    * @return a list of matched tokens (Left) and separators (Right)
    */
  protected def entitySplitParts(content: String) = {
    var index = 0
    val parts = Vector.newBuilder[Either[String, String]]
    for (m <- tokenSeparator.findAllMatchIn(content)) {
      if (index != m.start) {
        parts += Left(content.substring(index, m.start))
      }
      parts += Right(m.matched)
      index = m.end
    }
    if (index != content.length) {
      parts += Left(content.substring(index, content.length))
    }
    parts.result()
  }

  /** *
    * Normalizes terms by removing their accents (diacritical marks) and changing them to lower case
    *
    * @param term term to normalize
    * @return normalized term
    */
  private def uncachedNormalizeTerm(term: String) = {
    Normalization.removeDiacriticalMarks(term).toLowerCase(Locale.ROOT)
  }

  /**
    * term similarity using the Levenstein distance
    */
  private def termSimilarity(term1: String, term2: String) = {
    baseStringSimilarity(normalizeTerm(term1), normalizeTerm(term2))
  }
}

object EntityResolution {

  trait StringSimilarity {
    def apply(s1: String, s2: String): Double
  }

  object LevensteinSimilarity extends StringSimilarity {
    private val base = new LevensteinDistance

    override def toString = "LevensteinSimilarity"

    override def apply(s1: String, s2: String): Double = base.getDistance(s1, s2)
  }

  object JaroWinklerSimilarity extends StringSimilarity {
    private val base = new JaroWinklerDistance

    override def toString = "JaroWinklerSimilarity"

    override def apply(s1: String, s2: String): Double = base.getDistance(s1, s2)
  }

}