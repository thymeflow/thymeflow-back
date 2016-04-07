package thymeflow.text.alignment

import thymeflow.graph.FlowAlgorithms

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * @author David Montoya
  */
object Alignment {

  final val spaceScore = -1.0
  final val matchScore = 1.0
  final val mismatchScore = -1.0

  def alignment(queries: Seq[String],
                text: String,
                filter: (Double, Int, Int) => Boolean = (_, _, _) => true) = {
    var id = 0
    val graph = new scala.collection.mutable.HashMap[(AlignmentNode, AlignmentNode), (Double, Double)]
    val candidates = queries.collect {
      case query if query.nonEmpty =>
        id += 1
        val queryTerm = new QueryTerm(id, query)
        graph((Source, queryTerm)) = (1d, 0d)
        val matches = find(query, text, filter).map {
          case (queryRange, textRange, score1, score2) =>
            id += 1
            val queryTermMatch = new QueryTermMatch(id, query.substring(queryRange._1, queryRange._2 + 1))
            graph((queryTerm, queryTermMatch)) = (1d, (query.length - queryTermMatch.term.length).toDouble / query.length.toDouble)
            (queryTermMatch, textRange, score1, score2)
        }
        (queryTerm, matches)
    }
    val textMatches = candidates.flatMap(_._2.flatMap {
      case (_, (from, to), _, _) =>
        Vector(from, to + 1)
    }).distinct.sorted.sliding(2, 1).map {
      case Seq(from, to) =>
        id += 1
        new TextMatch(id, from, to - 1, text.substring(from, to))
    }.toVector
    val usedTextMatches = candidates.flatMap {
      case (_, termMatches) =>
        termMatches.flatMap {
          case (termMatch, (from, to), _, _) =>
            val includedTextMatches = textMatches.collect {
              case textMatch if textMatch.from <= to && from <= textMatch.to =>
                (textMatch, textMatch.to - textMatch.from + 1)
            }
            val totalWeight = includedTextMatches.map(_._2).sum.toDouble
            includedTextMatches.map {
              case (textMatch, weight) =>
                graph((termMatch, textMatch)) = (weight.toDouble / totalWeight, 1d)
                textMatch
            }
        }
    }.distinct

    usedTextMatches.foreach {
      case textMatch => graph((textMatch, Target)) = (1d, 0d)
    }

    val nodes = graph.keys.flatMap(x => Vector(x._1, x._2)).toVector.distinct

    val (_, _, flow) = FlowAlgorithms.minCostMaxFlow[AlignmentNode](
      nodes, { case (from, to) => graph.get((from, to)).map(_._1).getOrElse(0) }, { case (from, to) => graph.get((from, to)).map(_._2).getOrElse(0d) },
      Source,
      Target
    )
    val result = candidates.map {
      case (queryTerm, _) =>
        @tailrec
        def bfs(fromNodes: Vector[AlignmentNode]): Vector[AlignmentNode] = {
          if (fromNodes.exists(_.isInstanceOf[TextMatch])) {
            fromNodes
          } else {
            bfs(fromNodes.flatMap {
              case from =>
                nodes.collect {
                  case to if flow(from, to) > 0d => to
                }
            }.distinct)
          }
        }
        (queryTerm, bfs(Vector(queryTerm)))
    }
    result
  }

  def find(query: String, text: String, filter: (Double, Int, Int) => Boolean = (_, _, _) => true) = {
    val rowSequence = RowSequence(query)
    val columnSequence = ColumnSequence(text)
    def getInitialScore(cell: Cell) = {
      if (cell.column == 0) {
        Some((cell.row * spaceScore, 0, cell.row))
      } else {
        None
      }
    }
    def getInitialPrevious(cell: Cell) = {
      if (cell.column == 0 && cell.row != 0) {
        Some(CellAbove)
      } else {
        None
      }
    }
    val (rows, cols, score, previous) = sequenceAlignment(rowSequence, columnSequence, positiveScores = false, getInitialScore, getInitialPrevious)
    val iterator = for (i <- Iterator.range(0, rows); j <- Iterator.range(0, cols)) yield {
      val cell = Cell(i, j)
      cell -> score(cell)
    }
    val orderedScores = iterator.toIndexedSeq.sortBy(_._2._1).reverse
    alignCandidates(previous, orderedScores, filter)(rowSequence, columnSequence)
  }

  def alignCandidates(previous: Cell => Option[Direction],
                      orderedScores: IndexedSeq[(Cell, (Double, Int, Int))],
                      filter: (Double, Int, Int) => Boolean)(implicit rowSequence: RowSequence, columnSequence: ColumnSequence) = {
    val scanned = new mutable.HashMap[Cell, Int]
    var bestRanges = Vector[((Int, Int), (Int, Int))]()
    val best = Vector.newBuilder[((Int, Int), (Int, Int), Double, Double)]

    orderedScores.foreach {
      case (cell, (score, matches, mismatches)) if score > 0.0 =>
        @tailrec
        def scanPrevious(currentCell: Cell,
                         currentLength: Int = 0): Option[Cell] = {
          scanned.get(currentCell) match {
            case Some(length) =>
              if (length >= currentLength) {
                return None
              }
            case None =>
              scanned(currentCell) = currentLength
          }
          previous(currentCell) match {
            case Some(direction) =>
              val previousCell = direction match {
                case CellLeft =>
                  currentCell.left
                case CellAboveLeft =>
                  currentCell.aboveLeft
                case CellAbove =>
                  currentCell.above
              }
              scanPrevious(previousCell, currentLength + 1)
            case None =>
              Some(currentCell)
          }
        }
        if (!scanned.contains(cell) && previous(cell).contains(CellAboveLeft) && filter(score, matches, mismatches)) {
          scanPrevious(cell).foreach {
            case (firstCell) =>
              val rowRange = (firstCell.row, cell.row - 1)
              val colRange = (firstCell.column, cell.column - 1)
              if (!bestRanges.exists {
                case (bestRowRange, bestColRange) =>
                  (rowRange._2 <= bestRowRange._2 && bestRowRange._1 <= rowRange._1) &&
                    (colRange._2 <= bestColRange._2 && bestColRange._1 <= colRange._1)
              }) {
                best += ((rowRange, colRange, score, matches.toDouble / (matches + mismatches).toDouble))
                bestRanges +:=(rowRange, colRange)
              }
          }
        }
      case _ =>
    }
    best.result()
  }

  private def sequenceAlignment(rowSequence: RowSequence,
                                columnSequence: ColumnSequence,
                                positiveScores: Boolean = true,
                                getInitialScore: (Cell) => Option[(Double, Int, Int)] = _ => None,
                                getInitialPrevious: (Cell) => Option[Direction] = _ => None) = {
    val nRows = rowSequence.text.length + 1
    val nCols = columnSequence.text.length + 1
    val scores: Array[Array[(Double, Int, Int)]] = Array.ofDim(nRows, nCols)
    (Iterator.range(0, nRows).map((_, 0)) ++ Iterator.range(1, nCols).map((0, _))).foreach {
      case (row, col) =>
        scores(row)(col) = getInitialScore(Cell(row, col)).getOrElse((0.0, 0, 0))
    }

    val previousMap = new mutable.HashMap[Cell, Direction]
    def getScore(cell: Cell) = {
      val score = scores(cell.row)(cell.column)
      if (score == null) {
        (0.0, 0, 0)
      } else {
        score
      }
    }
    def setScore(cell: Cell, score: (Double, Int, Int)) = scores(cell.row)(cell.column) = score
    def setPrevious(cell: Cell, previous: Direction) = previousMap(cell) = previous
    for (row <- Iterator.range(1, nRows); column <- Iterator.range(1, nCols)) {
      val currentCell = Cell(row, column)
      fillInCell(currentCell, getScore, setScore, setPrevious, positiveScores)(rowSequence, columnSequence)
    }
    def getPrevious(cell: Cell) = {
      getInitialPrevious(cell) match {
        case None => previousMap.get(cell)
        case previous => previous
      }
    }
    (nRows, nCols, getScore _, getPrevious _)
  }

  private def fillInCell(currentCell: Cell,
                         getScore: Cell => (Double, Int, Int),
                         setScore: (Cell, (Double, Int, Int)) => Unit,
                         setPrevious: (Cell, Direction) => Unit,
                         positiveScores: Boolean)(implicit rowSequence: RowSequence, columnSequence: ColumnSequence) {
    val cellAbove = currentCell.above
    val cellLeft = currentCell.left
    val cellAboveLeft = currentCell.aboveLeft
    val rowSpaceScore = getScore(cellAbove) match {
      case (score, matches, nonMatches) => (score + spaceScore, matches, nonMatches + 1)
    }
    val colSpaceScore = getScore(cellLeft) match {
      case (score, matches, nonMatches) => (score + spaceScore, matches, nonMatches + 1)
    }
    var matchOrMismatchScore = getScore(cellAboveLeft)
    val rowChar = currentCell.rowChar
    if (rowChar == currentCell.columnChar) {
      matchOrMismatchScore = matchOrMismatchScore match {
        case (score, matches, nonMatches) => (score + matchScore, matches + 1, nonMatches)
      }
    } else {
      matchOrMismatchScore = matchOrMismatchScore match {
        case (score, matches, nonMatches) => (score + mismatchScore, matches, nonMatches + 1)
      }
    }
    if (rowSpaceScore._1 >= colSpaceScore._1) {
      if (matchOrMismatchScore._1 >= rowSpaceScore._1) {
        if (matchOrMismatchScore._1 > 0 || !positiveScores) {
          setScore(currentCell, matchOrMismatchScore)
          setPrevious(currentCell, CellAboveLeft)
        }
      } else {
        if (rowSpaceScore._1 > 0 || !positiveScores) {
          setScore(currentCell, rowSpaceScore)
          setPrevious(currentCell, CellAbove)
        }
      }
    } else {
      if (matchOrMismatchScore._1 >= colSpaceScore._1) {
        if (matchOrMismatchScore._1 > 0 || !positiveScores) {
          setScore(currentCell, matchOrMismatchScore)
          setPrevious(currentCell, CellAboveLeft)
        }
      } else {
        if (colSpaceScore._1 > 0 || !positiveScores) {
          setScore(currentCell, colSpaceScore)
          setPrevious(currentCell, CellLeft)
        }
      }
    }
  }

  def align(sequence1: String, sequence2: String, filter: (Double, Int, Int) => Boolean = (_, _, _) => true) = {
    val rowSequence = RowSequence(sequence1)
    val columnSequence = ColumnSequence(sequence2)
    val (rows, cols, score, previous) = sequenceAlignment(rowSequence, columnSequence)
    val iterator = for (i <- Iterator.range(0, rows); j <- Iterator.range(0, cols)) yield {
      val cell = Cell(i, j)
      cell -> score(cell)
    }
    val orderedScores = iterator.toIndexedSeq.sortBy(_._2._1).reverse
    alignCandidates(previous, orderedScores, filter)(rowSequence, columnSequence)
  }

  sealed trait AlignmentNode {
    def id: Int

    override def hashCode(): Int = id.hashCode()

    override def equals(other: scala.Any): Boolean = {
      other match {
        case otherNode: AlignmentNode if canEqual(other) => otherNode.id == this.id
        case _ => false
      }
    }

    def canEqual(other: Any): Boolean = other.isInstanceOf[AlignmentNode]
  }

  sealed trait Direction

  class QueryTerm(val id: Int, val term: String) extends AlignmentNode {
    override def canEqual(other: Any): Boolean = other.isInstanceOf[QueryTerm]
  }

  class QueryTermMatch(val id: Int, val term: String) extends AlignmentNode {
    override def canEqual(other: Any): Boolean = other.isInstanceOf[QueryTermMatch]
  }

  class TextMatch(val id: Int, val from: Int, val to: Int, val term: String) extends AlignmentNode {
    override def canEqual(other: Any): Boolean = other.isInstanceOf[TextMatch]
  }

  case class RowSequence(text: String)

  case class ColumnSequence(text: String)

  case class Cell(row: Int, column: Int) {
    def rowChar(implicit rowSequence: RowSequence) = {
      rowSequence.text.charAt(row - 1)
    }

    def columnChar(implicit columnSequence: ColumnSequence) = {
      columnSequence.text.charAt(column - 1)
    }

    def above = {
      Cell(row - 1, column)
    }

    def aboveLeft = {
      Cell(row - 1, column - 1)
    }

    def left = {
      Cell(row, column - 1)
    }
  }

  object Source extends AlignmentNode {
    val id = -1

    override def equals(other: scala.Any): Boolean = {
      other match {
        case ref: AnyRef => ref eq this
        case _ => false
      }
    }
  }

  object Target extends AlignmentNode {
    val id = -2

    override def equals(other: scala.Any): Boolean = {
      other match {
        case ref: AnyRef => ref eq this
        case _ => false
      }
    }
  }

  object CellLeft extends Direction

  object CellAbove extends Direction

  object CellAboveLeft extends Direction

}
