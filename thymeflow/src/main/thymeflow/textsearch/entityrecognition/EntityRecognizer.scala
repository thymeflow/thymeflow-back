package thymeflow.textsearch.entityrecognition

import scala.concurrent.Future

/**
 * @author  David Montoya
 */

trait ContentPosition{
  def index: Int
  def count: Int

  def slice(content: Seq[String]) = {
    content.slice(index, index + count)
  }

  def mkString(content: Seq[String], separator: String = " ") = {
    slice(content).mkString(separator)
  }

  override def hashCode(): Int = (index, count).hashCode()

  def canEqual(other: Any): Boolean = other.isInstanceOf[ContentPosition]

  override def equals(other: Any): Boolean = other match {
    case that: ContentPosition =>
      (that canEqual this) &&
        (index == that.index) &&
        (count == that.count)
    case _ => false
  }
}

trait EntityRecognizer[ENTITY] {
  def recognizeEntities(content: Seq[String], searchDepth: Int, clearDuplicateNestedResults: Boolean): Future[(Traversable[(ContentPosition,Traversable[(ENTITY,Float)])])]
}

object ContentPosition{
  def apply(index: Int, count: Int) = {
    impl.ContentPosition(index = index, count = count)
  }
}
