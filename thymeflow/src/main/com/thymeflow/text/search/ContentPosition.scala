package com.thymeflow.text.search

/**
  * @author David Montoya
  */
trait ContentPosition{
  def index: Int
  def count: Int

  def mkString(content: IndexedSeq[String], separator: String = " ") = {
    slice(content).mkString(separator)
  }

  def slice(content: IndexedSeq[String]) = {
    content.slice(index, index + count)
  }

  override def hashCode(): Int = (index, count).hashCode()

  override def equals(other: Any): Boolean = other match {
    case that: ContentPosition =>
      (that canEqual this) &&
        (index == that.index) &&
        (count == that.count)
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[ContentPosition]
}

object ContentPosition{
  def apply(index: Int, count: Int) = {
    com.thymeflow.text.search.impl.ContentPosition(index = index, count = count)
  }
}
