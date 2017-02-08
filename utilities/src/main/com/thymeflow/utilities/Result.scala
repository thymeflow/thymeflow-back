package com.thymeflow.utilities

/**
  * @author Thomas Pellissier Tanon
  */
sealed abstract class Result[+T, +E] {
  def isOk: Boolean

  def isError: Boolean

  def ok: Option[T]

  def error: Option[E]
}

final case class Ok[T](value: T) extends Result[T, Nothing] {
  override def isOk: Boolean = true

  override def isError: Boolean = false

  override def ok: Option[T] = Some(value)

  override def error: Option[Nothing] = None
}

object Ok {
  def apply(): Ok[Unit] = Ok(())
}

final case class Error[+E](value: E) extends Result[Nothing, E] {
  override def isOk: Boolean = false

  override def isError: Boolean = true

  override def ok: Option[Nothing] = None

  override def error: Option[E] = Some(value)
}

object Error {
}
