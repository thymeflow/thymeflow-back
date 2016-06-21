package thymeflow.utilities

import scala.annotation.tailrec

/**
  * @author David Montoya
  */
object VectorExtensions {
  def reduceLeftTree[T](currentBranch: Vector[T])(op: (T, T) => T): T = reduceLeftTreeInternal(currentBranch)(op)

  @tailrec
  private def reduceLeftTreeInternal[T](currentBranch: Vector[T],
                                        rightBranches: List[Vector[T]] = Nil,
                                        resultStack: List[Option[T]] = Nil)(op: (T, T) => T): T = {
    currentBranch match {
      case Vector() =>
        throw new UnsupportedOperationException("empty.reduceLeftTree")
      case Vector(t) =>
        @tailrec
        def unfoldStack(stack: List[Option[T]]): List[Option[T]] = {
          stack match {
            case Some(t1) :: Some(t2) :: stackTail =>
              unfoldStack(Some(op(t2, t1)) :: stackTail)
            case Some(t1) :: None :: stackTail =>
              Some(t1) :: stackTail
            case _ =>
              stack
          }
        }
        (rightBranches, unfoldStack(Some(t) +: resultStack)) match {
          case (head :: tail, newResultStack) =>
            reduceLeftTreeInternal(head, tail, newResultStack)(op)
          case (Nil, Some(result) :: Nil) =>
            result
          case _ => throw new IllegalStateException("should not happen")
        }
      case _ =>
        val (t1s, t2s) = currentBranch.splitAt(currentBranch.size / 2)
        reduceLeftTreeInternal(t1s, t2s +: rightBranches, None :: resultStack)(op)
    }
  }
}
