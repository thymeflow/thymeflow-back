package pkb.rdf

import info.aduna.iteration.CloseableIteration

/**
  * @author Thomas Pellissier Tanon
  */
object Converters {

  implicit class CloseableIterationWrapper[E, X <: Exception](closeableIteration: CloseableIteration[E, X])
    extends Traversable[E] {

    override def foreach[U](f: (E) => U): Unit = {
      while (closeableIteration.hasNext) {
        f(closeableIteration.next())
      }
      closeableIteration.close()
    }
  }

}
