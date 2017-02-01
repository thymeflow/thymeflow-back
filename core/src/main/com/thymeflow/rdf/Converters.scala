package com.thymeflow.rdf

import org.eclipse.rdf4j.common.iteration.CloseableIteration

/**
  * @author Thomas Pellissier Tanon
  */
object Converters {

  implicit class CloseableIterationWrapper[E, X <: Exception](closeableIteration: CloseableIteration[E, X])
    extends Iterator[E] {

    override def hasNext: Boolean = {
      if (closeableIteration.hasNext) {
        true
      } else {
        closeableIteration.close()
        false
      }
    }

    override def next(): E = {
      closeableIteration.next()
    }
  }
}
