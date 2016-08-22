package com.thymeflow.spatial.cartesian

/**
  * @author David Montoya
  */
trait Envelope {
  require(lower.size == upper.size)

  def lower: IndexedSeq[Double]

  def upper: IndexedSeq[Double]

  def apply(lower: IndexedSeq[Double], upper: IndexedSeq[Double]): Envelope

  def expand(radius: Double): Envelope = {
    apply(
      lower.map(_ - radius),
      upper.map(_ + radius)
    )
  }

  def include(lower: IndexedSeq[Double], upper: IndexedSeq[Double]) = {
    if (lower.length != this.dimension) throw new IllegalArgumentException(s"expected lower coordinate of the same dimension expected ${this.dimension} received ${lower.length}")
    if (upper.length != this.dimension) throw new IllegalArgumentException(s"expected upper coordinate of the same dimension expected ${this.dimension} received ${upper.length}")
    _include(lower, upper)
  }

  protected def _include(lower: IndexedSeq[Double], upper: IndexedSeq[Double]) = {
    val newLowerCoordinates = this.lower.zip(lower).map {
      case (l, coordinate) =>
        math.min(l, coordinate)
    }
    val newUpperCoordinates = this.upper.zip(upper).map {
      case (u, coordinate) =>
        math.max(u, coordinate)
    }
    apply(newLowerCoordinates, newUpperCoordinates)
  }

  def include(envelope: Envelope) = {
    if (envelope.dimension != this.dimension) throw new IllegalArgumentException(s"expected envelope of the same dimension expected ${this.dimension} received ${envelope.dimension}")
    _include(envelope.lower, envelope.upper)
  }

  def include(coordinate: IndexedSeq[Double]) = {
    if (coordinate.size != this.dimension) throw new IllegalArgumentException(s"expected coordinate of the same dimension expected ${this.dimension} received ${coordinate.size}")
    val newLowerCoordinates = new Array[Double](this.dimension)
    val newUpperCoordinates = new Array[Double](this.dimension)
    for (i <- 0 until this.dimension) {
      val v = coordinate(i)
      val l = lower(i)
      val u = upper(i)
      if (v < l) {
        newLowerCoordinates(i) = v
        newUpperCoordinates(i) = u
      } else if (v > u) {
        newLowerCoordinates(i) = l
        newUpperCoordinates(i) = v
      } else {
        // l <= v <= u
        newLowerCoordinates(i) = l
        newUpperCoordinates(i) = u
      }
    }
    apply(newLowerCoordinates, newUpperCoordinates)
  }

  def dimension = lower.size
}

object Envelope {
  def apply(coordinate: Coordinate) = {
    new impl.PointEnvelope(coordinate.toSeq)
  }

  def apply(lower: Coordinate, upper: Coordinate) = {
    new impl.Envelope(lower.toSeq, upper.toSeq)
  }
}