package com.thymeflow.spatial.cartesian.impl

import com.thymeflow.spatial

/**
  * @author David Montoya
  */
case class PointEnvelope(coordinates: IndexedSeq[Double])
  extends spatial.cartesian.Envelope {

  override def apply(lower: IndexedSeq[Double], upper: IndexedSeq[Double]) = {
    Envelope(lower, upper)
  }

  override def lower: IndexedSeq[Double] = coordinates

  override def upper: IndexedSeq[Double] = coordinates
}

case class Envelope(lower: IndexedSeq[Double], upper: IndexedSeq[Double])
  extends spatial.cartesian.Envelope {

  override def apply(lower: IndexedSeq[Double], upper: IndexedSeq[Double]) = {
    Envelope(lower, upper)
  }
}


