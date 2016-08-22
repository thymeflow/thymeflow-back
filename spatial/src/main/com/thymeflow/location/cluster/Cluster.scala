package com.thymeflow.location.cluster

/**
  * @author David Montoya
  */
trait Cluster[OBSERVATION, SPACE, T] {
  def t: T

  def observations: IndexedSeq[OBSERVATION]

  def mean: SPACE

  def variance: Double

  def size = observations.size

  def weightedMean: SPACE
}
