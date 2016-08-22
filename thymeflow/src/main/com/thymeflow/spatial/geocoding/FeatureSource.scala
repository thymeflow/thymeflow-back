package com.thymeflow.spatial.geocoding

/**
  * @author David Montoya
  */
trait FeatureSource {
  def isValid: Boolean

  def iri: String
}

