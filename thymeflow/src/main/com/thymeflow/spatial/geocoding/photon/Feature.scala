package com.thymeflow.spatial.geocoding.photon

import com.thymeflow.spatial.geocoding.FeatureSource
import com.thymeflow.spatial.geographic.Point
import com.thymeflow.spatial.{Address, geocoding}

/**
  * @author David Montoya
  */
case class Feature(name: Option[String] = None, point: Point, address: Address, source: FeatureSource) extends geocoding.Feature {
  def isValid = source.isValid
}
