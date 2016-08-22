package com.thymeflow.spatial.geocoding

import com.thymeflow.spatial.Address
import com.thymeflow.spatial.geographic.Point

/**
  * @author David Montoya
  */
trait Feature {
  def source: FeatureSource

  def point: Point

  def address: Address

  def name: Option[String]
}
