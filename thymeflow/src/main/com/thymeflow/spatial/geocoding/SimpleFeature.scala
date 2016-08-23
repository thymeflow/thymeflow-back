package com.thymeflow.spatial.geocoding

import com.thymeflow.spatial.Address
import com.thymeflow.spatial.geographic.Point

/**
  * @author David Montoya
  */
case class SimpleFeature(name: Option[String] = None, point: Point, address: Address, source: FeatureSource) extends Feature
