package com.thymeflow.spatial.geographic.geodesics.models

import com.thymeflow.spatial.geographic.datum.WGS84Ellipsoid
import com.thymeflow.spatial.geographic.geodesics.SpheroidGeodesic
import com.thymeflow.spatial.geographic.geodesics.calculator.KarneyGeodeticCalculator

/**
  * @author David Montoya
  */
trait WGS84KarneySpheroidGeodesic extends SpheroidGeodesic {
  val geodeticCalculator = new KarneyGeodeticCalculator(WGS84KarneySpheroidGeodesic.this.ellipsoid)

  def ellipsoid = WGS84Ellipsoid

  override def adjustProjection = true
}

object WGS84KarneySpheroidGeodesic extends WGS84KarneySpheroidGeodesic
