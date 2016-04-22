package thymeflow.spatial.geographic.geodesics.models

import thymeflow.spatial.geographic.datum.WGS84Ellipsoid
import thymeflow.spatial.geographic.geodesics.SpheroidGeodesic
import thymeflow.spatial.geographic.geodesics.calculator.KarneyGeodeticCalculator

/**
  * @author David Montoya
  */
trait WGS84KarneySpheroidGeodesic extends SpheroidGeodesic {
  val geodeticCalculator = new KarneyGeodeticCalculator(WGS84KarneySpheroidGeodesic.this.ellipsoid)

  def ellipsoid = WGS84Ellipsoid

  override def adjustProjection = true
}

object WGS84KarneySpheroidGeodesic extends WGS84KarneySpheroidGeodesic
