package thymeflow.spatial.geographic.metric.models

import thymeflow.spatial.geographic.geodesics.models.WGS84DefaultSpheroidGeodesic
import thymeflow.spatial.geographic.metric.GeographyLinearMetric

/**
  * @author David Montoya
  */
trait WGS84GeographyLinearMetric extends GeographyLinearMetric {
  val geodesic = WGS84DefaultSpheroidGeodesic
}

object WGS84GeographyLinearMetric extends WGS84GeographyLinearMetric