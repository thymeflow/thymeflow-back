package thymeflow.spatial.geographic.datum

/**
  * @author David Montoya
  */
object WGS84Ellipsoid extends Ellipsoid {
  final val semiMajorAxis = 6378137.0
  final val inverseFlattening = 298.257223563
  final val semiMinorAxis = semiMajorAxis * (1 - 1 / inverseFlattening)
  final val isSphere = false
}
