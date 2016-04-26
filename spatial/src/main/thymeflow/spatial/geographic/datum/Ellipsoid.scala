package thymeflow.spatial.geographic.datum

/**
  * @author David Montoya
  */
trait Ellipsoid {
  def semiMinorAxis: Double

  def semiMajorAxis: Double

  def inverseFlattening: Double

  def isSphere: Boolean


}
