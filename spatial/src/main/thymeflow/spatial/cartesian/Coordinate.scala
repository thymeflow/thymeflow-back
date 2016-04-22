package thymeflow.spatial.cartesian

/**
  * @author David Montoya
  */
trait CoordinateLike[T] {
  def x: Double

  def y: Double

  def z: Double

  def copy(x: Double = this.x, y: Double = this.y, z: Double = this.z) = {
    build(x, y, z)
  }

  def norm = {
    math.sqrt(x * x + y * y + z * z)
  }

  protected def repr: T

  protected def build(x: Double, y: Double, z: Double): T
}

trait Coordinate extends CoordinateLike[Coordinate] with CoordinateOperationsLike[Coordinate]

trait CoordinateOperationsLike[T <: CoordinateLike[T]] extends CoordinateLike[T] {

  def toSeq: IndexedSeq[Double] = Array(x, y, z)

  def normalize: T = {
    val n = norm
    if (n == 0) repr else build(x / n, y / n, z / n)
  }

  def dotProduct(c2: T) = {
    x * c2.x + y * c2.y + z * c2.z
  }

  def crossProduct(c2: T): T = {
    build(
      this.y * c2.z - this.z * c2.y,
      this.z * c2.x - this.x * c2.z,
      this.x * c2.y - this.y * c2.x
    )
  }

  def scale(d: Double) = {
    Coordinate(x * d, y * d, z * d)
  }

  def minus(c2: T): T = {
    build(x - c2.x, y - c2.y, z - c2.z)
  }

  def plus(c2: T): T = {
    build(x + c2.x, y + c2.y, z + c2.z)
  }

  def project(p0: T, p1: T): T = {
    if (this.equals(p0) || this.equals(p1)) {
      repr
    } else {
      projectionFactor(p0, p1) match {
        case Some(r) =>
          if (r < 0.0) {
            p0
          } else if (r > 1.0) {
            p1
          } else {
            val x = p0.x + r * (p1.x - p0.x)
            val y = p0.y + r * (p1.y - p0.y)
            val z = p0.z + r * (p1.z - p0.z)
            build(x, y, z)
          }
        case None =>
          p0
      }
    }
  }

  def projectionFactor(p0: T, p1: T): Option[Double] = {
    if (this == p0) return Some(0.0)
    if (this == p1) return Some(1.0)
    /*     	      AC dot AB
                   r = ---------
                         ||AB||^2
                r has the following meaning:
                r=0 P = A
                r=1 P = B
                r<0 P is on the backward extension of AB
                r>1 P is on the forward extension of AB
                0<r<1 P is interior to AB
        */
    val dx: Double = p1.x - p0.x
    val dy: Double = p1.y - p0.y
    val len: Double = dx * dx + dy * dy

    // handle zero-length segments
    if (len <= 0.0) {
      return None
    }

    val r: Double = ((this.x - p0.x) * dx + (this.y - p0.y) * dy) / len
    Some(r)
  }

  def distance(A: T, B: T): Double = {

    if (A.x == B.x && A.y == B.y) return this.distance(A)

    val len2: Double = (B.x - A.x) * (B.x - A.x) + (B.y - A.y) * (B.y - A.y)
    val r: Double = ((this.x - A.x) * (B.x - A.x) + (this.y - A.y) * (B.y - A.y)) / len2

    if (r <= 0.0) this.distance(A)
    else if (r >= 1.0) this.distance(B)

    /*
     * (2) s = (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay)
     *         -----------------------------
     *                    L^2
     *
     * Then the distance from C to P = |s|*L.
     *
     * This is the same calculation as {@link #distancePointLinePerpendicular}.
     * Unrolled here for performance.
     */
    val s: Double = ((A.y - this.y) * (B.x - A.x) - (A.x - this.x) * (B.y - A.y)) / len2
    Math.abs(s) * Math.sqrt(len2)
  }

  def distance(c2: T) = {
    val dx = x - c2.x
    val dy = y - c2.y
    val dz = z - c2.z
    math.sqrt(dx * dx + dy * dy + dz * dz)
  }
}

object Coordinate {
  def apply(x: Double, y: Double): Coordinate = impl.Coordinate(x, y)

  def apply(x: Double, y: Double, z: Double): Coordinate = impl.Coordinate(x, y, z)

  def unapply(c: Coordinate): Option[(Double, Double, Double)] = Some((c.x, c.y, c.z))
}