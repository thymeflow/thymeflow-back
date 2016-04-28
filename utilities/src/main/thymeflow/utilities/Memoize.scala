package thymeflow.utilities

import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A memoized unary function.
 *
 * @tparam T the argument type
 * @tparam R the return type
 */
trait Memoize1[-T, +R] extends (T => R) with StrictLogging {

  // map that stores (argument, result) pairs
  protected[this] val vals: mutable.Map[T, R]

  def f: T => R

  // Forgets memoized results
  def clear(): Unit = {
    vals.clear()
  }

  // Given an argument x,
  //   If vals contains x return vals(x).
  //   Otherwise, update vals so that vals(x) == f(x) and return f(x).
  def apply(x: T): R = {
    vals getOrElseUpdate (x, f(x))
  }
}

trait ConcurrentMemoize[-T, +R] {
  protected[this] val vals = new java.util.concurrent.ConcurrentHashMap[T,R]().asScala
}

trait NonConcurrentMemoize[-T, +R] {
  protected[this] val vals = mutable.OpenHashMap.empty[T,R]
}

/**
 * A memoized unary function.
 *
 * @param f A unary function to memoize
 * @tparam T the argument type
 * @tparam R the return type
 */
private class PersistentMemoize1[-T, +R](val f: T => R) extends Memoize1[T,R] with NonConcurrentMemoize[T,R]

/**
 * A memoized unary function backed-up by a FIFO cache.
 *
 * @param g A unary function to memoize
 * @tparam T the argument type
 * @tparam R the return type
 */
private class FifoCache[-T, +R](g: T => R, cacheSize: Int) extends Memoize1[T,R] with NonConcurrentMemoize[T,R]{

  private[this] val fifoQueue = mutable.Queue.empty[T]

  override def clear(): Unit ={
    vals.clear()
    fifoQueue.clear()
  }

  override def f ={
    x =>
      if(fifoQueue.size >= cacheSize){
        vals.remove(fifoQueue.dequeue())
      }
      fifoQueue.enqueue(x)
      g(x)
  }
}


/**
 * A memoized unary function backed-up by a concurrent FIFO cache.
 *
 * @param g A unary function to memoize
 * @tparam T the argument type
 * @tparam R the return type
 */
private class ConcurrentFifoCache[-T, +R](g: T => R, cacheSize: Int) extends Memoize1[T,R] with ConcurrentMemoize[T,R]{

  private[this] val fifoQueue = new java.util.concurrent.ConcurrentLinkedQueue[T]

  override def clear(): Unit ={
    vals.clear()
    fifoQueue.clear()
  }

  override def f ={
    x =>
      if(fifoQueue.size >= cacheSize){
        vals.remove(fifoQueue.poll())
      }
      fifoQueue.add(x)
      g(x)
  }
}

object Memoize {
  /**
   * Memoize a unary (single-argument) function with a FIFO-cache
   *
   * @param f the unary function to memoize
   */
  def fifoCache[T, R](cacheSize: Int, f: T => R) : Memoize1[T,R] = new FifoCache(f, cacheSize)

  def fifoCache[T1, T2, R](cacheSize: Int, f: (T1,T2) => R) : (T1,T2) => R = Function.untupled(new FifoCache(f.tupled, cacheSize))

  /**
   * Memoize a unary (single-argument) function with a concurrent FIFO-cache
   *
   * @param f the unary function to memoize
   */
  def concurrentFifoCache[T, R](cacheSize: Int, f: T => R) : Memoize1[T,R] = new ConcurrentFifoCache(f, cacheSize)

  def concurrentFifoCache[T1, T2, R](cacheSize: Int, f: (T1,T2) => R) : (T1,T2) => R = Function.untupled(new ConcurrentFifoCache(f.tupled, cacheSize))

  /**
   * Memoize a binary (two-argument) function.
   *
   * @param f the binary function to memoize
   *
   * This works by turning a function that takes two arguments of type
   * T1 and T2 into a function that takes a single argument of type
   * (T1, T2), memoizing that "tupled" function, then "untupling" the
   * memoized function.
   */
  def memoize[T1, T2, R](f: (T1, T2) => R) : (T1,T2) => R =
    Function.untupled(memoize(f.tupled))

  /**
   * Memoize a ternary (three-argument) function.
   *
   * @param f the ternary function to memoize
   */
  def memoize[T1, T2, T3, R](f: (T1, T2, T3) => R): (T1,T2,T3) => R =
    Function.untupled(memoize(f.tupled))

  /**
   * Fixed-point combinator (for memoizing recursive functions).
   */
  def Y[T, R](f: (T => R) => T => R): (T => R) = {
    lazy val yf: (T => R) = memoize(f(yf)(_))
    yf
  }

  // ... more memoize methods for higher-arity functions ...

  /**
    * Memoize a unary (single-argument) function.
    *
    * @param f the unary function to memoize
    */
  def memoize[T, R](f: T => R): Memoize1[T, R] = new PersistentMemoize1(f)
}