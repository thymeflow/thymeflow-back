package pkb.utilities

import java.io.{PrintWriter, StringWriter}

import scala.annotation.tailrec

/**
  * @author David Montoya
  */
object ExceptionUtils {

  /**
    * <p>Gets the stack trace from a Throwable as a String.</p>
    *
    * @param throwable  the <code>Throwable</code> to be examined
    * @return the stack trace as generated by the exception's
    *         <code>printStackTrace(PrintWriter)</code> method
    */
  def getStackTrace(throwable: Throwable): String = {
    getUnrolledStackTrace(throwable, maxDepth = 1)
  }

  /**
    * <p>Gets the concatenation of stack traces from the causes of a Throwable
    * as a String.</p>
    *
    * @param throwable  the <code>Throwable</code> to be examined
    * @param maxDepth   the maximum cause depth to examine
    * @return the stack trace as generated by the exception's
    *         <code>printStackTrace(PrintWriter)</code> method
    */
  def getUnrolledStackTrace(throwable: Throwable, maxDepth: Int = 10): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw, true)
    @tailrec
    def unroll(t: Throwable, depth: Int): Unit = {
      if (t != null && depth > 0) {
        t.printStackTrace(pw)
        unroll(t.getCause, depth - 1)
      }
    }
    unroll(throwable, maxDepth)
    sw.getBuffer.toString
  }
}
