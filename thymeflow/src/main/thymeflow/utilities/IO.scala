package thymeflow.utilities

import java.io._
import java.nio.charset.Charset

import org.apache.commons.io.IOUtils

/**
 * @author  David Montoya
 */
object IO {
  def toString(input: InputStream, charset: Charset = Charset.forName("UTF-8")) = {
    IOUtils.toString(input, charset)
  }

  def utf8InputStreamReader(input: InputStream) = {
    val inputStreamReader = new BufferedReader(new InputStreamReader(input, "UTF-8"))
    inputStreamReader.mark(1)
    val c = inputStreamReader.read()
    if (c != 0xFEFF) {
      inputStreamReader.reset()
    }
    inputStreamReader
  }

  def utf8OutputStreamWriter(output: OutputStream, writeBOM: Boolean = false) = {
    val writer = new BufferedWriter(new OutputStreamWriter(output, "UTF-8"))
    if(writeBOM){
      writer.append('\uFEFF')
    }
    writer
  }
}
