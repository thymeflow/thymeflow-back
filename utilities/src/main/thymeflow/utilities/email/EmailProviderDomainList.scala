package thymeflow.utilities.email

import java.util.Locale

/**
  * @author David Montoya
  *         Maintains a list of free/disposable email providers
  *         Source: https://github.com/willwhite/freemail
  *
  */
object EmailProviderDomainList {
  lazy val disposable = readEmailList("email/providers/disposable.txt")
  lazy val free = readEmailList("email/providers/free.txt")
  lazy val all = disposable ++ free

  // TODO: Provide automatic download/update facilities from an online URL
  def readEmailList(resourcePath: String) = {
    val inputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream(resourcePath)
    scala.io.Source.fromInputStream(inputStream).getLines.map(_.toLowerCase(Locale.ROOT)).toSet
  }

}
