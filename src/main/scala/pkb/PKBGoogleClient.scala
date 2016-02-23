package pkb

import com.github.sardine.impl.SardineImpl
import org.openrdf.model.impl.SimpleValueFactory
import org.openrdf.rio.{RDFFormat, Rio}
import pkb.sync.CardDavSynchronizer
import pkb.sync.utils.OAuth2

object PKBGoogleClient {
  def main(args: Array[String]) {
    val sardine = new SardineImpl(OAuth2.Google.getAccessToken(Array(
      "https://www.googleapis.com/auth/carddav",
      "https://www.googleapis.com/auth/calendar.readonly"
    )))
    val cardDavSyncronizer = new CardDavSynchronizer(SimpleValueFactory.getInstance, sardine)
    Rio.write(cardDavSyncronizer.synchronize("https://www.googleapis.com/.well-known/carddav"), System.out, RDFFormat.TURTLE)
  }
}
