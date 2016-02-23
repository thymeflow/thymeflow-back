package pkb

import java.util.Scanner

import com.github.sardine.impl.SardineImpl
import org.openrdf.model.impl.{LinkedHashModel, SimpleValueFactory}
import org.openrdf.rio.{RDFFormat, Rio}
import pkb.sync.utils.OAuth2
import pkb.sync.{CalDavSynchronizer, CardDavSynchronizer}

object PKBGoogleClient {
  def main(args: Array[String]) {
    val sardine = new SardineImpl(OAuth2.Google.getAccessToken(Array(
      "https://www.googleapis.com/auth/carddav",
      "https://www.googleapis.com/auth/calendar"
    )))

    val model = new LinkedHashModel

    //CardDav
    val cardDavSynchronizer = new CardDavSynchronizer(SimpleValueFactory.getInstance, sardine)
    model.addAll(cardDavSynchronizer.synchronize("https://www.googleapis.com/.well-known/carddav"))

    //CalDav
    val calDavSynchronizer = new CalDavSynchronizer(SimpleValueFactory.getInstance, sardine)
    System.out.println("Enter your calendar ID (usually your email address):")
    val calendarId = new Scanner(System.in).next
    model.addAll(calDavSynchronizer.synchronize("https://apidata.googleusercontent.com/caldav/v2/" + calendarId + "/events/"))

    Rio.write(model, System.out, RDFFormat.TURTLE)
  }
}
