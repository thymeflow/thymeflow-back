package pkb

import java.util.Properties
import javax.mail.Session

import com.github.sardine.impl.SardineImpl
import org.apache.commons.io.IOUtils
import org.openrdf.model.impl.{LinkedHashModel, SimpleValueFactory}
import org.openrdf.rio.{RDFFormat, Rio}
import pkb.sync.utils.OAuth2
import pkb.sync.{CalDavSynchronizer, CardDavSynchronizer, EmailSynchronizer}

object PKBGoogleClient {
  def main(args: Array[String]) {
    val model = new LinkedHashModel

    val accessToken = OAuth2.Google.getAccessToken(Array(
      "https://www.googleapis.com/auth/userinfo.email",
      "https://www.googleapis.com/auth/carddav",
      "https://www.googleapis.com/auth/calendar",
      "https://mail.google.com/"
    ))
    val sardine = new SardineImpl(accessToken)

    //Get user email
    val gmailAddress = IOUtils.toString(sardine.get("https://www.googleapis.com/userinfo/email"))
      .split("&")(0).split("=")(1) //The result has the format "email=foo@gmail.com&..."


    //CardDav
    val cardDavSynchronizer = new CardDavSynchronizer(SimpleValueFactory.getInstance, sardine)
    model.addAll(cardDavSynchronizer.synchronize("https://www.googleapis.com/.well-known/carddav"))

    //CalDav
    val calDavSynchronizer = new CalDavSynchronizer(SimpleValueFactory.getInstance, sardine)
    model.addAll(calDavSynchronizer.synchronize("https://apidata.googleusercontent.com/caldav/v2/" + gmailAddress + "/events/"))

    //Emails
    val emailSynchronizer = new EmailSynchronizer(SimpleValueFactory.getInstance)
    val props = new Properties()
    props.put("mail.imap.ssl.enable", "true")
    props.put("mail.imap.auth.mechanisms", "XOAUTH2")
    val store = Session.getInstance(props).getStore("imap")
    store.connect("imap.gmail.com", gmailAddress, accessToken)
    model.addAll(emailSynchronizer.synchronize(store, 100)) //TODO: bad to have a such hardcoded limit but nice for tests

    Rio.write(model, System.out, RDFFormat.TRIG)
  }
}
