package pkb

import java.util.Properties
import javax.mail.Session

import com.github.sardine.impl.SardineImpl
import org.apache.commons.io.IOUtils
import org.openrdf.model.impl.SimpleValueFactory
import org.openrdf.rio.{RDFFormat, Rio}
import pkb.rdf.RepositoryFactory
import pkb.sync.utils.OAuth2
import pkb.sync.{CalDavSynchronizer, CardDavSynchronizer, EmailSynchronizer}

/**
  * @author Thomas Pellissier Tanon
  */
object PKBGoogleClient {
  def main(args: Array[String]) {
    val repositoryConnection = RepositoryFactory.initializedMemoryRepository.getConnection

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
    val cardDavSynchronizer = new CardDavSynchronizer(SimpleValueFactory.getInstance, sardine, "https://www.googleapis.com/.well-known/carddav")
    cardDavSynchronizer.synchronize().foreach(document =>
      repositoryConnection.add(document.model)
    )

    //CalDav
    val calDavSynchronizer = new CalDavSynchronizer(SimpleValueFactory.getInstance, sardine, "https://apidata.googleusercontent.com/caldav/v2/" + gmailAddress + "/events/")
    calDavSynchronizer.synchronize().foreach(document =>
      repositoryConnection.add(document.model)
    )

    //Emails
    val props = new Properties()
    props.put("mail.imap.ssl.enable", "true")
    props.put("mail.imap.auth.mechanisms", "XOAUTH2")
    val store = Session.getInstance(props).getStore("imap")
    store.connect("imap.gmail.com", gmailAddress, accessToken)
    val emailSynchronizer = new EmailSynchronizer(SimpleValueFactory.getInstance, store, 100)
    emailSynchronizer.synchronize().foreach(document =>
      repositoryConnection.add(document.model)
    ) //TODO: bad to have a such hardcoded limit but nice for tests

    Rio.write(repositoryConnection.getStatements(null, null, null).asList(), System.out, RDFFormat.TRIG)
  }
}
