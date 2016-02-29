package pkb

import java.util.Properties
import javax.mail.Session

import com.github.sardine.impl.SardineImpl
import org.apache.commons.io.IOUtils
import org.openrdf.model.impl.SimpleValueFactory
import org.openrdf.repository.sail.SailRepository
import org.openrdf.rio.{RDFFormat, Rio}
import org.openrdf.sail.memory.MemoryStore
import pkb.sync.utils.OAuth2
import pkb.sync.{CalDavSynchronizer, CardDavSynchronizer, EmailSynchronizer}

/**
  * @author Thomas Pellissier Tanon
  */
object PKBGoogleClient {
  def main(args: Array[String]) {
    val repository = new SailRepository(new MemoryStore())
    repository.initialize()
    val repositoryConnection = repository.getConnection
    repositoryConnection.add(getClass.getClassLoader.getResource("rdfs-ontology.ttl"), "", RDFFormat.TURTLE)

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
    repositoryConnection.add(cardDavSynchronizer.synchronize("https://www.googleapis.com/.well-known/carddav"))

    //CalDav
    val calDavSynchronizer = new CalDavSynchronizer(SimpleValueFactory.getInstance, sardine)
    repositoryConnection.add(calDavSynchronizer.synchronize("https://apidata.googleusercontent.com/caldav/v2/" + gmailAddress + "/events/"))

    //Emails
    val emailSynchronizer = new EmailSynchronizer(SimpleValueFactory.getInstance)
    val props = new Properties()
    props.put("mail.imap.ssl.enable", "true")
    props.put("mail.imap.auth.mechanisms", "XOAUTH2")
    val store = Session.getInstance(props).getStore("imap")
    store.connect("imap.gmail.com", gmailAddress, accessToken)
    repositoryConnection.add(emailSynchronizer.synchronize(store, 100)) //TODO: bad to have a such hardcoded limit but nice for tests

    Rio.write(repositoryConnection.getStatements(null, null, null).asList(), System.out, RDFFormat.TRIG)
  }
}
