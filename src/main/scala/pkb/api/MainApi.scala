package pkb.api

import java.util.Properties
import javax.mail.Session

import akka.actor.ActorSystem
import com.github.sardine.impl.SardineImpl
import org.apache.commons.io.IOUtils
import org.openrdf.model.impl.SimpleValueFactory
import pkb.Pipeline
import pkb.rdf.RepositoryFactory
import pkb.sync.{CalDavSynchronizer, CardDavSynchronizer, EmailSynchronizer}
import spray.http._
import spray.routing._

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * @author Thomas Pellissier Tanon
  */
object MainApi extends App with SimpleRoutingApp with SparqlService {

  override protected val repositoryConnection = RepositoryFactory.initializedMemoryRepository.getConnection
  implicit val system = ActorSystem("pkb")
  private val pipeline = new Pipeline(repositoryConnection)
  private val redirectionTarget = Uri("http://localhost:4200") //TODO: should be in configuration

  startServer(interface = "localhost", port = 8080) {
    path("sparql") {
      sparqlRoute
    } ~
      pathPrefix("oauth") {
        pathPrefix("google") {
          path("auth") {
            redirect(OAuth2.Google.getAuthUri(Array(
              "https://www.googleapis.com/auth/userinfo.email",
              "https://www.googleapis.com/auth/carddav",
              "https://www.googleapis.com/auth/calendar",
              "https://mail.google.com/"
            ), "http://localhost:8080/oauth/google/token"), StatusCodes.TemporaryRedirect) //TODO: avoid to hardcode the URI
          } ~
            path("token") {
              parameter('code) { code =>
                OAuth2.Google.getAccessToken(code, "http://localhost:8080/oauth/google/token").foreach(onGoogleToken)
                redirect(redirectionTarget, StatusCodes.TemporaryRedirect)
              }
            }
        }
    }
  }

  private def onGoogleToken(token: String): Unit = {
    val sardine = new SardineImpl(token)

    //Get user email
    val gmailAddress = IOUtils.toString(sardine.get("https://www.googleapis.com/userinfo/email"))
      .split("&")(0).split("=")(1) //The result has the format "email=foo@gmail.com&..."

    //CardDav
    pipeline.addSynchronizer(
      new CardDavSynchronizer(SimpleValueFactory.getInstance, sardine, "https://www.googleapis.com/.well-known/carddav")
    )

    //CalDav
    pipeline.addSynchronizer(
      new CalDavSynchronizer(SimpleValueFactory.getInstance, sardine, "https://apidata.googleusercontent.com/caldav/v2/" + gmailAddress + "/events/")
    )

    //Emails
    val props = new Properties()
    props.put("mail.imap.ssl.enable", "true")
    props.put("mail.imap.auth.mechanisms", "XOAUTH2")
    val store = Session.getInstance(props).getStore("imap")
    store.connect("imap.gmail.com", gmailAddress, token)
    pipeline.addSynchronizer(new EmailSynchronizer(SimpleValueFactory.getInstance, store, 100))

    pipeline.run(2) //TODO: bad hack
  }
}
