package pkb.api

import java.io.File
import java.util.Properties
import javax.mail.Session

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import com.github.sardine.impl.SardineImpl
import org.apache.commons.io.IOUtils
import org.openrdf.IsolationLevels
import pkb.Pipeline
import pkb.actors._
import pkb.inferencer.{InverseFunctionalPropertyInferencer, PrimaryFacetEnricher}
import pkb.rdf.RepositoryFactory
import pkb.sync.{CalDavSynchronizer, CardDavSynchronizer, EmailSynchronizer, FileSynchronizer}

/**
  * @author Thomas Pellissier Tanon
  */
object MainApi extends App with SparqlService {

  override protected val repository = RepositoryFactory.initializedMemoryRepository(
    persistenceDirectory = Some(new File(System.getProperty("java.io.tmpdir") + "/pkb/sesame-memory")),
    isolationLevel = IsolationLevels.SERIALIZABLE,
    elasticSearch = false
  )
  private val redirectionTarget = Uri("http://localhost:4200")
  //TODO: should be in configuration
  private val pipeline = new Pipeline(
    repository.getConnection,
    List(new InverseFunctionalPropertyInferencer(repository.getConnection), new PrimaryFacetEnricher(repository.getConnection))
  )

  private val route = {
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
      } ~
      path("upload") {
        uploadedFile("file") {
          case (fileInfo, file) =>
            pipeline.addSource(
              FileSynchronizer.Config(file, Some(fileInfo.contentType.mediaType.value))
            )
            redirect(redirectionTarget, StatusCodes.TemporaryRedirect)
        }
      }
  }
  Http().bindAndHandle(route, "localhost", 8080)

  //TODO: make it configurable

  private def onGoogleToken(token: String): Unit = {
    val sardine = new SardineImpl(token)

    //Get user email
    val gmailAddress = IOUtils.toString(sardine.get("https://www.googleapis.com/userinfo/email"))
      .split("&")(0).split("=")(1) //The result has the format "email=foo@gmail.com&..."

    //CardDav
    pipeline.addSource(
      CardDavSynchronizer.Config(sardine, "https://www.googleapis.com/.well-known/carddav")
    )

    //CalDav
    pipeline.addSource(
      CalDavSynchronizer.Config(sardine, "https://apidata.googleusercontent.com/caldav/v2/" + gmailAddress + "/events/")
    )

    //Emails
    val props = new Properties()
    props.put("mail.imap.ssl.enable", "true")
    props.put("mail.imap.auth.mechanisms", "XOAUTH2")
    val store = Session.getInstance(props).getStore("imap")
    store.connect("imap.gmail.com", gmailAddress, token)
    pipeline.addSource(EmailSynchronizer.Config(store))
  }
}
