package thymeflow.api

import java.util.Properties
import java.util.concurrent.TimeUnit
import javax.mail.{MessagingException, Session}

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import com.github.sardine.impl.SardineImpl
import org.apache.commons.io.IOUtils
import org.openrdf.repository.Repository
import thymeflow.Pipeline
import thymeflow.actors._
import thymeflow.sync._

import scala.concurrent.duration.Duration

/**
  * @author Thomas Pellissier Tanon
  */
trait Api extends App with SparqlService {

  private val redirectionTarget = Uri("http://localhost:4200")
  //TODO: avoid to hardcode the URI
  private val googleOAuth = OAuth2.Google("http://localhost:8080/oauth/google/token")
  //TODO: avoid to hardcode the URI
  private val microsoftOAuth = OAuth2.Microsoft("http://localhost:8080/oauth/microsoft/token")
  //TODO: avoid to hardcode the URI
  private val route = {
    path("sparql") {
      sparqlRoute
    } ~
      pathPrefix("oauth") {
        pathPrefix("google") {
          path("auth") {
            redirect(googleOAuth.getAuthUri(Array(
              "https://www.googleapis.com/auth/userinfo.email",
              "https://www.googleapis.com/auth/carddav",
              "https://www.googleapis.com/auth/calendar",
              "https://mail.google.com/"
            )), StatusCodes.TemporaryRedirect)
          } ~
            path("token") {
              parameter('code) { code =>
                logger.info(s"Google token received at time $durationSinceStart")
                googleOAuth.getAccessToken(code).foreach(tokenRenewal(_, onGoogleToken))
                redirect(redirectionTarget, StatusCodes.TemporaryRedirect)
              }
            }
        } ~
          pathPrefix("microsoft") {
            path("auth") {
              redirect(microsoftOAuth.getAuthUri(Array("wl.imap", "wl.offline_access")), StatusCodes.TemporaryRedirect)
            } ~
              path("token") {
                parameter('code) { code =>
                  logger.info(s"Microsoft token received at time $durationSinceStart")
                  microsoftOAuth.getAccessToken(code).foreach(tokenRenewal(_, onMicrosoftToken))
                  redirect(redirectionTarget, StatusCodes.TemporaryRedirect)
                }
              }
          }
      } ~
      pathPrefix("imap") {
        post {
          formFieldMap { fields =>
            logger.info(s"IMAP accound on ${fields.get("host").get} received at time $durationSinceStart")
            val props = new Properties()
            if (fields.get("ssl").contains("true")) {
              props.put("mail.imap.ssl.enable", "true")
            }
            try {
              val store = Session.getInstance(props).getStore("imap")
              store.connect(fields.get("host").get, fields.get("user").get, fields.get("password").get)
              pipeline.addSource(EmailSynchronizer.Config(store))
            } catch {
              case e: MessagingException => logger.error(e.getLocalizedMessage, e)
                complete(StatusCodes.InternalServerError, "IMAP error: " + e.getLocalizedMessage)
            }
            redirect(redirectionTarget, StatusCodes.TemporaryRedirect)
          }
        }
      } ~
      path("upload") {
        uploadedFile("file") {
          case (fileInfo, file) =>
            logger.info(s"File $file received at time $durationSinceStart")
            pipeline.addSource(
              FileSynchronizer.Config(file, Some(fileInfo.contentType.mediaType.value))
            )
            redirect(redirectionTarget, StatusCodes.TemporaryRedirect)
        }
      }
  }

  protected def pipeline: Pipeline

  protected def repository: Repository

  Http().bindAndHandle(route, "localhost", 8080)

  //TODO: make it configurable

  protected def durationSinceStart: Duration = {
    Duration(System.currentTimeMillis() - executionStart, TimeUnit.MILLISECONDS)
  }

  private def tokenRenewal[T <: OAuth2.RenewableToken[T], R](token: T, onNewToken: (T => R)): Unit = {
    var currentToken = token
    onNewToken(currentToken)

    token.onShouldBeRenewed(currentToken.renew().foreach(newToken => {
      currentToken = newToken
      onNewToken(currentToken)
    }))
  }

  private def onGoogleToken(token: googleOAuth.Token): Unit = {
    val sardine = new SardineImpl(token.access_token)

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

    try {
      val store = Session.getInstance(props).getStore("imap")
      store.connect("imap.gmail.com", gmailAddress, token.access_token)
      pipeline.addSource(EmailSynchronizer.Config(store))
    } catch {
      case e: MessagingException => logger.error(e.getLocalizedMessage, e)
        complete(StatusCodes.InternalServerError, "Google IMAP error: " + e.getLocalizedMessage)
    }
  }

  private def onMicrosoftToken(token: microsoftOAuth.Token): Unit = {
    //Emails
    val props = new Properties()
    props.put("mail.imap.ssl.enable", "true")
    props.put("mail.imap.auth.mechanisms", "XOAUTH2")

    try {
      val store = Session.getInstance(props).getStore("imap")
      store.connect("imap-mail.outlook.com", token.user_id.get, token.access_token)
      pipeline.addSource(EmailSynchronizer.Config(store))
    } catch {
      case e: MessagingException => logger.error(e.getLocalizedMessage, e)
        complete(StatusCodes.InternalServerError, "Microsoft IMAP error: " + e.getLocalizedMessage)
    }
  }
}
