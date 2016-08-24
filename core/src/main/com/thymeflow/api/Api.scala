package com.thymeflow.api

import java.nio.file.Paths
import java.util.Properties
import java.util.concurrent.TimeUnit
import javax.mail.{MessagingException, Session}

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import com.github.sardine.impl.SardineImpl
import com.thymeflow.Pipeline
import com.thymeflow.actors._
import com.thymeflow.sync._
import com.thymeflow.sync.facebook.FacebookSynchronizer
import org.apache.commons.io.IOUtils
import org.openrdf.repository.Repository

import scala.concurrent.duration.Duration

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
trait Api extends App with SparqlService with CorsSupport {

  private val config = com.thymeflow.config.default

  private val backendUri = Uri(s"${config.getString("thymeflow.http.backend-uri")}")
  private val frontendUri = Uri(s"${config.getString("thymeflow.http.frontend-uri")}")
  private val googleOAuth = OAuth2.Google(backendUri.withPath(Uri.Path("/oauth/google/token")).toString)
  private val microsoftOAuth = OAuth2.Microsoft(backendUri.withPath(Uri.Path("/oauth/microsoft/token")).toString)
  private val facebookOAuth = OAuth2.Facebook(backendUri.withPath(Uri.Path("/oauth/facebook/token")).toString)
  private val uploadsPath = Paths.get(config.getString("thymeflow.api.repository.data-directory"), "uploads")

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
                redirect(frontendUri, StatusCodes.TemporaryRedirect)
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
                  redirect(frontendUri, StatusCodes.TemporaryRedirect)
                }
              }
          } ~
          pathPrefix("facebook") {
            path("auth") {
              redirect(facebookOAuth.getAuthUri(Vector("email",
                "publish_actions",
                "user_about_me",
                "user_birthday",
                "user_education_history",
                "user_friends",
                "user_games_activity",
                "user_hometown",
                "user_likes",
                "user_location",
                "user_photos",
                "user_posts",
                "user_relationship_details",
                "user_relationships",
                "user_religion_politics",
                "user_status",
                "user_tagged_places",
                "user_videos",
                "user_website",
                "user_work_history",
                "user_events",
                "rsvp_event")), StatusCodes.TemporaryRedirect)
            } ~
              path("token") {
                parameter('code) { code =>
                  logger.info(s"Facebook token received at time $durationSinceStart")
                  facebookOAuth.getAccessToken(code).foreach(tokenRenewal(_, onFacebookToken))
                  redirect(frontendUri, StatusCodes.TemporaryRedirect)
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
              pipeline.addSourceConfig(EmailSynchronizer.Config(store))
            } catch {
              case e: MessagingException => logger.error(e.getLocalizedMessage, e)
                complete(StatusCodes.InternalServerError, "IMAP error: " + e.getLocalizedMessage)
            }
            redirect(frontendUri, StatusCodes.TemporaryRedirect)
          }
        }
      } ~
      path("upload") {
        corsHandler {
          uploadedFile("file") {
            case (fileInfo, file) =>
              logger.info(s"File ${fileInfo.fileName} uploaded at time $durationSinceStart")
              pipeline.addSourceConfig(
                FileSynchronizer.Config(file.toPath, Some(fileInfo.contentType.mediaType.value), Some(uploadsPath.resolve(fileInfo.fileName)))
              )
              complete {
                StatusCodes.NoContent
              }
          }
        }
      }
  }

  protected def pipeline: Pipeline

  protected def repository: Repository

  Http().bindAndHandle(route, backendUri.authority.host.toString(), backendUri.effectivePort)

  logger.info(s"Thymeflow API setup at $backendUri.")

  protected def durationSinceStart: Duration = {
    Duration(System.currentTimeMillis() - executionStart, TimeUnit.MILLISECONDS)
  }

  private def tokenRenewal[T <: OAuth2.RenewableToken[T], R](token: T, onNewToken: (T => R)): Unit = {
    onNewToken(token)
    token.onRefresh(() => onNewToken(token))
  }

  private def onGoogleToken(token: googleOAuth.Token): Unit = {
    val sardine = new SardineImpl(token.accessToken)

    //Get user email
    val gmailAddress = IOUtils.toString(sardine.get("https://www.googleapis.com/userinfo/email"))
      .split("&")(0).split("=")(1) //The result has the format "email=foo@gmail.com&..."

    //CardDav
    pipeline.addSourceConfig(
      CardDavSynchronizer.Config(sardine, "https://www.googleapis.com/.well-known/carddav")
    )

    //CalDav
    pipeline.addSourceConfig(
      CalDavSynchronizer.Config(sardine, "https://apidata.googleusercontent.com/caldav/v2/" + gmailAddress + "/events/")
    )

    //Emails
    val props = new Properties()
    props.put("mail.imap.ssl.enable", "true")
    props.put("mail.imap.auth.mechanisms", "XOAUTH2")

    try {
      val store = Session.getInstance(props).getStore("imap")
      store.connect("imap.gmail.com", gmailAddress, token.accessToken)
      val inboxOnly = config.getBoolean("thymeflow.synchronizer.email-synchronizer.google.inbox-only")
      pipeline.addSourceConfig(EmailSynchronizer.Config(store, folderNamesToKeep = if (inboxOnly) Some(Set("INBOX")) else None))
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
      store.connect("imap-mail.outlook.com", token.user_id.get, token.accessToken)
      pipeline.addSourceConfig(EmailSynchronizer.Config(store))
    } catch {
      case e: MessagingException => logger.error(e.getLocalizedMessage, e)
        complete(StatusCodes.InternalServerError, "Microsoft IMAP error: " + e.getLocalizedMessage)
    }
  }

  private def onFacebookToken(token: facebookOAuth.Token): Unit = {
    pipeline.addSourceConfig(FacebookSynchronizer.Config(token.accessToken))
  }
}
