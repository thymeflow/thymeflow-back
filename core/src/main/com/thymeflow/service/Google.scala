package com.thymeflow.service

import java.util.Properties

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.thymeflow.actors.ActorSystemContext
import com.thymeflow.service.authentication.OAuth2
import com.thymeflow.service.source.{CalDavSource, CardDavSource, ImapSource}
import com.typesafe.config.Config
import javax.mail.Session

import scala.concurrent.Future

/**
  * @author David Montoya
  */
object Google extends Service with OAuth2Service {
  val name = "Google"
  val routeName = "google"

  def account(accessToken: String)(implicit config: Config, actorContext: ActorSystemContext): Future[ServiceAccount] = {
    import actorContext.Implicits._
    val authorization = Authorization(OAuth2BearerToken(accessToken))
    Http().singleRequest(HttpRequest(uri = Uri("https://www.googleapis.com/userinfo/email"), headers = List(authorization))).flatMap {
      case HttpResponse(StatusCodes.OK, _, entity, _) =>
        Unmarshal(entity).to[String].map(
          _.split("&")(0).split("=")(1) //The result has the format "email=foo@gmail.com&..."
        ).map {
          googleAddress => {
            // Imap Message Store
            def imapConnect() = {
              val props = new Properties()
              props.put("mail.imap.ssl.enable", "true")
              props.put("mail.imap.auth.mechanisms", "XOAUTH2")

              val store = Session.getInstance(props).getStore("imap")

              store.connect("imap.gmail.com", googleAddress, accessToken)
              store
            }

            val inboxOnly = config.getBoolean("thymeflow.synchronizer.email-synchronizer.google.inbox-only")

            ServiceAccount(this,
              googleAddress,
              Map(
                // CardDav
                "Contacts" -> CardDavSource("https://www.googleapis.com/.well-known/carddav", accessToken),
                // CalDav
                "Calendar" -> CalDavSource(s"https://apidata.googleusercontent.com/caldav/v2/${googleAddress}/events/", accessToken),
                // Imap
                "Emails" -> ImapSource(() => imapConnect(), folderNamesToKeep = if (inboxOnly) Some(Set("INBOX")) else None)
              )
            )
          }
        }
    }
  }

  def oAuth2(redirectUri: String)(implicit config: Config) = OAuth2(
    "https://accounts.google.com/o/oauth2/v2/auth",
    "https://www.googleapis.com/oauth2/v4/token",
    clientId = config.getString("thymeflow.oauth.google.client-id"),
    clientSecret = config.getString("thymeflow.oauth.google.client-secret"),
    redirectUri
  )(
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/carddav",
    "https://www.googleapis.com/auth/calendar",
    "https://mail.google.com/"
  )
}