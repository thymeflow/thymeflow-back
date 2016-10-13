package com.thymeflow.service

import java.util.Properties
import javax.mail.Session

import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.thymeflow.actors.ActorSystemContext
import com.thymeflow.service.source._
import com.thymeflow.sync.OAuth2
import com.typesafe.config.Config
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.Future

/**
  * @author David Montoya
  */
object Microsoft extends Service with OAuth2Service with DefaultJsonProtocol with SprayJsonSupport {
  val name = "Microsoft"
  val routeName = "microsoft"

  final val apiEndpoint = Uri("https://apis.live.net")
  final val apiPath = Path("/v5.0")

  case class Emails(account: String)

  case class Me(id: String, name: Option[String], emails: Emails)

  implicit val emailsFormat = jsonFormat1(Emails)
  implicit val meFormat: RootJsonFormat[Me] = jsonFormat3(Me)

  def account(accessToken: String)(implicit config: Config, actorContext: ActorSystemContext): Future[ServiceAccount] = {
    Http().singleRequest(HttpRequest(HttpMethods.GET, apiEndpoint.withPath(apiPath / "me").withQuery(
      Query(
        ("access_token", accessToken)
      )
    ))).flatMap {
      case HttpResponse(StatusCodes.OK, _, entity, _) =>
        Unmarshal(entity.withContentType(ContentTypes.`application/json`)).to[Me].flatMap {
          me =>
            // Imap Message Store
            def connect() = {
              val props = new Properties()
              props.put("mail.imap.ssl.enable", "true")
              props.put("mail.imap.auth.mechanisms", "XOAUTH2")

              val store = Session.getInstance(props).getStore("imap")
              store.connect("imap-mail.outlook.com", me.emails.account, accessToken)
              store
            }

            Future.successful(ServiceAccount(me.emails.account, Map("mainImap" -> ImapSource(connect))))
        }
    }
  }

  def oAuth2(redirectUri: String)(implicit config: Config) = OAuth2(
    "https://login.live.com/oauth20_authorize.srf", //https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
    "https://login.live.com/oauth20_token.srf", //"https://login.microsoftonline.com/common/oauth2/v2.0/token",
    clientId = config.getString("thymeflow.oauth.microsoft.client-id"),
    clientSecret = config.getString("thymeflow.oauth.microsoft.client-secret"),
    redirectUri
  )("wl.imap", "wl.offline_access", "wl.emails")
}