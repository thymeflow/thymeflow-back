package thymeflow.sync

import akka.actor.Cancellable
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import spray.json.DefaultJsonProtocol
import thymeflow.actors._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

object OAuth2 {
  def Google(redirectUri: String) = new OAuth2(
    "https://accounts.google.com/o/oauth2/v2/auth",
    "https://www.googleapis.com/oauth2/v4/token",
    "503500000487-nutchjm39fo3p5l171cqo1k5lsprjpau.apps.googleusercontent.com",
    "39tNR9btCqLNKIriJFY28Yop", //TODO: remove before making the repository public
    redirectUri
  )

  def Microsoft(redirectUri: String) = new OAuth2(
    "https://login.live.com/oauth20_authorize.srf", //https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
    "https://login.live.com/oauth20_token.srf", //"https://login.microsoftonline.com/common/oauth2/v2.0/token",
    "0000000044185A2B",
    "O0RVtj-cfkiINpVVdc-LrF-3euqHf2cW", //TODO: remove before making the repository public
    redirectUri
  )
}

class OAuth2(authorizeUri: String, tokenUri: String, clientId: String, clientSecret: String, redirectUri: String)
  extends SprayJsonSupport with DefaultJsonProtocol {

  implicit lazy val TokenFormat = jsonFormat6(Token)

  def getAuthUri(scopes: Traversable[String]): Uri = {
    Uri(authorizeUri).withQuery(Query(
      ("scope", scopes.mkString(" ")),
      ("redirect_uri", redirectUri),
      ("response_type", "code"),
      ("client_id", clientId)
    ))
  }

  def getAccessToken(code: String): Future[Token] = {
    Http().singleRequest(HttpRequest(HttpMethods.POST, tokenUri).withEntity(FormData(Query(
      ("code", code),
      ("client_id", clientId),
      ("client_secret", clientSecret),
      ("redirect_uri", redirectUri),
      ("grant_type", "authorization_code")
    )).toEntity)).flatMap {
      case HttpResponse(StatusCodes.OK, _, entity, _) =>
        Unmarshal(entity).to[Token]
    }
  }

  case class Token(access_token: String, token_type: String, expires_in: Long, refresh_token: Option[String], id_token: Option[String], user_id: Option[String]) {
    def renew(): Future[Token] = refresh_token.map(getAccessToken(_)).get //TODO: avoid hard fail

    def onShouldBeRenewed(f: ⇒ Unit): Cancellable =
      system.scheduler.schedule((expires_in - 1) seconds, (expires_in - 1) seconds)(thymeflow.actors.executor)
  }
}
