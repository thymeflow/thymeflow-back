package pkb.sync

import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import pkb.actors._
import spray.json.DefaultJsonProtocol

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
}

class OAuth2(authorizeUri: String, tokenUri: String, clientId: String, clientSecret: String, redirectUri: String)
  extends SprayJsonSupport with DefaultJsonProtocol {

  implicit lazy val TokenFormat = jsonFormat5(Token)

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

  case class Token(access_token: String, token_type: String, expires_in: Long, refresh_token: String, id_token: String) {
    def renew(): Future[Token] = getAccessToken(refresh_token)

    def whenShouldBeRenewed = system.scheduler.schedule((expires_in - 1) seconds, (expires_in - 1) seconds)
  }
}
