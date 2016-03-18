package pkb.api

import pkb.api.MainApi._
import spray.client.pipelining._
import spray.http.{FormData, Uri}
import spray.httpx.SprayJsonSupport._
import spray.httpx.unmarshalling._
import spray.json.DefaultJsonProtocol

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object OAuth2 {
  val Google = new OAuth2(
    "https://accounts.google.com/o/oauth2/v2/auth",
    "https://www.googleapis.com/oauth2/v4/token",
    "503500000487-nutchjm39fo3p5l171cqo1k5lsprjpau.apps.googleusercontent.com",
    "39tNR9btCqLNKIriJFY28Yop" //TODO: remove before making the repository public
  )
}

class OAuth2(authorizeUri: String, tokenUri: String, clientId: String, clientSecret: String) extends DefaultJsonProtocol {

  implicit
  val TokenFormat = jsonFormat5(Token)

  def getAuthUri(scopes: Traversable[String], redirectUri: String): Uri = {
    Uri(authorizeUri).withQuery(
      ("scope", scopes.mkString(" ")),
      ("redirect_uri", redirectUri),
      ("response_type", "code"),
      ("client_id", clientId)
    )
  }

  def getAccessToken(code: String, redirectUri: String): Future[String] = {
    val pipeline = sendReceive ~> unmarshal[Token]

    pipeline(Post(tokenUri, FormData(Seq(
      ("code", code),
      ("client_id", clientId),
      ("client_secret", clientSecret),
      ("redirect_uri", redirectUri),
      ("grant_type", "authorization_code")
    )))).map(_.access_token)
  }

  case class Token(access_token: String, token_type: String, expires_in: Long, refresh_token: String, id_token: String)
}
