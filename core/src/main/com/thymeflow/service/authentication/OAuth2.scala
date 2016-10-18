package com.thymeflow.service.authentication

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import com.thymeflow.service.authentication.OAuth2.{RefreshedToken, RenewableToken}
import com.typesafe.scalalogging.StrictLogging
import spray.json.DefaultJsonProtocol

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  *
  */
object OAuth2 {

  case class RefreshedToken(access_token: String, token_type: String, expires_in: Long)

  trait RenewableToken {
    def accessToken: String

    def refreshAction(implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext): Option[() => Future[RenewableToken]]

    def expiresIn: Long
  }
}

case class OAuth2(authorizeUri: String, tokenUri: String, clientId: String, clientSecret: String, redirectUri: String)(scopes: String*)
  extends SprayJsonSupport with DefaultJsonProtocol with StrictLogging {

  def authUri: Uri = {
    Uri(authorizeUri).withQuery(Query(
      ("scope", scopes.mkString(" ")),
      ("redirect_uri", redirectUri),
      ("response_type", "code"),
      ("client_id", clientId),
      ("access_type", "offline"),
      ("prompt", "consent")
    ))
  }

  def accessToken(code: String)(implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext): Future[Token] = {
    implicit val TokenFormat = jsonFormat6(Token)
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


  case class Token(access_token: String, token_type: String, expires_in: Long, refresh_token: Option[String], id_token: Option[String], user_id: Option[String]) extends RenewableToken {
    override def expiresIn = expires_in

    override def accessToken = access_token

    override def refreshAction(implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext): Option[() => Future[Token]] = {
      implicit val RefreshedTokenFormat = jsonFormat3(RefreshedToken)
      refresh_token.map {
        refreshToken =>
          () => {
            val future = Http().singleRequest(HttpRequest(HttpMethods.POST, tokenUri).withEntity(FormData(Query(
              ("refresh_token", refreshToken),
              ("client_id", clientId),
              ("client_secret", clientSecret),
              ("grant_type", "refresh_token")
            )).toEntity)).flatMap {
              case HttpResponse(StatusCodes.OK, _, entity, _) =>
                Unmarshal(entity).to[RefreshedToken].map(refreshedToken => {
                  this.copy(access_token = refreshedToken.access_token,
                    token_type = refreshedToken.token_type,
                    expires_in = refreshedToken.expires_in)
                })
            }
            future.onFailure {
              case e => logger.error("Error refreshing token.", e)
            }
            future
          }
      }
    }
  }

}

