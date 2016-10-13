package com.thymeflow.sync

import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.thymeflow.actors._
import com.typesafe.scalalogging.StrictLogging
import spray.json.DefaultJsonProtocol

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

object OAuth2 {

  trait RenewableToken[Token] {
    def accessToken: String
    def onRefresh[T](f: () => T): Unit
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
      ("access_type", "offline")
    ))
  }

  def accessToken(code: String): Future[Token] = {
    implicit val TokenFormat = jsonFormat(Token, "access_token", "token_type", "expires_in", "refresh_token", "id_token", "user_id")
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

  case class Token(private var access_token: String, private var token_type: String, private var expires_in: Long, private val refresh_token: Option[String], private val id_token: Option[String], user_id: Option[String])
    extends OAuth2.RenewableToken[Token] {

    private val refreshCallbacks = new ArrayBuffer[() => Any]()

    override def accessToken: String = access_token

    refresh_token.foreach(refreshToken =>
      system.scheduler.scheduleOnce((expires_in - 10) seconds)(refresh(refreshToken))
    )

    override def onRefresh[T](f: () => T): Unit = {
      refreshCallbacks :+ f
    }

    private def refresh(refreshToken: String): Unit = {
      implicit val RefreshedTokenFormat = jsonFormat3(RefreshedToken)
      Http().singleRequest(HttpRequest(HttpMethods.POST, tokenUri).withEntity(FormData(Query(
        ("refresh_token", refreshToken),
        ("client_id", clientId),
        ("client_secret", clientSecret),
        ("grant_type", "refresh_token")
      )).toEntity)).foreach {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          Unmarshal(entity).to[RefreshedToken].foreach(refreshedToken => {
            access_token = refreshedToken.access_token
            token_type = refreshedToken.token_type
            expires_in = refreshedToken.expires_in
            system.scheduler.scheduleOnce((expires_in - 10) seconds)(refresh(refreshToken))
          })
      }
    }
  }

  case class RefreshedToken(access_token: String, token_type: String, expires_in: Long)
}

