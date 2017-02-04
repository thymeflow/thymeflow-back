package com.thymeflow.service

import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.thymeflow.actors.ActorSystemContext
import com.thymeflow.service.authentication.OAuth2
import com.thymeflow.service.source.FacebookGraphApiSource
import com.typesafe.config.Config
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.Future

/**
  * @author David Montoya
  */
object Facebook extends Service with OAuth2Service with DefaultJsonProtocol with SprayJsonSupport {
  val name = "Facebook"
  val routeName = "facebook"

  final val namespace = "http://graph.facebook.com"
  final val apiEndpoint = Uri("https://graph.facebook.com")
  final val apiPath = Path("/v2.8")

  case class Me(id: String, name: Option[String], email: Option[String])

  implicit val meFormat: RootJsonFormat[Me] = jsonFormat3(Me)

  def account(accessToken: String)(implicit config: Config, actorContext: ActorSystemContext): Future[ServiceAccount] = {
    import actorContext.Implicits._
    Http().singleRequest(HttpRequest(HttpMethods.GET, apiEndpoint.withPath(apiPath / "me").withQuery(
      Query(
        ("access_token", accessToken),
        ("fields", "id,name,email")
      )
    ))).flatMap {
      case HttpResponse(StatusCodes.OK, _, entity, _) =>
        Unmarshal(entity.withContentType(ContentTypes.`application/json`)).to[Me].flatMap {
          me =>
            val accountId = me.email.getOrElse(me.name.getOrElse(me.id))
            Future.successful(ServiceAccount(this, accountId, Map("Facebook" -> FacebookGraphApiSource(accessToken))))
        }
    }
  }

  def oAuth2(redirectUri: String)(implicit config: Config) = OAuth2(
    "https://www.facebook.com/dialog/oauth",
    apiEndpoint.withPath(apiPath / "oauth" / "access_token").toString(),
    clientId = config.getString("thymeflow.oauth.facebook.client-id"),
    clientSecret = config.getString("thymeflow.oauth.facebook.client-secret"),
    redirectUri
  )("email",
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
    "rsvp_event")

}
