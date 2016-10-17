package com.thymeflow.api

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import com.thymeflow.Supervisor
import com.thymeflow.actors.ActorSystemContext
import com.thymeflow.rdf.repository.Repository
import com.thymeflow.service._
import com.thymeflow.service.authentication.OAuth2
import com.typesafe.config.Config

import scala.annotation.tailrec
import scala.concurrent.duration.Duration

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
trait Api extends SparqlService with SystemTasksService with CorsSupport {
  protected implicit val actorSystemContext: ActorSystemContext

  import actorSystemContext.Implicits._
  protected implicit def config: Config
  private val backendUri = Uri(config.getString("thymeflow.http.backend-uri").replaceAll("/$", ""))
  private val frontendUri = Uri(config.getString("thymeflow.http.frontend-uri"))
  protected override val allowedOrigin = frontendUri.withPath(Path.Empty).toString

  def oAuthTokenUri(serviceName: String) = {
    backendUri.withPath(backendUri.path / "oauth" / serviceName / "token").toString
  }

  private val oAuth2Services = Vector(Google, Microsoft, Facebook)

  private val uploadsPath = Paths.get(config.getString("thymeflow.data-directory"), "uploads")

  private val route = {
    path("sparql") {
      sparqlRoute
    } ~
      pathPrefix("oauth") {
        oAuth2Services.foldLeft[Route](reject) {
          case (p, service) =>
            val oAuth2 = service.oAuth2(oAuthTokenUri(service.routeName))
            p ~ pathPrefix(service.routeName) {
              path("auth") {
                redirect(oAuth2.authUri, StatusCodes.TemporaryRedirect)
              } ~
                path("token") {
                  parameter('code) { code =>
                    logger.info(s"${service.name} token received at time $durationSinceStart")
                    oAuth2.accessToken(code).foreach(oAuth2TokenRenewal(_, onOAuth2Token(service)))
                    redirect(frontendUri, StatusCodes.TemporaryRedirect)
                  }
                }
            }
        }
      } ~
      pathPrefix(Email.routeName) {
        post {
          formFieldMap { fields =>
            logger.info(s"IMAP account on ${fields("host")} received at time $durationSinceStart")
            Email.account(
              host = fields("host"),
              user = fields("user"),
              password = fields("password"),
              ssl = fields.get("ssl").contains("true")
            ).foreach(onAccount)
            redirect(frontendUri, StatusCodes.TemporaryRedirect)
          }
        }
      } ~
      path(File.routeName) {
        corsHandler {
          uploadedFile("file") {
            case (fileInfo, file) =>
              logger.info(s"File ${fileInfo.fileName} uploaded at time $durationSinceStart")
              File.account(file.toPath, Some(fileInfo.contentType.mediaType.value), Some(uploadsPath.resolve(fileInfo.fileName))).foreach(onAccount)
              complete(StatusCodes.NoContent, "")
          }
        }
      } ~
      path("system-tasks") {
        systemTasksRoute
      }
  }

  private def onAccount(account: ServiceAccount): Unit = {
    supervisor.addServiceAccount(account)
  }

  protected def supervisor: Supervisor.Interactor

  protected def repository: Repository

  private val executionStart: Long = System.currentTimeMillis()
  protected def durationSinceStart: Duration = {
    Duration(System.currentTimeMillis() - executionStart, TimeUnit.MILLISECONDS)
  }

  private def oAuth2TokenRenewal[T, R](token: OAuth2.RenewableToken[T], onNewToken: (OAuth2.RenewableToken[T] => R)): Unit = {
    onNewToken(token)
    token.onRefresh(() => onNewToken(token))
  }

  private def onOAuth2Token(service: OAuth2Service)(token: OAuth2.RenewableToken[_]): Unit = {
    service.account(token.accessToken).foreach(onAccount)
  }

  def start(): Unit = {
    @tailrec
    def segments(path: Path, tailSegments: List[String] = List.empty): List[String] = {
      if (path.isEmpty) {
        tailSegments
      } else {
        if (path.startsWithSegment) {
          segments(path.tail, path.head.toString :: tailSegments)
        } else {
          segments(path.tail, tailSegments)
        }
      }
    }
    val prefixedRoute = segments(backendUri.path).foldLeft(route) {
      case (nestedRoute, segment) =>
        pathPrefix(segment) {
          nestedRoute
        }
    }
    val listenInterface = config.getString("thymeflow.http.listen-interface")
    val listenPort = config.getInt("thymeflow.http.listen-port")

    Http().bindAndHandle(prefixedRoute, listenInterface, listenPort)

    logger.info(s"Thymeflow API bound to $listenInterface:$listenPort. BackendUri is $backendUri. FrontendUri is $frontendUri.")
  }
}
