package com.thymeflow.service

import java.nio.file.{Path => JavaPath}

import com.thymeflow.actors.ActorSystemContext
import com.thymeflow.service.source.PathSource
import com.typesafe.config.Config

import scala.concurrent.Future

/**
  * @author David Montoya
  */
object File extends Service {
  def name = "File"

  def routeName = "upload"

  def account(path: JavaPath,
              mimeType: Option[String] = None,
              documentPath: Option[JavaPath] = None)(implicit config: Config, actorContext: ActorSystemContext): Future[ServiceAccount] = {
    Future.successful(ServiceAccount(documentPath.getOrElse(path).toAbsolutePath.toString, Map("main" -> PathSource(path, mimeType, documentPath))))
  }

}
