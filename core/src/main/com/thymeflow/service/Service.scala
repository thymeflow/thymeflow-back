package com.thymeflow.service

import java.time.Instant

import com.thymeflow.actors.ActorSystemContext
import com.thymeflow.service.source.Source
import com.thymeflow.sync.OAuth2
import com.typesafe.config.Config

import scala.concurrent.Future

/**
  * @author David Montoya
  */
trait Service {
  def name: String

  def routeName: String
}

case class ServiceAccount(id: String, sources: Map[String, Source])

case class ServiceAccountTask(service: Service, account: ServiceAccount, name: String, status: TaskStatus)

sealed trait TaskStatus

case object Idle extends TaskStatus

case class Progress(startDate: Instant, progress: Option[Int]) extends TaskStatus

case class Done(startDate: Instant, endDate: Instant) extends TaskStatus

trait OAuth2Service extends Service {
  def oAuth2(redirectUri: String)(implicit config: Config): OAuth2

  def account(accessToken: String)(implicit config: Config, actorContext: ActorSystemContext): Future[ServiceAccount]
}
