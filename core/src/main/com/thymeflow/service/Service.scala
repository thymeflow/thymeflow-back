package com.thymeflow.service

import java.time.Instant

import com.thymeflow.actors.ActorSystemContext
import com.thymeflow.service.authentication.OAuth2
import com.thymeflow.service.source.Source
import com.typesafe.config.Config
import org.eclipse.rdf4j.model.IRI

import scala.concurrent.Future

/**
  * @author David Montoya
  */
trait Service {
  def name: String

  def routeName: String

  override def toString: String = name
}

case class ServiceAccount(service: Service, accountId: String, sources: Map[String, Source])

case class ServiceAccountSources(sources: Traversable[(ServiceAccountSource, Source)])

case class ServiceAccountSource(service: Service, accountId: String, sourceName: String, iri: IRI)

case class ServiceAccountSourceTask[+T <: TaskStatus](source: ServiceAccountSource, taskName: String, status: T)

sealed trait TaskStatus

case object Idle extends TaskStatus

case class Progress(value: Long, total: Long)

case class Working(startDate: Instant = Instant.now(), progress: Option[Progress] = None) extends TaskStatus

case class Done(startDate: Instant, endDate: Instant = Instant.now()) extends TaskStatus

case class Error(startDate: Instant, errorDate: Instant = Instant.now()) extends TaskStatus

trait OAuth2Service extends Service {
  def oAuth2(redirectUri: String)(implicit config: Config): OAuth2

  def account(accessToken: String)(implicit config: Config, actorContext: ActorSystemContext): Future[ServiceAccount]
}
