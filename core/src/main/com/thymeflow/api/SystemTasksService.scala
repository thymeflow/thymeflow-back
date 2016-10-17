package com.thymeflow.api

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import akka.util.Timeout
import com.thymeflow.Supervisor
import com.thymeflow.actors.ActorSystemContext
import com.thymeflow.api.SystemTasksService._
import com.thymeflow.service.{Done, Error, Idle, Working}
import com.thymeflow.utilities.JsonFormats
import spray.json._

import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

/**
  * @author David Montoya
  */
trait SystemTasksService extends Directives with CorsSupport {
  protected implicit val actorSystemContext: ActorSystemContext
  import JsonProtocol._
  import SprayJsonSupport._
  import actorSystemContext.Implicits._

  implicit val timeout = Timeout(5 seconds)

  protected def supervisor: Supervisor.Interactor

  def listTasks() = {
    supervisor.listTasks().map {
      serviceAccountTasks =>
        serviceAccountTasks.map {
          case (taskId, serviceAccountTask) =>
            val (startDate, status, progress) = serviceAccountTask.status match {
              case Idle => (None, s"Idle", None)
              case Done(start, end) => (Some(start), s"Done $end", None)
              case Working(start, progressOption) =>
                val progressPercentageOption = progressOption.map {
                  progress => (BigDecimal(progress.value) / BigDecimal(progress.total) * 100).toInt
                }
                (Some(start), "Working", progressPercentageOption)
              case Error(start, end) =>
                (Some(start), s"Error $end", None)
            }
            ResourceObject(
              Some(taskId.toString),
              "system-task",
              Task(`type` = "service",
                startDate = startDate,
                name = List(serviceAccountTask.source.service.name, serviceAccountTask.source.accountId, serviceAccountTask.source.sourceName).mkString(" "),
                status = status,
                progress = progress)
            )
        }
    }
  }

  protected val systemTasksRoute = {
    corsHandler {
      get {
        complete {
          listTasks().map {
            tasks => ResourceObjects(tasks)
          }
        }
      }
    }
  }
}

object SystemTasksService {

  case class Task(`type`: String, name: String, startDate: Option[Instant], status: String, progress: Option[Int])

  case class ResourceObject[T](id: Option[String], `type`: String, attributes: T)

  case class ResourceObjects[T](data: Seq[ResourceObject[T]])

  object JsonProtocol extends DefaultJsonProtocol with JsonFormats.InstantJsonFormat {
    implicit val printer: CompactPrinter = CompactPrinter
    implicit val taskFormat: RootJsonFormat[Task] = jsonFormat5(Task)

    implicit def resourceObjectFormat[T](implicit innerFormat: JsonFormat[T]): RootJsonFormat[ResourceObject[T]] = jsonFormat3(ResourceObject[T])

    implicit def resourceObjectsFormat[T](implicit resourceObjectFormat: JsonFormat[ResourceObject[T]]): RootJsonFormat[ResourceObjects[T]] = jsonFormat1(ResourceObjects[T])
  }

}
