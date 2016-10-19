package com.thymeflow.api

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import akka.util.Timeout
import com.thymeflow.Supervisor
import com.thymeflow.actors.ActorSystemContext
import com.thymeflow.api.JsonApi.{ResourceObject, ResourceObjects}
import com.thymeflow.api.SystemTasksService._
import com.thymeflow.service.{Done, Error, Idle, Working}
import spray.json.RootJsonFormat

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
            val (status, startDate, endDate, progress) = serviceAccountTask.status match {
              case Idle =>
                ("idle", None, None, None)
              case Done(start, end) =>
                ("done", Some(start), Some(end), None)
              case Working(start, progressOption) =>
                val progressPercentageOption = progressOption.collect {
                  case progress if progress.total != 0 =>
                    (BigDecimal(progress.value) / BigDecimal(progress.total) * 100).toInt
                }
                ("working", Some(start), None, progressPercentageOption)
              case Error(start, end) =>
                ("error", Some(start), Some(end), None)
            }
            ResourceObject(
              Some(taskId.toString),
              "system-task",
              Task(
                `type` = "synchronization",
                service = serviceAccountTask.source.service.name,
                account = serviceAccountTask.source.accountId,
                source = serviceAccountTask.source.sourceName,
                name = serviceAccountTask.taskName,
                status = status,
                startDate = startDate,
                endDate = endDate,
                progress = progress
              )
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

  case class Task(`type`: String,
                  service: String,
                  account: String,
                  source: String,
                  name: String,
                  status: String,
                  startDate: Option[Instant],
                  endDate: Option[Instant],
                  progress: Option[Int])

  object JsonProtocol extends JsonApi.JsonProtocol {
    implicit val taskFormat: RootJsonFormat[Task] = jsonFormat9(Task)
  }

}
