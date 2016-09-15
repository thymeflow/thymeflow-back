package com.thymeflow.api

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import com.thymeflow.api.SystemTasksService.{JsonProtocol, Task, Tasks}
import com.thymeflow.utilities.JsonFormats
import spray.json._

import scala.language.implicitConversions

/**
  * @author David Montoya
  */
trait SystemTasksService extends Directives with CorsSupport {

  import JsonProtocol._
  import SprayJsonSupport._

  protected val systemTasksRoute = {
    corsHandler {
      get {
        complete {
          Tasks(List(
            Task("loading", "Files: file.zip", Instant.now, "Done", Some(100)),
            Task("loading", "CalDAV: angela@example.com", Instant.now, "In progress", Some((Instant.now.getEpochSecond % 100).toInt)),
            Task("loading", "CardDAV: angela@example.com", Instant.now, "In progress", None),
            Task("loading", "Email: angela@example2.com", Instant.now, "Waiting", Some(0))
          ))
        }
      }
    }
  }
}

object SystemTasksService {

  case class Task(`type`: String, name: String, startDate: Instant, status: String, progress: Option[Int])

  case class Tasks(tasks: List[Task])

  object JsonProtocol extends DefaultJsonProtocol with JsonFormats.InstantJsonFormat {
    implicit val printer: CompactPrinter = CompactPrinter
    implicit val taskFormat: JsonFormat[Task] = jsonFormat5(Task)
    implicit val tasksFormat: RootJsonFormat[Tasks] = jsonFormat1(Tasks)
  }

}
