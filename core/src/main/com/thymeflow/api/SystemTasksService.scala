package com.thymeflow.api

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import com.thymeflow.api.SystemTasksService._
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
          ResourceObjects(Vector(
            ResourceObject(Some("1"), "task", Task("loading", "Files: file.zip", Instant.now, "Done", Some(100))),
            ResourceObject(Some("2"), "task", Task("loading", "CalDAV: angela@example.com", Instant.now, "In progress", Some((Instant.now.getEpochSecond % 100).toInt))),
            ResourceObject(Some("3"), "task", Task("loading", "CardDAV: angela@example.com", Instant.now, "In progress", None)),
            ResourceObject(Some("4"), "task", Task("loading", "Email: angela@example2.com", Instant.now, "Waiting", Some(0)))
          ))
        }
      }
    }
  }
}

object SystemTasksService {

  case class Task(`type`: String, name: String, startDate: Instant, status: String, progress: Option[Int])

  case class ResourceObject[T](id: Option[String], `type`: String, attributes: T)

  case class ResourceObjects[T](data: scala.collection.immutable.Seq[ResourceObject[T]])

  object JsonProtocol extends DefaultJsonProtocol with JsonFormats.InstantJsonFormat {
    implicit val printer: CompactPrinter = CompactPrinter
    implicit val taskFormat: RootJsonFormat[Task] = jsonFormat5(Task)

    implicit def resourceObjectFormat[T](implicit innerFormat: JsonFormat[T]): RootJsonFormat[ResourceObject[T]] = jsonFormat3(ResourceObject[T])

    implicit def resourceObjectsFormat[T](implicit resourceObjectFormat: JsonFormat[ResourceObject[T]]): RootJsonFormat[ResourceObjects[T]] = jsonFormat1(ResourceObjects[T])
  }

}
