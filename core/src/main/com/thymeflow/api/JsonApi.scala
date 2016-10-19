package com.thymeflow.api

import com.thymeflow.utilities.JsonFormats
import spray.json.{CompactPrinter, DefaultJsonProtocol, JsonFormat, RootJsonFormat}

/**
  * @author David Montoya
  */
trait JsonApi {

}


object JsonApi {

  case class ResourceObject[T](id: Option[String], `type`: String, attributes: T)

  case class ResourceObjects[T](data: Seq[ResourceObject[T]])

  trait JsonProtocol extends DefaultJsonProtocol with JsonFormats.InstantJsonFormat {
    implicit val printer: CompactPrinter = CompactPrinter

    implicit def resourceObjectFormat[T](implicit innerFormat: JsonFormat[T]): RootJsonFormat[ResourceObject[T]] = jsonFormat3(ResourceObject[T])

    implicit def resourceObjectsFormat[T](implicit resourceObjectFormat: JsonFormat[ResourceObject[T]]): RootJsonFormat[ResourceObjects[T]] = jsonFormat1(ResourceObjects[T])
  }

}
