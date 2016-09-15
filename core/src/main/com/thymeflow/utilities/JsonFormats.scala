package com.thymeflow.utilities

import java.time.Instant

import spray.json.{JsString, JsValue, JsonFormat}

/**
  * @author David Montoya
  */
object JsonFormats {

  trait InstantJsonFormat {
    implicit val instantFormat = new JsonFormat[Instant] {
      override def read(json: JsValue): Instant = json match {
        case JsString(s) => Instant.parse(s)
        case _ => throw new IllegalArgumentException(s"Invalid Instant: expected JsString, got $json.")
      }

      override def write(obj: Instant): JsValue = JsString(obj.toString)

    }
  }

}
