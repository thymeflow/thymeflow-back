package com.thymeflow.api

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

/**
  * @author David Montoya
  */
trait CorsSupport {
  private val config = com.thymeflow.config.default

  lazy val allowedOriginHeader = {
    val sAllowedOrigin = config.getString("thymeflow.http.frontend-uri")
    if (sAllowedOrigin == "*")
      `Access-Control-Allow-Origin`.*
    else
      `Access-Control-Allow-Origin`(HttpOrigin(sAllowedOrigin))
  }

  private def addAccessControlHeaders = {
    mapResponseHeaders { headers =>
      allowedOriginHeader +:
        `Access-Control-Allow-Credentials`(true) +:
        `Access-Control-Allow-Headers`("Token", "Content-Type", "Content-Range", "Content-Disposition", "X-Requested-With", "Accept", "Origin") +:
        headers
    }
  }

  private def preflightRequestHandler = options {
    complete(HttpResponse(200).withHeaders(
      `Access-Control-Allow-Methods`(OPTIONS, POST, PATCH, PUT, GET, DELETE)
    )
    )
  }

  def corsHandler(r: Route) = addAccessControlHeaders {
    preflightRequestHandler ~ r
  }
}
