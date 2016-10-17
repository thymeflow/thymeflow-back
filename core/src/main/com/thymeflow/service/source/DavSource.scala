package com.thymeflow.service.source

/**
  * @author David Montoya
  */
trait DavSource extends Source {
  def baseUri: String

  def accessToken: String
}

case class CalDavSource(baseUri: String, accessToken: String) extends DavSource

case class CardDavSource(baseUri: String, accessToken: String) extends DavSource