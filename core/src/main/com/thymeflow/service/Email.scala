package com.thymeflow.service

import java.util.Properties

import akka.http.scaladsl.model.Uri
import com.thymeflow.actors.ActorSystemContext
import com.thymeflow.service.source.ImapSource
import com.typesafe.config.Config
import javax.mail.Session

import scala.concurrent.Future

/**
  * @author David Montoya
  */
object Email extends Service {
  def name = "Email"

  def routeName = "imap"

  def account(host: String,
              user: String,
              password: String,
              ssl: Boolean)(implicit config: Config, actorContext: ActorSystemContext): Future[ServiceAccount] = {
    // Imap Message Store
    def connect() = {
      val props = new Properties()
      if (ssl) {
        props.put("mail.imap.ssl.enable", "true")
      }
      val store = Session.getInstance(props).getStore("imap")
      store.connect(host, user, password)
      store
    }

    val accountId = Uri(scheme = "imap", authority = Uri.Authority(host = Uri.Host(host), userinfo = user)).toString()
    Future.successful(ServiceAccount(this, accountId, Map("Emails" -> ImapSource(() => connect))))
  }

}
