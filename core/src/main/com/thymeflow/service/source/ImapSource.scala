package com.thymeflow.service.source

import javax.mail.Store

/**
  * @author David Montoya
  */
case class ImapSource(connect: () => Store, folderNamesToKeep: Option[Set[String]] = None) extends Source
