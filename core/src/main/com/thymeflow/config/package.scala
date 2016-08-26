package com.thymeflow

import com.typesafe.config.ConfigFactory

/**
  * @author David Montoya
  */
package object config {
  private lazy val applicationConfig = ConfigFactory.load()
  private lazy val cliConfig = ConfigFactory.load("cli")

  def application = applicationConfig

  def cli = cliConfig
}
