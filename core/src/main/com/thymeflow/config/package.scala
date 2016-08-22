package com.thymeflow

import com.typesafe.config.ConfigFactory

/**
  * @author David Montoya
  */
package object config {
  private lazy val defaultConfig = ConfigFactory.load()

  def default = defaultConfig
}
