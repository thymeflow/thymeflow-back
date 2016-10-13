package com.thymeflow.service.source

import java.nio.file.Path

/**
  * @author David Montoya
  */
case class PathSource(path: Path, mimeType: Option[String] = None, documentPath: Option[Path] = None) extends Source