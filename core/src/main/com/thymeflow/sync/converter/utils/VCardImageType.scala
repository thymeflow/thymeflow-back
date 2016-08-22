package com.thymeflow.sync.converter.utils

import ezvcard.parameter.ImageType

/**
  * @author Thomas Pellissier Tanon
  * TODO: contribute to Ezvcard?
  */
object VCardImageType {
  private val fileStart = Map(
    ImageType.JPEG -> Array(0xFF.toByte, 0xD8.toByte, 0xFF.toByte),
    ImageType.GIF -> Array('G', 'I', 'F'),
    ImageType.PNG -> Array(0x89.toByte, 0x50.toByte, 0x4e.toByte, 0x47.toByte, 0x0d.toByte, 0x0a.toByte)
  )

  def guess(content: Array[Byte]): Option[ImageType] = {
    fileStart.foreach {
      case (imageType, start) =>
        if(start.indices.forall(pos => content(pos) == start(pos))) {
          return Some(imageType)
        }
    }
    None
  }
}
