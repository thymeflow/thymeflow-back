package pkb.sync.converter.utils

import java.util.UUID

import org.openrdf.model.{IRI, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
class UUIDConverter(valueFactory: ValueFactory) {

  def convert(rawUuid: String): IRI = {
    valueFactory.createIRI(buildUuidUri(rawUuid))
  }

  private def buildUuidUri(str: String): String = {
    val simplifiedStr = str.replace("urn:uuid", "")
    try {
      "urn:uuid:" + UUID.fromString(simplifiedStr).toString
    } catch {
      case e: IllegalArgumentException =>
        "urn:uuid:" + UUID.nameUUIDFromBytes(str.getBytes).toString
    }
  }
}
