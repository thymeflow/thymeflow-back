package pkb.sync.converter.utils

import java.util.UUID

import org.openrdf.model.{IRI, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
class UUIDConverter(valueFactory: ValueFactory) {

  def convert(rawUuid: String): IRI = {
    val simplifiedUuid = rawUuid.replace("urn:uuid:", "")
    try {
      valueFactory.createIRI("urn:uuid:" + UUID.fromString(simplifiedUuid).toString)
    } catch {
      case e: IllegalArgumentException =>
        create(simplifiedUuid)
    }
  }

  def create(baseStr: String): IRI = {
    valueFactory.createIRI("urn:uuid:" + UUID.nameUUIDFromBytes(baseStr.getBytes).toString)
  }
}
