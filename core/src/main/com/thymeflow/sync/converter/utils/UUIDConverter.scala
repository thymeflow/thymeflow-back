package com.thymeflow.sync.converter.utils

import java.util.UUID

import org.openrdf.model.{BNode, IRI, ValueFactory}

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
        createIRI(simplifiedUuid)
    }
  }

  def createIRI(base: Any): IRI = {
    valueFactory.createIRI("urn:uuid:" + UUID.nameUUIDFromBytes(base.toString.getBytes).toString)
  }

  def createBNode(base: Any): BNode = {
    valueFactory.createBNode(UUID.nameUUIDFromBytes(base.toString.getBytes).toString)
  }
}
