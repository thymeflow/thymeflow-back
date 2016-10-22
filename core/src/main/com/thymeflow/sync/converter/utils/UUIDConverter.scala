package com.thymeflow.sync.converter.utils

import java.util.UUID

import org.openrdf.model.{BNode, IRI, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
class UUIDConverter(valueFactory: ValueFactory) {

  def uuidBasedOnString(base: String) = {
    UUID.nameUUIDFromBytes(base.getBytes)
  }

  def convert(rawUuid: String): IRI = {
    val simplifiedUuid = rawUuid.replace("urn:uuid:", "")
    try {
      valueFactory.createIRI("urn:uuid:" + UUID.fromString(simplifiedUuid).toString)
    } catch {
      case e: IllegalArgumentException =>
        createIRI(simplifiedUuid)
    }
  }

  def createIRI(base: Any, iriCreator: String => IRI): IRI = {
    iriCreator(uuidBasedOnString(base.toString).toString)
  }

  def createIRI(base: Any): IRI = {
    createIRI(base, (s) => valueFactory.createIRI("urn:uuid:" + s))
  }

  def createBNode(base: Any): BNode = {
    valueFactory.createBNode(uuidBasedOnString(base.toString).toString)
  }
}
