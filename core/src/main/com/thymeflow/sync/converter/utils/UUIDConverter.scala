package com.thymeflow.sync.converter.utils

import java.util.UUID

import org.eclipse.rdf4j.model.{BNode, IRI, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
class UUIDConverter(valueFactory: ValueFactory) {

  def uuidFromByteArrayBase(base: Array[Byte]) = {
    UUID.nameUUIDFromBytes(base)
  }

  def uuidFromBase(base: Any) = {
    base match {
      case byteArray: Array[Byte] => uuidFromByteArrayBase(byteArray)
      case _ => uuidFromByteArrayBase(base.toString.getBytes)
    }
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
    iriCreator(uuidFromBase(base).toString)
  }

  def createIRI(base: Any): IRI = {
    createIRI(base, (s) => valueFactory.createIRI("urn:uuid:" + s))
  }

  def createBNode(base: Any): BNode = {
    valueFactory.createBNode(uuidFromBase(base).toString)
  }
}
