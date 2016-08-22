package com.thymeflow.rdf.sail.inferencer

import org.openrdf.sail.inferencer.InferencerConnection
import org.openrdf.sail.inferencer.fc.AbstractForwardChainingInferencer
import org.openrdf.sail.{NotifyingSail, SailException}

/**
  * The baseSail should do RDFS inferencing
  *
  * @author Thomas Pellissier Tanon
  */
class ForwardChainingSimpleOWLInferencer(baseSail: NotifyingSail)
  extends AbstractForwardChainingInferencer(baseSail) {

  override def initialize(): Unit = {
    super.initialize()
    val con = getConnection
    try {
      con.begin()
      con.addAxiomStatements()
      con.commit()
    } finally {
      con.close()
    }
  }

  override def getConnection: ForwardChainingSimpleOWLInferencerConnection = {
    try {
      new ForwardChainingSimpleOWLInferencerConnection(this, super.getConnection.asInstanceOf[InferencerConnection])
    }
    catch {
      case e: ClassCastException => throw new SailException(e.getMessage, e)
    }
  }
}
