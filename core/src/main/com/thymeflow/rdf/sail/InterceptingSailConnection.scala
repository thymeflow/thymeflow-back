package com.thymeflow.rdf.sail

import org.eclipse.rdf4j.model.{IRI, Resource, Value, ValueFactory}
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper
import org.eclipse.rdf4j.sail.{NotifyingSailConnection, UpdateContext}

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
class InterceptingSailConnection(wrappedSailConnection: NotifyingSailConnection, valueFactory: ValueFactory, interceptor: SailInterceptor) extends NotifyingSailConnectionWrapper(wrappedSailConnection) {
  override def addStatement(updateContext: UpdateContext, subj: Resource, pred: IRI, obj: Value, contexts: Resource*) {
    if (interceptor.onSparqlAddStatement(subj, pred, obj, contexts: _*)(valueFactory)) {
      wrappedSailConnection.addStatement(updateContext, subj, pred, obj, contexts: _*)
    }
  }

  override def removeStatement(updateContext: UpdateContext, subj: Resource, pred: IRI, obj: Value, contexts: Resource*) {
    if (interceptor.onSparqlRemoveStatement(subj, pred, obj, contexts: _*)(valueFactory)) {
      wrappedSailConnection.removeStatement(updateContext, subj, pred, obj, contexts: _*)
    }
  }

  override def startUpdate(modify: UpdateContext) {
    wrappedSailConnection.startUpdate(modify)
  }

  override def endUpdate(modify: UpdateContext) {
    wrappedSailConnection.endUpdate(modify)
  }
}