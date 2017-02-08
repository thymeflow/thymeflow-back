package com.thymeflow.rdf.sail

import com.thymeflow.rdf.model.{StatementSet, StatementSetDiff}
import org.eclipse.rdf4j.model.{IRI, Resource, Value, ValueFactory}
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper
import org.eclipse.rdf4j.sail.{NotifyingSailConnection, UpdateContext}

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
class InterceptingSailConnection(wrappedSailConnection: NotifyingSailConnection,
                                 valueFactory: ValueFactory,
                                 interceptor: SailInterceptor) extends NotifyingSailConnectionWrapper(wrappedSailConnection) {
  private var addedStatements = StatementSet.empty(valueFactory)
  private var removedStatements = StatementSet.empty(valueFactory)

  override def addStatement(updateContext: UpdateContext, subj: Resource, pred: IRI, obj: Value, contexts: Resource*) {
    addedStatements.add(subj, pred, obj, contexts: _*)
  }

  override def removeStatement(updateContext: UpdateContext, subj: Resource, pred: IRI, obj: Value, contexts: Resource*) {
    removedStatements.add(subj, pred, obj, contexts: _*)
  }

  override def startUpdate(modify: UpdateContext) {
    wrappedSailConnection.startUpdate(modify)
  }

  override def endUpdate(modify: UpdateContext) {
    wrappedSailConnection.endUpdate(modify)
    val diff = StatementSetDiff(addedStatements, removedStatements)
    addedStatements = StatementSet.empty(valueFactory)
    removedStatements = StatementSet.empty(valueFactory)
    interceptor.onSparqlUpdateDiff(diff)
  }
}