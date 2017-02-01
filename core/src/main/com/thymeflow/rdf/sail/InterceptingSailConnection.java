package com.thymeflow.rdf.sail;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper;

/**
 * @author Thomas Pellissier Tanon
 */
class InterceptingSailConnection extends NotifyingSailConnectionWrapper {

    private NotifyingSailConnection wrappedSailConnection;
    private SailInterceptor interceptor;

    InterceptingSailConnection(NotifyingSailConnection wrappedSailConnection, SailInterceptor interceptor) {
        super(wrappedSailConnection);

        this.wrappedSailConnection = wrappedSailConnection;
        this.interceptor = interceptor;
    }

    @Override
    public void addStatement(UpdateContext updateContext, Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
        if (interceptor.onSparqlAddStatement(subj, pred, obj, contexts)) {
            wrappedSailConnection.addStatement(updateContext, subj, pred, obj, contexts);
        }
    }

    @Override
    public void removeStatement(UpdateContext updateContext, Resource subj, IRI pred, Value obj, Resource... contexts)
            throws SailException {
        if (interceptor.onSparqlRemoveStatement(subj, pred, obj, contexts)) {
            wrappedSailConnection.removeStatement(updateContext, subj, pred, obj, contexts);
        }
    }
}
