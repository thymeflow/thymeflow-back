package thymeflow.rdf.sail;

import org.openrdf.model.IRI;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.UpdateContext;
import org.openrdf.sail.helpers.NotifyingSailConnectionWrapper;

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
