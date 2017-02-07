package com.thymeflow.rdf.sail;

import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;

/**
 * A sail which allows to block insert/delete
 *
 * @author Thomas Pellissier Tanon
 */
public class InterceptingSail extends NotifyingSailWrapper {

    private SailInterceptor interceptor;

    /**
     * Creates a new SailWrapper that wraps the supplied Sail.
     */
    public InterceptingSail(NotifyingSail baseSail, SailInterceptor interceptor) {
        super(baseSail);

        this.interceptor = interceptor;
    }

    @Override
    public NotifyingSailConnection getConnection() throws SailException {
        return new InterceptingSailConnection(super.getConnection(), getValueFactory(), interceptor);
    }
}
