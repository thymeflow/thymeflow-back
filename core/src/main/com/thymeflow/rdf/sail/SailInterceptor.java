package com.thymeflow.rdf.sail;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

/**
 * @author Thomas Pellissier Tanon
 */
public interface SailInterceptor {
    /**
     * Method called when an insertion is tried on the sail
     *
     * @return boolean if the insertion should be done
     */
    default boolean onSparqlAddStatement(Resource subject, IRI predicate, Value object, Resource... contexts) {
        return true;
    }

    /**
     * Method called when an deletion is tried on the sail
     *
     * @return boolean if the deletion should be done
     */
    default boolean onSparqlRemoveStatement(Resource subject, IRI predicate, Value object, Resource... contexts) {
        return true;
    }
}
