/*
 * Licensed to Aduna under one or more contributor license agreements.
 * See the NOTICE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Aduna licenses this file to you under the terms of the Aduna BSD
 * License (the "License"); you may not use this file except in compliance
 * with the License. See the LICENSE.txt file distributed with this work
 * for the full License.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.openrdf.sail.memory;


import org.openrdf.model.IRI;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.SimpleEvaluationStrategy;
import org.openrdf.sail.SailException;
import org.openrdf.sail.SailReadOnlyException;
import org.openrdf.sail.base.SailSourceConnection;
import org.openrdf.sail.helpers.DefaultSailChangedEvent;

/**
 * Implementation of a Sail Connection for memory stores.
 *
 * @author Arjohn Kampman
 * @author jeen
 */
public class SimpleMemoryStoreConnection extends SailSourceConnection {

	/*-----------*
     * Variables *
	 *-----------*/

    protected final SimpleMemoryStore sail;

    private volatile DefaultSailChangedEvent sailChangedEvent;

	/*--------------*
	 * Constructors *
	 *--------------*/

    protected SimpleMemoryStoreConnection(SimpleMemoryStore sail) {
        super(sail, sail.getSailStore(), sail.getFederatedServiceResolver());
        this.sail = sail;
        sailChangedEvent = new DefaultSailChangedEvent(sail);
    }

	/*---------*
	 * Methods *
	 *---------*/

    @Override
    protected void startTransactionInternal()
            throws SailException {
        if (!sail.isWritable()) {
            throw new SailReadOnlyException("Unable to start transaction: data file is locked or read-only");
        }
        super.startTransactionInternal();
        sail.cancelSyncTask();
    }

    @Override
    protected void commitInternal()
            throws SailException {
        super.commitInternal();

        sail.scheduleSyncTask();
        sail.notifySailChanged(sailChangedEvent);

        // create a fresh event object.
        sailChangedEvent = new DefaultSailChangedEvent(sail);
    }

    @Override
    protected void rollbackInternal()
            throws SailException {
        super.rollbackInternal();
        // create a fresh event object.
        sailChangedEvent = new DefaultSailChangedEvent(sail);
    }

    @Override
    protected void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
            throws SailException {
        // assume the triple is not yet present in the triple store
        sailChangedEvent.setStatementsAdded(true);
    }

    public boolean addInferredStatement(Resource subj, IRI pred, Value obj, Resource... contexts)
            throws SailException {
        boolean ret = super.addInferredStatement(subj, pred, obj, contexts);
        // assume the triple is not yet present in the triple store
        sailChangedEvent.setStatementsAdded(true);
        return ret;
    }

    @Override
    protected void removeStatementsInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
            throws SailException {
        sailChangedEvent.setStatementsRemoved(true);
    }

    public boolean removeInferredStatement(Resource subj, IRI pred, Value obj, Resource... contexts)
            throws SailException {
        boolean ret = super.removeInferredStatement(subj, pred, obj, contexts);
        sailChangedEvent.setStatementsRemoved(true);
        return ret;
    }

    @Override
    protected EvaluationStrategy getEvaluationStrategy(Dataset dataset, TripleSource tripleSource) {
        return new SimpleEvaluationStrategy(tripleSource, dataset, getFederatedServiceResolver(), sail.getIterationCacheSyncThreshold());
    }

    @Override
    protected void clearInternal(Resource... contexts)
            throws SailException {
        super.clearInternal(contexts);
        sailChangedEvent.setStatementsRemoved(true);
    }

    public void clearInferred(Resource... contexts)
            throws SailException {
        super.clearInferred(contexts);
        sailChangedEvent.setStatementsRemoved(true);
    }
}
