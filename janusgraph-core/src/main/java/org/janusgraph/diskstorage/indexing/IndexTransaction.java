// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.indexing;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.LoggableTransaction;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.dlq.DLQManager;
import org.janusgraph.diskstorage.dlq.ElasticSearchDLQ;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DLQ_ENABLED;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DLQ_KAFKA_BOOTSTRAP_SERVERS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DLQ_KAFKA_TOPIC;

/**
 * Wraps the transaction handle of an index and buffers all mutations against an index for efficiency.
 * Also acts as a proxy to the {@link IndexProvider} methods.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexTransaction implements BaseTransaction, LoggableTransaction {
    private static final Logger log =
        LoggerFactory.getLogger(IndexTransaction.class);
    private static final int DEFAULT_OUTER_MAP_SIZE = 3;
    private static final int DEFAULT_INNER_MAP_SIZE = 5;

    private final IndexProvider index;
    private final BaseTransaction indexTx;
    private final KeyInformation.IndexRetriever keyInformation;

    private final Duration maxWriteTime;

    private Map<String,Map<String,IndexMutation>> mutations;

    // Dead Letter Queue
    private final ElasticSearchDLQ dlq;
    private final String indexName;

    public IndexTransaction(final IndexProvider index, final KeyInformation.IndexRetriever keyInformation,
                            BaseTransactionConfig config,
                            Configuration disConfig,
                            Duration maxWriteTime) throws BackendException {
        Preconditions.checkNotNull(index);
        Preconditions.checkNotNull(keyInformation);
        this.index=index;
        this.keyInformation = keyInformation;
        this.indexTx=index.beginTransaction(config);
        Preconditions.checkNotNull(indexTx);
        this.maxWriteTime = maxWriteTime;
        this.mutations = new HashMap<>(DEFAULT_OUTER_MAP_SIZE);
        // Initialize Dead Letter Queue
        this.dlq = initializeDLQ(disConfig);
        this.indexName = GraphDatabaseConfiguration.INDEX_NAME.getDefaultValue();
    }

    public void add(String store, String documentId, IndexEntry entry, boolean isNew) {
        getIndexMutation(store,documentId, isNew, false).addition(new IndexEntry(entry.field, entry.value, entry.getMetaData()));
    }

    public void add(String store, String documentId, String key, Object value, boolean isNew) {
        getIndexMutation(store,documentId,isNew,false).addition(new IndexEntry(key,value));
    }

    public void delete(String store, String documentId, String key, Object value, boolean deleteAll) {
        getIndexMutation(store,documentId,false,deleteAll).deletion(new IndexEntry(key,value));
    }

    private IndexMutation getIndexMutation(String store, String documentId, boolean isNew, boolean isDeleted) {
        final Map<String, IndexMutation> storeMutations = mutations.computeIfAbsent(store, k -> new HashMap<>(DEFAULT_INNER_MAP_SIZE));
        IndexMutation m = storeMutations.get(documentId);
        if (m==null) {
            m = new IndexMutation(keyInformation.get(store), isNew, isDeleted);
            storeMutations.put(documentId, m);
        } else {
            //IndexMutation already exists => if we deleted and re-created it we need to remove the deleted flag
            if (isNew && m.isDeleted()) {
                m.resetDelete();
                assert !m.isNew() && !m.isDeleted();
            }
        }
        return m;
    }

    public void clearStorage() throws BackendException {
        index.clearStorage();
    }

    public void clearStore(String storeName) throws BackendException {
        index.clearStore(storeName);
    }

    public void register(String store, String key, KeyInformation information) throws BackendException {
        index.register(store,key,information,indexTx);
    }

    public Stream<String> queryStream(IndexQuery query) throws BackendException {
        return index.query(query, keyInformation, indexTx);
    }

    public Number queryAggregation(IndexQuery query, Aggregation aggregation) throws BackendException {
        return index.queryAggregation(query, keyInformation, indexTx, aggregation);
    }

    public Stream<RawQuery.Result<String>> queryStream(RawQuery query) throws BackendException {
        return index.query(query, keyInformation,indexTx);
    }

    public Long totals(RawQuery query) throws BackendException {
        return index.totals(query, keyInformation,indexTx);
    }

    public void restore(Map<String, Map<String,List<IndexEntry>>> documents) throws BackendException {
        index.restore(documents, keyInformation,indexTx);
    }

    @Override
    public void commit() throws BackendException {
        flushInternal();
        indexTx.commit();
    }

    @Override
    public void rollback() throws BackendException {
        mutations=null;
        indexTx.rollback();
    }

    private void flushInternal() throws BackendException {
        if (mutations!=null && !mutations.isEmpty()) {
            //Consolidate all mutations prior to persistence to ensure that no addition accidentally gets swallowed by a delete
            for (Map<String, IndexMutation> store : mutations.values()) {
                for (IndexMutation mut : store.values()) mut.consolidate();
            }

            // Keep a reference to mutations for failure handling if retries fail
            final Map<String, Map<String, IndexMutation>> mutationsToExecute = mutations;
            
            try {
                BackendOperation.execute(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        index.mutate(mutationsToExecute, keyInformation, indexTx);
                        return true;
                    }

                    @Override
                    public String toString() {
                        return "IndexMutation";
                    }
                }, maxWriteTime);
            } catch (Throwable e) {
                log.error("=== CAUGHT EXCEPTION IN FLUSHINTERNAL ===");
                log.error("Exception type: {}", e.getClass().getName());
                log.error("Exception message: {}", e.getMessage());

                try {
                    log.info("Calling handleMutationFailure...");
                    handleMutationFailure(mutationsToExecute, e);
                    log.info("handleMutationFailure completed successfully");
                } catch (Exception handlerEx) {
                    log.warn("Failed to handle mutation failure", handlerEx);
                }
            }

            mutations=null;
        }
    }

    @Override
    public void logMutations(DataOutput out) {
        VariableLong.writePositive(out,mutations.size());
        for (Map.Entry<String,Map<String,IndexMutation>> store : mutations.entrySet()) {
            out.writeObjectNotNull(store.getKey());
            VariableLong.writePositive(out,store.getValue().size());
            for (Map.Entry<String,IndexMutation> doc : store.getValue().entrySet()) {
                out.writeObjectNotNull(doc.getKey());
                IndexMutation mut = doc.getValue();
                out.putByte((byte)(mut.isNew()?1:(mut.isDeleted()?2:0)));
                List<IndexEntry> additions = mut.getAdditions();
                VariableLong.writePositive(out,additions.size());
                for (IndexEntry add : additions) writeIndexEntry(out,add);
                List<IndexEntry> deletions = mut.getDeletions();
                VariableLong.writePositive(out,deletions.size());
                for (IndexEntry del: deletions) writeIndexEntry(out,del);
            }
        }
    }

    private void writeIndexEntry(DataOutput out, IndexEntry entry) {
        out.writeObjectNotNull(entry.field);
        out.writeClassAndObject(entry.value);
    }

    public void invalidate(String store) {
        keyInformation.invalidate(store);
    }

    /**
     * Handles mutation failure after all retries have been exhausted.
     * This is called by the transaction layer when BackendOperation.execute() fails.
     * Writes the failed mutations to the configured Dead Letter Queue (DLQ) if enabled.
     *
     * @param mutations The mutations that failed after all retry attempts
     * @param cause The exception that caused the final failure
     */
    private void handleMutationFailure(Map<String, Map<String, IndexMutation>> mutations, Throwable cause) {
        log.error("Mutation failed after all retries exhausted for index '{}'", indexName, cause);
        log.info("handleMutationFailure called - DLQ status: dlq={}, isEnabled={}",
            dlq != null ? "initialized" : "NULL",
            dlq != null && dlq.isEnabled() ? "true" : "false/null");

        if (dlq == null) {
            log.warn("❌ DLQ is NULL! Failed mutations will NOT be written to DLQ.");
            log.warn("This means DLQ was not properly initialized during ElasticSearchIndex construction.");
            log.warn("Check startup logs for DLQ initialization messages.");
            log.warn("Ensure 'index.search.elasticsearch.dlq.enabled=true' is set in your JanusGraph configuration.");
            return;
        }

        if (!dlq.isEnabled()) {
            log.warn("DLQ is initialized but not enabled. Failed mutations will not be written.");
            return;
        }

        try {
            // Try to determine which store failed from the mutations
            String storeName = mutations.isEmpty() ? "unknown" : mutations.keySet().iterator().next();
            log.info("Writing failed mutations to DLQ for stores: {}", mutations.keySet());
            dlq.writeToDLQ(indexName, mutations, storeName, cause);
            log.info("✅ Successfully written failed mutations to DLQ for stores: {}", mutations.keySet());
        } catch (Exception dlqEx) {
            log.error("❌ Failed to write to DLQ after retry exhaustion", dlqEx);
        }
    }

    private ElasticSearchDLQ initializeDLQ(Configuration config) {
        log.info("========================================");
        log.info("Initializing Elasticsearch DLQ...");
        log.info("========================================");

        // Log the full config path for debugging
        log.info("Checking config key: {}", DLQ_ENABLED.toStringWithoutRoot());

        // First try to get from config
        boolean dlqEnabled = config.get(DLQ_ENABLED);

        log.info("DLQ enabled: {}", dlqEnabled);

        if (!dlqEnabled) {
            log.info("Elasticsearch DLQ is DISABLED");
            log.info("To enable, set: index.dlq.enabled=true in your configuration");
            log.info("========================================");
            return null;
        }

        try {
            log.info("DLQ is ENABLED, loading Kafka configuration...");

            String bootstrapServers = config.get(DLQ_KAFKA_BOOTSTRAP_SERVERS);
            String topic = config.get(DLQ_KAFKA_TOPIC);

            log.info("DLQ Configuration:");
            log.info("  Bootstrap Servers: '{}'", bootstrapServers);
            log.info("  Topic: '{}'", topic);

            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                log.error("❌ DLQ is enabled but kafka-bootstrap-servers is not configured. DLQ will be disabled.");
                log.error("Please set: index.dlq.kafka-bootstrap-servers=<your-kafka-servers>");
                log.info("========================================");
                return null;
            }

            // Additional Kafka configuration can be passed as empty map for now
            // Can be extended later if needed
            Map<String, Object> kafkaConfig = new HashMap<>();

            // Use singleton DLQ manager to ensure DLQ remains open for application lifecycle
            DLQManager dlqManager = DLQManager.getInstance();
            ElasticSearchDLQ dlqInstance = dlqManager.getDLQ(bootstrapServers, topic, kafkaConfig);

            if (dlqInstance != null) {
                // Increment reference count for this DLQ instance
                dlqManager.incrementDLQReference(bootstrapServers, topic);
                log.info("✅ Successfully initialized Kafka DLQ via DLQManager!");
                log.info("DLQ reference count: {}", dlqManager.getReferenceCount(bootstrapServers, topic));
            } else {
                log.warn("⚠️  DLQManager returned null DLQ (possibly shutting down)");
            }
            log.info("========================================");
            return dlqInstance;

        } catch (Exception e) {
            log.error("❌ Failed to initialize DLQ. DLQ will be disabled.", e);
            log.info("========================================");
            return null;
        }
    }
}
