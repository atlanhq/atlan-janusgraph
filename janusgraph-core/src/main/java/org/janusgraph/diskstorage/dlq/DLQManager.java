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

package org.janusgraph.diskstorage.dlq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Singleton manager for DLQ instances to ensure they remain open for the application lifecycle.
 * This prevents DLQ from being closed when individual ElasticSearchIndex instances are closed.
 */
public class DLQManager {

    private static final Logger log = LoggerFactory.getLogger(DLQManager.class);
    private static final DLQManager INSTANCE = new DLQManager();

    private final ConcurrentHashMap<String, ElasticSearchDLQ> dlqInstances = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> dlqReferenceCounts = new ConcurrentHashMap<>();

    private DLQManager() {
        // Register shutdown hook to close all DLQs when the JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("JVM shutting down, closing all managed DLQ instances...");
            dlqInstances.forEach((key, dlq) -> {
                try {
                    dlq.close();
                    log.info("Closed DLQ instance: {}", key);
                } catch (IOException e) {
                    log.error("Error closing DLQ instance: {}", key, e);
                }
            });
            dlqInstances.clear();
            dlqReferenceCounts.clear();
            log.info("All managed DLQ instances closed.");
        }));
    }

    public static DLQManager getInstance() {
        return INSTANCE;
    }

    /**
     * Get or create a DLQ instance. If one doesn't exist, create it.
     * If one exists, return the existing instance.
     */
    public ElasticSearchDLQ getDLQ(String bootstrapServers, String dlqTopic, Map<String, Object> additionalConfig) {
        String key = bootstrapServers + ":" + dlqTopic;
        
        return dlqInstances.computeIfAbsent(key, k -> {
            log.info("Creating new KafkaElasticSearchDLQ instance for key: {}", key);
            dlqReferenceCounts.computeIfAbsent(key, s -> new AtomicInteger(0));
            return new KafkaElasticSearchDLQ(bootstrapServers, dlqTopic, additionalConfig);
        });
    }

    /**
     * Increment reference count for a DLQ instance
     */
    public void incrementDLQReference(String bootstrapServers, String dlqTopic) {
        String key = bootstrapServers + ":" + dlqTopic;
        dlqReferenceCounts.computeIfPresent(key, (s, count) -> {
            count.incrementAndGet();
            log.debug("Incremented DLQ reference count for {}. New count: {}", key, count.get());
            return count;
        });
    }

    /**
     * Decrement reference count for a DLQ instance
     */
    public void decrementDLQReference(String bootstrapServers, String dlqTopic) {
        String key = bootstrapServers + ":" + dlqTopic;
        dlqReferenceCounts.computeIfPresent(key, (s, count) -> {
            int newCount = count.decrementAndGet();
            log.debug("Decremented DLQ reference count for {}. New count: {}", key, newCount);
            if (newCount <= 0) {
                // If count drops to zero, consider closing, but rely on shutdown hook for robustness
                log.info("DLQ reference count for {} dropped to 0. Will be closed on JVM shutdown.", key);
            }
            return count;
        });
    }

    /**
     * Get reference count for a DLQ instance
     */
    public int getReferenceCount(String bootstrapServers, String dlqTopic) {
        String key = bootstrapServers + ":" + dlqTopic;
        AtomicInteger count = dlqReferenceCounts.get(key);
        return count != null ? count.get() : 0;
    }

    /**
     * Get status of all DLQ instances
     */
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new ConcurrentHashMap<>();
        status.put("totalInstances", dlqInstances.size());
        status.put("instances", dlqInstances.keySet());
        
        Map<String, Integer> referenceCounts = new ConcurrentHashMap<>();
        dlqReferenceCounts.forEach((key, count) -> referenceCounts.put(key, count.get()));
        status.put("referenceCounts", referenceCounts);
        
        return status;
    }

    /**
     * For testing or explicit shutdown scenarios (use with caution)
     */
    public void closeDLQ(String bootstrapServers, String dlqTopic) throws IOException {
        String key = bootstrapServers + ":" + dlqTopic;
        ElasticSearchDLQ dlq = dlqInstances.remove(key);
        if (dlq != null) {
            dlq.close();
            dlqReferenceCounts.remove(key);
            log.info("Explicitly closed DLQ instance: {}", key);
        }
    }

    /**
     * Close all DLQ instances (for testing)
     */
    public void closeAll() throws IOException {
        log.info("Closing all DLQ instances...");
        for (Map.Entry<String, ElasticSearchDLQ> entry : dlqInstances.entrySet()) {
            try {
                entry.getValue().close();
                log.info("Closed DLQ instance: {}", entry.getKey());
            } catch (IOException e) {
                log.error("Error closing DLQ instance: {}", entry.getKey(), e);
            }
        }
        dlqInstances.clear();
        dlqReferenceCounts.clear();
        log.info("All DLQ instances closed.");
    }
}
