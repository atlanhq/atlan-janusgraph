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
            return new KafkaElasticSearchDLQ(bootstrapServers, dlqTopic, additionalConfig);
        });
    }
}
