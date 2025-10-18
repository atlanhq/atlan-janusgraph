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

package org.janusgraph.diskstorage.es.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Kafka-based implementation of ElasticSearchDLQ.
 * Writes failed Elasticsearch mutations to a Kafka topic for later replay.
 */
public class KafkaElasticSearchDLQ implements ElasticSearchDLQ {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaElasticSearchDLQ.class);
    
    private final KafkaProducer<String, String> producer;
    private final String dlqTopic;
    private final ObjectMapper mapper;
    private final boolean enabled;
    
    public KafkaElasticSearchDLQ(String bootstrapServers, String dlqTopic, Map<String, Object> additionalConfig) {
        this.dlqTopic = dlqTopic;
        this.mapper = new ObjectMapper();
        this.enabled = bootstrapServers != null && !bootstrapServers.isEmpty() 
                      && dlqTopic != null && !dlqTopic.isEmpty();
        
        if (enabled) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            
            // Add any additional configuration
            if (additionalConfig != null) {
                props.putAll(additionalConfig);
            }
            
            this.producer = new KafkaProducer<>(props);
            log.info("Initialized Kafka DLQ with topic: {}, bootstrap servers: {}", dlqTopic, bootstrapServers);
        } else {
            this.producer = null;
            log.info("Kafka DLQ is disabled");
        }
    }
    
    @Override
    public void writeToDLQ(String indexName, 
                          Map<String, Map<String, IndexMutation>> mutations,
                          String storeName,
                          Throwable error) {
        if (!enabled) {
            log.debug("DLQ is disabled, skipping write for index: {}", indexName);
            return;
        }
        
        try {
            // Convert IndexMutation to SerializableIndexMutation
            Map<String, Map<String, SerializableIndexMutation>> serializableMutations = convertMutations(mutations);
            
            // Extract error information
            String errorMessage = error.getMessage();
            String errorClass = error.getClass().getName();
            String stackTrace = getStackTrace(error);
            
            // Create DLQ entry
            DLQEntry entry = new DLQEntry(
                indexName,
                storeName,
                serializableMutations,
                errorMessage,
                errorClass,
                System.currentTimeMillis(),
                stackTrace
            );
            
            // Serialize to JSON
            String payload = mapper.writeValueAsString(entry);
            
            // Create Kafka record with index name as key for partitioning
            ProducerRecord<String, String> record = new ProducerRecord<>(
                dlqTopic,
                indexName + "-" + storeName,
                payload
            );
            
            // Send asynchronously with callback
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to write DLQ entry to Kafka topic {} for index {}", 
                             dlqTopic, indexName, exception);
                } else {
                    log.info("Successfully wrote DLQ entry to Kafka topic {} for index {} at offset {}", 
                            dlqTopic, indexName, metadata.offset());
                }
            });
            
            // Ensure message is sent (for critical failures, we want immediate persistence)
            producer.flush();
            
        } catch (Exception e) {
            log.error("Failed to serialize and write DLQ entry for index {}", indexName, e);
            // Don't throw - we don't want DLQ failures to cause additional issues
        }
    }
    
    private Map<String, Map<String, SerializableIndexMutation>> convertMutations(
            Map<String, Map<String, IndexMutation>> mutations) {
        
        Map<String, Map<String, SerializableIndexMutation>> result = new HashMap<>();
        
        for (Map.Entry<String, Map<String, IndexMutation>> storeEntry : mutations.entrySet()) {
            Map<String, SerializableIndexMutation> storeMutations = new HashMap<>();
            
            for (Map.Entry<String, IndexMutation> docEntry : storeEntry.getValue().entrySet()) {
                IndexMutation mutation = docEntry.getValue();
                
                SerializableIndexMutation serializableMutation = new SerializableIndexMutation(
                    mutation.isNew(),
                    mutation.isDeleted(),
                    mutation.getAdditions().stream()
                        .map(SerializableIndexMutation.SerializableIndexEntry::from)
                        .collect(Collectors.toList()),
                    mutation.getDeletions().stream()
                        .map(SerializableIndexMutation.SerializableIndexEntry::from)
                        .collect(Collectors.toList())
                );
                
                storeMutations.put(docEntry.getKey(), serializableMutation);
            }
            
            result.put(storeEntry.getKey(), storeMutations);
        }
        
        return result;
    }
    
    private String getStackTrace(Throwable error) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            error.printStackTrace(pw);
            return sw.toString();
        } catch (Exception e) {
            return "Unable to capture stack trace: " + e.getMessage();
        }
    }
    
    @Override
    public boolean isEnabled() {
        return enabled;
    }
    
    @Override
    public void close() throws IOException {
        if (producer != null) {
            try {
                producer.flush();
                producer.close();
                log.info("Closed Kafka DLQ producer");
            } catch (Exception e) {
                log.error("Error closing Kafka DLQ producer", e);
                throw new IOException("Failed to close Kafka DLQ producer", e);
            }
        }
    }
}

