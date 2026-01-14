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

import org.janusgraph.diskstorage.indexing.IndexMutation;

import java.io.Closeable;
import java.util.Map;

/**
 * Interface for Dead Letter Queue to handle failed Elasticsearch mutations.
 * Implementations can use various backends like Kafka, SQS, File system, etc.
 */
public interface ElasticSearchDLQ extends Closeable {
    
    /**
     * Write failed mutations to DLQ
     * 
     * @param indexName The name of the index
     * @param mutations The mutations that failed
     * @param storeName The store name within the index
     * @param error The error that caused the failure
     */
    void writeToDLQ(String indexName, 
                    Map<String, Map<String, IndexMutation>> mutations,
                    String storeName,
                    Throwable error);
    
    /**
     * Check if DLQ is enabled and ready
     * 
     * @return true if DLQ is operational
     */
    boolean isEnabled();
}

