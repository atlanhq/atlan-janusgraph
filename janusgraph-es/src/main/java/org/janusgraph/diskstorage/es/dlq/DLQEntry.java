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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a failed Elasticsearch mutation entry in the Dead Letter Queue.
 * Contains all information needed to replay the mutation later.
 */
public class DLQEntry implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private final String indexName;
    private final String storeName;
    private final Map<String, Map<String, SerializableIndexMutation>> mutations;
    private final String errorMessage;
    private final String errorClass;
    private final long timestamp;
    private final String stackTrace;
    
    @JsonCreator
    public DLQEntry(@JsonProperty("indexName") String indexName,
                    @JsonProperty("storeName") String storeName,
                    @JsonProperty("mutations") Map<String, Map<String, SerializableIndexMutation>> mutations,
                    @JsonProperty("errorMessage") String errorMessage,
                    @JsonProperty("errorClass") String errorClass,
                    @JsonProperty("timestamp") long timestamp,
                    @JsonProperty("stackTrace") String stackTrace) {
        this.indexName = indexName;
        this.storeName = storeName;
        this.mutations = mutations != null ? mutations : new HashMap<>();
        this.errorMessage = errorMessage;
        this.errorClass = errorClass;
        this.timestamp = timestamp;
        this.stackTrace = stackTrace;
    }
    
    public String getIndexName() {
        return indexName;
    }
    
    public String getStoreName() {
        return storeName;
    }
    
    public Map<String, Map<String, SerializableIndexMutation>> getMutations() {
        return mutations;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public String getErrorClass() {
        return errorClass;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public String getStackTrace() {
        return stackTrace;
    }
    
    @Override
    public String toString() {
        return "DLQEntry{" +
                "indexName='" + indexName + '\'' +
                ", storeName='" + storeName + '\'' +
                ", mutationCount=" + mutations.size() +
                ", errorClass='" + errorClass + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}

