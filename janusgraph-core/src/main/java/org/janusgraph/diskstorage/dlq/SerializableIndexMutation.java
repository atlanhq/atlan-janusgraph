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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.janusgraph.diskstorage.indexing.IndexEntry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializable representation of an IndexMutation for DLQ storage.
 */
public class SerializableIndexMutation implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private final boolean isNew;
    private final boolean isDeleted;
    private final List<SerializableIndexEntry> additions;
    private final List<SerializableIndexEntry> deletions;
    
    @JsonCreator
    public SerializableIndexMutation(@JsonProperty("isNew") boolean isNew,
                                    @JsonProperty("isDeleted") boolean isDeleted,
                                    @JsonProperty("additions") List<SerializableIndexEntry> additions,
                                    @JsonProperty("deletions") List<SerializableIndexEntry> deletions) {
        this.isNew = isNew;
        this.isDeleted = isDeleted;
        this.additions = additions != null ? additions : new ArrayList<>();
        this.deletions = deletions != null ? deletions : new ArrayList<>();
    }
    
    public boolean isNew() {
        return isNew;
    }
    
    public boolean isDeleted() {
        return isDeleted;
    }
    
    public List<SerializableIndexEntry> getAdditions() {
        return additions;
    }
    
    public List<SerializableIndexEntry> getDeletions() {
        return deletions;
    }
    
    /**
     * Serializable representation of an IndexEntry
     */
    public static class SerializableIndexEntry implements Serializable {
        
        private static final long serialVersionUID = 1L;
        
        private final String field;
        private final Object value;
        
        @JsonCreator
        public SerializableIndexEntry(@JsonProperty("field") String field,
                                      @JsonProperty("value") Object value) {
            this.field = field;
            this.value = value;
        }
        
        public static SerializableIndexEntry from(IndexEntry entry) {
            return new SerializableIndexEntry(entry.field, entry.value);
        }
        
        public String getField() {
            return field;
        }
        
        public Object getValue() {
            return value;
        }
        
        public IndexEntry toIndexEntry() {
            return new IndexEntry(field, value);
        }
    }
}

