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

package org.janusgraph.diskstorage.keycolumnvalue.cache;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.CacheMetricsAction;
import org.nustaq.serialization.FSTConfiguration;
import org.redisson.api.RLock;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.CACHE_KEYSPACE_PREFIX;

/**
 * @author naveenaechan
 */
public class ExpirationKCVSRedisCache extends KCVSCache {

    public static final String REDIS_INDEX_CACHE_PREFIX = "index";
    private final long cacheTimeMS;
    private RedissonClient redissonClient;
    private RMapCache<KeySliceQuery, byte[]> redisCache;
    private RMapCache<StaticBuffer, ArrayList<KeySliceQuery>> redisIndexKeys;
    private static Logger logger = Logger.getLogger("janusgraph-redis-logger");
    private static FSTConfiguration fastConf = FSTConfiguration.createDefaultConfiguration();

    public ExpirationKCVSRedisCache(final KeyColumnValueStore store, String metricsName, final long cacheTimeMS,
                                    final long invalidationGracePeriodMS, final long maximumByteSize, Configuration configuration) {
        super(store, metricsName);
        Preconditions.checkArgument(cacheTimeMS > 0, "Cache expiration must be positive: %s", cacheTimeMS);
        Preconditions.checkArgument(System.currentTimeMillis() + 1000L * 3600 * 24 * 365 * 100 + cacheTimeMS > 0, "Cache expiration time too large, overflow may occur: %s", cacheTimeMS);
        this.cacheTimeMS = cacheTimeMS;
        Preconditions.checkArgument(invalidationGracePeriodMS >= 0, "Invalid expiration grace period: %s", invalidationGracePeriodMS);

        redissonClient = RedissonCache.getRedissonClient(configuration);
        redisCache = redissonClient.getMapCache(String.join("-", configuration.get(CACHE_KEYSPACE_PREFIX), metricsName));
        redisIndexKeys = redissonClient.getMapCache(String.join("-", configuration.get(CACHE_KEYSPACE_PREFIX), REDIS_INDEX_CACHE_PREFIX, metricsName));

        logger.info("********************** Configurations are loaded **********************");
    }

    @Override
    public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh) throws BackendException {
        incActionBy(1, CacheMetricsAction.RETRIEVAL, txh);
        try {
            return get(query, () -> {
                incActionBy(1, CacheMetricsAction.MISS, txh);
                return store.getSlice(query, unwrapTx(txh));
            });
        } catch (Exception e) {
            if (e instanceof JanusGraphException) throw (JanusGraphException) e;
            else if (e.getCause() instanceof JanusGraphException) throw (JanusGraphException) e.getCause();
            else throw new JanusGraphException(e);
        }
    }

    private EntryList get(KeySliceQuery query, Callable<EntryList> valueLoader) {
        byte[] bytQuery = redisCache.get(query);
        EntryList entries = bytQuery != null ? (EntryList) fastConf.asObject(bytQuery) : null;
        if (entries == null) {
            logger.log(Level.INFO, "Reading from the store.................");
            try {
                entries = valueLoader.call();
                if (entries == null) {
                    throw new CacheLoader.InvalidCacheLoadException("valueLoader must not return null, key=" + query);
                } else {
                    redisCache.fastPutAsync(query, fastConf.asByteArray(entries), this.cacheTimeMS,TimeUnit.MILLISECONDS);
                    RLock lock = redisIndexKeys.getLock(query.getKey());
                    try {
                        lock.tryLock(1, 3, TimeUnit.SECONDS);
                        ArrayList<KeySliceQuery> queryList = redisIndexKeys.get(query.getKey());
                        if (queryList == null)
                            queryList = new ArrayList<>();
                        queryList.add(query);
                        redisIndexKeys.fastPutAsync(query.getKey(), queryList, this.cacheTimeMS,TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return entries;
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        final Map<StaticBuffer, EntryList> results = new HashMap<>(keys.size());
        final List<StaticBuffer> remainingKeys = new ArrayList<>(keys.size());
        KeySliceQuery[] ksqs = new KeySliceQuery[keys.size()];
        incActionBy(keys.size(), CacheMetricsAction.RETRIEVAL, txh);
        byte[] bytResult = null;
        //Find all cached queries
        for (int i = 0; i < keys.size(); i++) {
            final StaticBuffer key = keys.get(i);
            ksqs[i] = new KeySliceQuery(key, query);
            EntryList result = null;
            bytResult = redisCache.get(ksqs[i]);
            result = bytResult != null ? (EntryList) fastConf.asObject(bytResult) : null;
            if (result != null) results.put(key, result);
            else remainingKeys.add(key);
        }
        //Request remaining ones from backend
        if (!remainingKeys.isEmpty()) {
            incActionBy(remainingKeys.size(), CacheMetricsAction.MISS, txh);
            Map<StaticBuffer, EntryList> subresults = store.getSlice(remainingKeys, query, unwrapTx(txh));

            for (int i = 0; i < keys.size(); i++) {
                StaticBuffer key = keys.get(i);
                EntryList subresult = subresults.get(key);
                if (subresult != null) {
                    results.put(key, subresult);
                    if (ksqs[i] != null) {
                        logger.info("adding to cache subresult " + subresult);
                        redisCache.fastPutAsync(ksqs[i], fastConf.asByteArray(subresult), this.cacheTimeMS, TimeUnit.MILLISECONDS);
                        RLock lock = redisIndexKeys.getLock(ksqs[i].getKey());
                        try {
                            lock.tryLock(3, 10, TimeUnit.SECONDS);
                            ArrayList<KeySliceQuery> queryList = redisIndexKeys.get(ksqs[i].getKey());
                            if (queryList == null)
                                queryList = new ArrayList<>();
                            queryList.add(ksqs[i]);
                            redisIndexKeys.fastPutAsync(ksqs[i].getKey(), queryList, this.cacheTimeMS, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            }
        }
        return results;
    }

    @Override
    public void clearCache() {
        redisCache.clearExpire();
        redisIndexKeys.clearExpire();
    }

    @Override
    public void invalidate(StaticBuffer key, List<CachableStaticBuffer> entries) {
        List<KeySliceQuery> keySliceQueryList = redisIndexKeys.get(key);
        if (keySliceQueryList != null) {
            for (KeySliceQuery keySliceQuery : keySliceQueryList) {
                if (key.equals(keySliceQuery.getKey())) {
                    redisCache.fastRemoveAsync(keySliceQuery);
                }
            }
        }
    }

    @Override
    public void close() throws BackendException {
        super.close();
    }
}
