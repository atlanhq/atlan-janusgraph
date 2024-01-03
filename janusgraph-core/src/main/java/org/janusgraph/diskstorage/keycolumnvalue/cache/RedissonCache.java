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

import org.apache.commons.lang.ArrayUtils;
import org.janusgraph.core.JanusGraphConfigurationException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_CONNECTION_TIME_OUT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_KEEP_ALIVE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_LOCK_WATCHDOG_TIMEOUT_MS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_MASTER_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_PASSWORD;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_SENTINEL_URLS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_SERVER_MODE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_SERVER_URL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_USERNAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CLIENT_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_DATABASE_ID;

public class RedissonCache {

    private static final Logger log = LoggerFactory.getLogger(RedissonCache.class);
    private static RedissonClient client;
    private static final String REDIS_URL_PREFIX = "redis://";
    private static final String COMMA = ",";
    private static final String SENTINEL = "sentinel";
    private static final String STANDALONE = "single";
    private static String redisServerMode;
    private static int connectTimeout;
    private static boolean keepAlive;
    private static long watchdogTimeoutInMS;

    public static RedissonClient getRedissonClient(Configuration configuration) {
        synchronized (RedissonCache.class) {
            if (Objects.isNull(client)) {
                redisServerMode = configuration.get(REDIS_CACHE_SERVER_MODE);
                connectTimeout = configuration.get(REDIS_CACHE_CONNECTION_TIME_OUT);
                keepAlive = configuration.get(REDIS_CACHE_KEEP_ALIVE);
                watchdogTimeoutInMS = configuration.get(REDIS_CACHE_LOCK_WATCHDOG_TIMEOUT_MS);
                log.info("Creating connection for redis:{}", redisServerMode);
                Config config = new Config();
                switch (redisServerMode) {
                    case SENTINEL:
                        config.setLockWatchdogTimeout(watchdogTimeoutInMS)
                            .useSentinelServers()
                            .setDatabase(configuration.get(REDIS_DATABASE_ID))
                            .setClientName(String.join("-",configuration.get(REDIS_CLIENT_NAME),"janusgraph"))
                            .setReadMode(ReadMode.MASTER_SLAVE)
                            .setCheckSentinelsList(false)
                            .setConnectTimeout(connectTimeout)
                            .setIdleConnectionTimeout(5_000)
                            .setKeepAlive(keepAlive)
                            .setMasterName(configuration.get(REDIS_CACHE_MASTER_NAME))
                            .addSentinelAddress(formatUrls(configuration.get(REDIS_CACHE_SENTINEL_URLS).split(COMMA)))
                            .setUsername(configuration.get(REDIS_CACHE_USERNAME))
                            .setPassword(configuration.get(REDIS_CACHE_PASSWORD));
                        break;
                    case STANDALONE:
                        config.setLockWatchdogTimeout(watchdogTimeoutInMS)
                            .useSingleServer()
                            .setClientName(configuration.get(REDIS_CLIENT_NAME))
                            .setAddress(formatUrls(configuration.get(REDIS_CACHE_SERVER_URL).split(COMMA))[0])
                            .setConnectTimeout(connectTimeout)
                            .setKeepAlive(keepAlive)
                            .setUsername(configuration.get(REDIS_CACHE_USERNAME))
                            .setPassword(configuration.get(REDIS_CACHE_PASSWORD));
                        break;
                    default:
                        throw new JanusGraphConfigurationException("Invalid redis server mode");
                }
                client = Redisson.create(config);
            }
        }
        return client;
    }

    private static String[] formatUrls(String[] urls) throws IllegalArgumentException {
        if (ArrayUtils.isEmpty(urls)) {
            throw new JanusGraphConfigurationException("Invalid redis cluster urls");
        }
        return Arrays.stream(urls).map(url -> {
            if (url.startsWith(REDIS_URL_PREFIX)) {
                return url;
            }
            return REDIS_URL_PREFIX + url;
        }).toArray(String[]::new);
    }

}
