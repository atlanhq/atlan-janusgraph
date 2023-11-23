package org.janusgraph.diskstorage.keycolumnvalue.cache;

import org.apache.commons.lang.ArrayUtils;
import org.janusgraph.core.JanusGraphConfigurationException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;

import java.util.Arrays;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class RedissonCache {

    private static final String REDIS_URL_PREFIX = "redis://";
    private static final String COMMA = ",";
    private static final String JANUSGRAPH_REDIS = "janusgraph-redis";
    private static final String SENTINEL = "sentinel";
    private static final String STANDALONE = "single";
    private static String redisServerMode;
    private static int connectTimeout;
    private static boolean keepAlive;
    private static long watchdogTimeoutInMS;

    public static RedissonClient getRedissonClient(Configuration configuration) {
        redisServerMode = configuration.get(REDIS_CACHE_SERVER_MODE);
        connectTimeout = configuration.get(REDIS_CACHE_CONNECTION_TIME_OUT);
        keepAlive = configuration.get(REDIS_CACHE_KEEP_ALIVE);
        watchdogTimeoutInMS = configuration.get(REDIS_CACHE_LOCK_WATCHDOG_TIMEOUT_MS);

        Config config = new Config();
        switch (redisServerMode) {
            case SENTINEL:
                config.setLockWatchdogTimeout(watchdogTimeoutInMS)
                    .useSentinelServers()
                    .setClientName(JANUSGRAPH_REDIS)
                    .setReadMode(ReadMode.MASTER_SLAVE)
                    .setCheckSentinelsList(false)
                    .setConnectTimeout(connectTimeout)
                    .setKeepAlive(keepAlive)
                    .setMasterName(configuration.get(REDIS_CACHE_MASTER_NAME))
                    .addSentinelAddress(formatUrls(configuration.get(REDIS_CACHE_SENTINEL_URLS).split(COMMA)))
                    .setUsername(configuration.get(REDIS_CACHE_USERNAME))
                    .setPassword(configuration.get(REDIS_CACHE_PASSWORD));
                break;
            case STANDALONE:
                config.setLockWatchdogTimeout(watchdogTimeoutInMS)
                    .useSingleServer()
                    .setClientName(JANUSGRAPH_REDIS)
                    .setAddress(formatUrls(configuration.get(REDIS_CACHE_SERVER_URL).split(COMMA))[0])
                    .setConnectTimeout(connectTimeout)
                    .setKeepAlive(keepAlive)
                    .setUsername(configuration.get(REDIS_CACHE_USERNAME))
                    .setPassword(configuration.get(REDIS_CACHE_PASSWORD));
                break;
            default:
                throw new JanusGraphConfigurationException("Invalid redis server mode");
        }

        return Redisson.create(config);
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
