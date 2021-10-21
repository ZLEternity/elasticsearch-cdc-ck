package com.ctrip.cdc.ck;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * client(ClickHouseStatement) cache
 * @author leon
 */
public class CachedCkClient {

    private static final Logger log = LogManager.getLogger(CachedCkClient.class);
    /**
     * 3600000000 ms
     * 60000 min
     */
    private final long defaultCacheExpireTimeout = TimeUnit.MINUTES.toMillis(60000);


    private final CacheLoader<Properties, ClickHouseStatement> cacheLoader = new CacheLoader<Properties, ClickHouseStatement> (){
        @Override
        public ClickHouseStatement load(Properties properties) {
            return createClickHouseStatement(properties);
        }
    };


    private final RemovalListener<Properties, ClickHouseStatement> removalListener = notification -> {
        close(notification.getKey());
    };


    private final LoadingCache<Properties, ClickHouseStatement> guavaCache =
            CacheBuilder.newBuilder().expireAfterAccess(defaultCacheExpireTimeout, TimeUnit.MILLISECONDS)
      .removalListener(removalListener).build(cacheLoader);



    private ClickHouseStatement createClickHouseStatement(Properties properties) {
        if(properties == null || properties.isEmpty() ||
            !properties.containsKey("ck.domain") || !properties.containsKey("ck.username") || !properties.containsKey("ck.password")
            || !properties.containsKey("ck.db")){
            log.error("createClickHouseStatement param error");
            return null;
        }
//        String url = "jdbc:clickhouse://sys-elasticsearch-cdc-data-service.ck.qa.sys.k8s.cloud.qa.nt.xxx.com:80/elasticsearch_cdc";
        String url = String.format("jdbc:clickhouse://%s:80/%s", properties.getProperty("ck.domain"), properties.getProperty("ck.db"));

        ClickHouseProperties ckProperties = new ClickHouseProperties();
        ckProperties.setUser(properties.getProperty("ck.username"));
        ckProperties.setPassword(properties.getProperty("ck.password"));

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, ckProperties);

        ClickHouseConnection conn = null;
        ClickHouseStatement stmt = null;
        try {
            conn = dataSource.getConnection();
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Thread.currentThread().setContextClassLoader(null);
        return stmt;
    }


    public ClickHouseStatement getOrCreate(Properties properties){
        try {
            return guavaCache.get(properties);
        } catch(ExecutionException| UncheckedExecutionException e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    /** For explicitly closing ck client */
    private void close(Properties properties) {
        guavaCache.invalidate(properties);
    }


    public void clear() {
        guavaCache.invalidateAll();
    }
}
