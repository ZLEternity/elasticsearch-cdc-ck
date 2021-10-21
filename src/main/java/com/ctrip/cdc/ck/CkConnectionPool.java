package com.ctrip.cdc.ck;

import com.ctrip.cdc.Config;
import com.ctrip.cdc.PluginSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.clickhouse.ClickHouseStatement;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

public class CkConnectionPool {

//    private CDCListener cdcListener;
//    public void registerListener(CDCListener cdcListener) {
//        this.cdcListener = cdcListener;
//    }

    private static final CkConnectionPool instance = new CkConnectionPool();
    private CkConnectionPool() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (cachedCkClient != null) {
                cachedCkClient.clear();
            }
        }));
    }

    private Properties properties;
    CachedCkClient cachedCkClient = null;
    ReentrantLock lock = new ReentrantLock();
    private static final Logger log = LogManager.getLogger(CkConnectionPool.class);
    public static CkConnectionPool getInstance() {
        return instance;
    }

    public void refreshAllProducers(Map<String, Object> settings) {
        final String ckDomain = settings.get(PluginSettings.CDC_CK_DOMAIN).toString().trim();
        if ("".equals(ckDomain)) {
            log.error(PluginSettings.CDC_CK_DOMAIN + " is '"+ ckDomain +"'");
            return;
        }
        lock.lock();
        try {
            log.warn(settings);
            // the first time init
            if(cachedCkClient != null){
                // firstly clear cached
                cachedCkClient.clear();
            }
            cachedCkClient = new CachedCkClient();

            // finally, update the ck client properties
            properties = new Properties();
            for (Map.Entry<String, Object> entry : settings.entrySet()) {
                final String key = entry.getKey().substring(PluginSettings.CLUSTER_SETTING_PREFIX.length());
                final Object value = entry.getValue();
                log.warn(key + ":" + value);
                properties.put(key, value);
            }

            // 将cdcListener中的类变量（缓存）needInit设置为true，cdcListener中方法执行的时候需要重新初始化，即对配置变化可见
//            cdcListener.accept(true);
            Config.setNeedInit(true);
        } finally {
            lock.unlock();
        }
    }


    public ClickHouseStatement getCachedClickHouseStatement() {
        lock.lock();
        try {
            return cachedCkClient.getOrCreate(properties);
        } catch (Exception e) {
            log.error("cachedCkClient.getOrCreate error, {}", properties != null ? properties.toString() : null);
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        return getCachedClickHouseStatement();
    }

}
