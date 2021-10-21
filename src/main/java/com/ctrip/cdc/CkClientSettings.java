package com.ctrip.cdc;

import com.ctrip.cdc.ck.CkConnectionPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * all ck client configs are cluster level settings.
 */
public class CkClientSettings {

    private static final Logger log = LogManager.getLogger(CkClientSettings.class);

    static final Map<String, Object> settings = new ConcurrentHashMap<>();


    static final Setting<String> CDC_CK_DOMAIN_CONFIG = Setting.simpleString(PluginSettings.CDC_CK_DOMAIN, "",
            Setting.Property.NodeScope, Setting.Property.Dynamic);
    static final Setting<String> CDC_CK_USERNAME_CONFIG = Setting.simpleString(PluginSettings.CDC_CK_USERNAME, "",
            Setting.Property.NodeScope, Setting.Property.Dynamic);
    static final Setting<String> CDC_CK_PASSWORD_CONFIG = Setting.simpleString(PluginSettings.CDC_CK_PASSWORD, "",
            Setting.Property.NodeScope, Setting.Property.Dynamic);
    static final Setting<String> CDC_CK_DB_CONFIG = Setting.simpleString(PluginSettings.CDC_CK_DB, "",
            Setting.Property.NodeScope, Setting.Property.Dynamic);
    static final Setting<Integer> CDC_CK_REQUEST_TIMEOUT_MS_CONFIG = Setting.intSetting(
        PluginSettings.CDC_CK_REQUEST_TIMEOUT_MS, 30 * 1000, Setting.Property.NodeScope, Setting.Property.Dynamic);
    static final Setting<Integer> CDC_CK_BATCH_SIZE_CONFIG = Setting.intSetting(
        PluginSettings.CDC_CK_BATCH_SIZE, 10000, Setting.Property.NodeScope, Setting.Property.Dynamic);
    static final Setting<Integer> CDC_CK_BATCH_TIME_MS_CONFIG = Setting.intSetting(
        PluginSettings.CDC_CK_BATCH_TIME_MS, 20 * 1000, Setting.Property.NodeScope, Setting.Property.Dynamic);


    public static void addClusterLevelSettings(final ClusterSettings clusterSettings) {
        try {
            // get initial configurations when node starting up.
            for (Field field : CkClientSettings.class.getDeclaredFields()) {
                // TODO
                log.error("addClusterLevelSettings field:" + (field != null && field.get(null) != null ?
                    field.get(null).toString() : null));
                // assert field != null; 该方式在反射时会多一个 assertionsDisabled 属性，不使用
                // ignore settings/log field
                if ("settings".equals(field.getName()) || "log".equals(field.getName())) {
                    continue;
                }
                final Setting setting = (Setting) field.get(null);
                final Object value = clusterSettings.get(setting);
                if (value != null) {
                    settings.put(setting.getKey(), value);
                }
                CkConnectionPool.getInstance().refreshAllProducers(settings);
                // add settings' update consumer
                clusterSettings.addSettingsUpdateConsumer(setting, s -> updateCkConfigSetting(setting.getKey(), s));
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * update ck settings config 监听 处理接口
     * @param propertyName
     * @param propertyValue
     */
    public static void updateCkConfigSetting(String propertyName, Object propertyValue) {
        settings.put(propertyName, propertyValue);
        // notify ck connection pool to refresh all producer configurations. TODO
        CkConnectionPool.getInstance().refreshAllProducers(settings);
    }
}
