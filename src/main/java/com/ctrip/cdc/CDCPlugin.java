package com.ctrip.cdc;


import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CDCPlugin extends Plugin {
    List<Setting<?>> settings = new ArrayList<>();
    private final Setting<Boolean> cdcEnableSetting = Setting.boolSetting(PluginSettings.CDC_ENABLED, false,
            Setting.Property.IndexScope, Setting.Property.Dynamic);
    public CDCPlugin() {
        settings.add(cdcEnableSetting);
        // index level settings
        settings.add(Setting.simpleString(PluginSettings.CDC_CK_TABLE, "",
            Setting.Property.IndexScope, Setting.Property.Dynamic));
        settings.add(Setting.simpleString(PluginSettings.CDC_EXCLUDE_COLS, "",
                Setting.Property.IndexScope, Setting.Property.Dynamic));
//        settings.add(Setting.simpleString(PluginSettings.CDC_PK_COL, "",
//                Setting.Property.IndexScope, Setting.Property.Dynamic));
        settings.add(Setting.simpleString(PluginSettings.CDC_ALIAS, "",
                Setting.Property.IndexScope, Setting.Property.Dynamic));

        // cluster level settings
        settings.add(CkClientSettings.CDC_CK_DOMAIN_CONFIG);
        settings.add(CkClientSettings.CDC_CK_USERNAME_CONFIG);
        settings.add(CkClientSettings.CDC_CK_PASSWORD_CONFIG);
        settings.add(CkClientSettings.CDC_CK_REQUEST_TIMEOUT_MS_CONFIG);
        settings.add(CkClientSettings.CDC_CK_BATCH_SIZE_CONFIG);
        settings.add(CkClientSettings.CDC_CK_BATCH_TIME_MS_CONFIG);
        settings.add(CkClientSettings.CDC_CK_DB_CONFIG);

    }

    @Override
    public List<Setting<?>> getSettings() {
        return settings;
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        System.out.println("exec onIndexModule");
        final CDCListener cdcListener = new CDCListener(indexModule);
        indexModule.addSettingsUpdateConsumer(cdcEnableSetting, cdcListener);
        indexModule.addIndexOperationListener(cdcListener);
    }

    /**
     * node启动时候加载
     * @param client
     * @param clusterService
     * @param threadPool
     * @param resourceWatcherService
     * @param scriptService
     * @param xContentRegistry
     * @param environment
     * @param nodeEnvironment
     * @param namedWriteableRegistry
     * @return
     */
    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool, ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry, Environment environment, NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        System.out.println("exec createComponents");
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        // monitor dynamic cluster level settings changes
        CkClientSettings.addClusterLevelSettings(clusterSettings);
        return super.createComponents(client, clusterService, threadPool, resourceWatcherService, scriptService, xContentRegistry, environment, nodeEnvironment, namedWriteableRegistry);
    }
}