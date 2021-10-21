package com.ctrip.cdc;

public class PluginSettings {
    public static final String CDC_ENABLED = "index.cdc.enabled";
    public static final String CDC_EXCLUDE_COLS = "index.cdc.exclude.columns";
    public static final String CDC_CK_TABLE = "index.cdc.ck.table";
    // for support alias
    public static final String CDC_ALIAS = "index.cdc.alias";

    // clutser level settings constant
    public static final String CLUSTER_SETTING_PREFIX = "indices.cdc.";
    public static final String CDC_CK_DOMAIN = CLUSTER_SETTING_PREFIX + "ck.domain";
    public static final String CDC_CK_USERNAME = CLUSTER_SETTING_PREFIX + "ck.username";
    public static final String CDC_CK_PASSWORD = CLUSTER_SETTING_PREFIX + "ck.password";
    public static final String CDC_CK_REQUEST_TIMEOUT_MS = CLUSTER_SETTING_PREFIX + "ck.request.timeout.ms";
    public static final String CDC_CK_BATCH_SIZE = CLUSTER_SETTING_PREFIX + "ck.batch.size";
    public static final String CDC_CK_BATCH_TIME_MS = CLUSTER_SETTING_PREFIX + "ck.batch.time";
    public static final String CDC_CK_DB = CLUSTER_SETTING_PREFIX + "ck.db";
}
