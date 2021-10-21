package com.ctrip.cdc;

import com.ctrip.cdc.ck.CkConnectionPool;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;

import org.json.JSONException;
import org.json.JSONObject;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * @author leon
 */
public class CDCListener implements IndexingOperationListener, Consumer<Boolean> {

    ReentrantLock updateLock = new ReentrantLock();
    ReentrantLock delLock = new ReentrantLock();

    private static final String EMPTY_STR = "";

    private static final int DELETE_OP = 0;
    private static final int INSERT_OP = 1;
    private static final int UPDATE_OP = 2;

//    private volatile boolean needInit = true;

    private final IndexModule indexModule;
    private String[] excludeColsArr;
    private String db = EMPTY_STR;
    private String table = EMPTY_STR;
    private String indexAlias = EMPTY_STR;
    private volatile boolean useAlias = false;

    private List<DataBean> deleteDataBeans = new ArrayList<>();
    private List<DataBean> updateDataBeans = new ArrayList<>();

    public CDCListener(IndexModule indexModule) {
        this.indexModule = indexModule;
//        CkConnectionPool.getInstance().registerListener(this);
    }
    private static final Logger log = LogManager.getLogger(CDCListener.class);

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        if (delete.origin() != Engine.Operation.Origin.PRIMARY // not primary shard
                || !indexModule.getSettings().getAsBoolean(PluginSettings.CDC_ENABLED, false) // not enable cdc
                || result.getFailure() != null // failed operation
                || !result.isFound()) { // not found
            return;
        }

        if (Config.isNeedInit()) {
            initConfigs();
        }

        if (EMPTY_STR.equals(table) || EMPTY_STR.equals(db)) {
            log.error("postDelete: table or db is empty");
            return;
        }

        // properties from shardId
        String indexName;
        if(useAlias) {
            indexName = indexAlias;
        } else {
            indexName = shardId.getIndex().getName();
        }

        // properties from delete
        String delId = delete.id();
        sendDeleteEvent(db, table, indexName, delId);
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (index.origin() != Engine.Operation.Origin.PRIMARY // not primary shard
                || !indexModule.getSettings().getAsBoolean(PluginSettings.CDC_ENABLED, false) // cdc not enabled
                || result.getFailure() != null  // has failure
                || result.getResultType() != Engine.Result.Type.SUCCESS) { // not success
            return;
        }

        if (Config.isNeedInit()) {
            initConfigs();
        }

        if (EMPTY_STR.equals(table) || EMPTY_STR.equals(db)) {
            log.error("postIndex: table or db is empty");
            return;
        }

        // properties from shardId
        String indexName;

        if(useAlias) {
            indexName = indexAlias;
        } else {
            indexName = shardId.getIndex().getName();
        }
        // properties from index
        String utf8ToString = index.parsedDoc().source().utf8ToString();
        sendIndexEvent(db, table, indexName, utf8ToString, result.isCreated());
    }

    private void sendIndexEvent(String db, String table, String indexName, String utf8ToString, boolean created) {
        final Map<String, Object> clusterSettings = CkClientSettings.settings;
        int batchSize = clusterSettings.get(PluginSettings.CDC_CK_BATCH_SIZE) != null ?
            Integer.parseInt(clusterSettings.get(PluginSettings.CDC_CK_BATCH_SIZE).toString()) : 10000;

        ClickHouseStatement stmt = null;
        try {
            JSONObject content = new JSONObject(utf8ToString);
            if (excludeColsArr != null) {
                for (String col : excludeColsArr) {
                    if (content.has(col)) {
                        content.remove(col);
                    } else {
                        log.error(col + " not in content.");
                    }
                }
            }
            stmt = CkConnectionPool.getInstance().getCachedClickHouseStatement();
            // lock方案 简单实现 TODO 优化
            updateLock.lock();
            DataBean b = new DataBean();
            b.setContent(content.toString());
            b.setOp(created ? INSERT_OP : UPDATE_OP);
            b.setIndex(indexName);
            b.setTs(new Date());
            updateDataBeans.add(b);
            if(CollectionUtils.isNotEmpty(updateDataBeans) && updateDataBeans.size() >= batchSize){
                write2ck(stmt, updateDataBeans, db, table);
                // clear list：考虑GC，可使用LinkedList 清理数据，避免直接替换
                updateDataBeans = new ArrayList<>();
            }
            updateLock.unlock();
        } catch (JSONException e) {
            log.error(e.getMessage());
        }
    }

    private void sendDeleteEvent(String db, String table, String indexName, String delId) {
        final Map<String, Object> clusterSettings = CkClientSettings.settings;
        int batchSize = clusterSettings.get(PluginSettings.CDC_CK_BATCH_SIZE) != null ?
            Integer.parseInt(clusterSettings.get(PluginSettings.CDC_CK_BATCH_SIZE).toString()) : 10000;

        ClickHouseStatement stmt = null;
        try {
            if (delId == null) {
                log.error("delete not found primary id, id is null");
                return;
            }
            JSONObject content = new JSONObject();
            // TODO , delete by query 性能测试
            content.put("_docId", delId);
            stmt = CkConnectionPool.getInstance().getCachedClickHouseStatement();
            delLock.lock();
            DataBean b = new DataBean();
            b.setContent(content.toString());
            b.setOp(DELETE_OP);
            b.setIndex(indexName);
            b.setTs(new Date());
            deleteDataBeans.add(b);
            if(CollectionUtils.isNotEmpty(deleteDataBeans) && deleteDataBeans.size() >= batchSize){
                write2ck(stmt, deleteDataBeans, db, table);
                deleteDataBeans = new ArrayList<>();
            }
            delLock.unlock();
        } catch (JSONException e) {
            log.error(e.getMessage());
        }
    }


    private void write2ck(ClickHouseStatement stmt, List<DataBean> dataBeans, String db, String table){
        // "INSERT INTO elasticsearch_cdc.elasticsearch_cdc_info"
        String sql = String.format("INSERT INTO %s.%s", db, table);
        try {
            assert stmt != null;
            stmt.write().send(sql, new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                        for (DataBean bean : dataBeans) {
                            stream.writeString(bean.getContent());
                            stream.writeInt32(bean.getOp());
                            stream.writeString(bean.getIndex());
                            stream.writeDateTime(bean.getTs());
                        }
                    }
                },
                ClickHouseFormat.RowBinary);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private synchronized void initConfigs() {
        if (!Config.isNeedInit()) {
            return;
        }

        // table and db setting
        table = EMPTY_STR;
        db = EMPTY_STR;
        indexAlias = EMPTY_STR;

        // index settings list
        final Settings settings = indexModule.getSettings();
        // cluster setting list
        final Map<String, Object> clusterSettings = CkClientSettings.settings;


        table = settings.get(PluginSettings.CDC_CK_TABLE, EMPTY_STR).trim();
        if (EMPTY_STR.equals(table)) {
//            needInit = false;
            Config.setNeedInit(false);
            return;
        }
        db = clusterSettings.get(PluginSettings.CDC_CK_DB) != null ? clusterSettings.get(PluginSettings.CDC_CK_DB).toString() : EMPTY_STR;
        if (EMPTY_STR.equals(db)) {
//            needInit = false;
            Config.setNeedInit(false);
            return;
        }

        // support alias
        indexAlias = settings.get(PluginSettings.CDC_ALIAS, EMPTY_STR).trim();
        useAlias = !EMPTY_STR.equals(indexAlias);

        final String excludeCols = indexModule.getSettings().get(PluginSettings.CDC_EXCLUDE_COLS, EMPTY_STR).trim();
        if (!EMPTY_STR.equals(excludeCols)) {
            this.excludeColsArr = excludeCols.split(",");
        }
//        needInit = false;
        Config.setNeedInit(false);
    }

    @Deprecated
    @Override
    public void accept(Boolean cdcEnabled) {
        if (cdcEnabled) {
//            needInit = true;
            Config.setNeedInit(true);
        }
    }
}
