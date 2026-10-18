package com.example.remotedatasync.sink;

import com.example.remotedatasync.bean.DatabaseConfig;
import com.example.remotedatasync.bean.RowChange;
import com.example.remotedatasync.bean.TableInfo;
import com.example.remotedatasync.manager.DynamicShardedConnectionManager;
import com.example.remotedatasync.manager.GroupedExecutorManager;
import com.example.remotedatasync.manager.TableLevelQueueManager;
import com.example.remotedatasync.service.DatabaseMetadataService;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据库级 Sink（每个映射一个）：管理该库下所有表的写入，并处理 DDL 事件。
 * 本地可运行版本中等价于 Flink 的 KeyedSinkProcessFunction。
 * <p>
 * 持有库名与 {@link WatermarkAck} 回调，在创建表级 Sink 时透传，使表级 Sink 落库成功后可回灌水位。
 * </p>
 *
 * @author 50707
 */
@Slf4j
public class DataBaseSinkFunction {

    private final DatabaseConfig sourceDB;
    private final DatabaseConfig targetDB;
    private final DynamicShardedConnectionManager connMgr;
    private final GroupedExecutorManager executorManager;
    private final TableLevelQueueManager queueManager;
    private final DatabaseMetadataService metadataService;
    private final int batchSize;
    private final int maxWalRetries;
    private final String instanceKey;
    private final String dbName;
    private final WatermarkAck ack;
    /** 忽略同步的表（DML/DDL 均跳过） */
    private final Set<String> ignoreTables;

    /** 表名 -> 表级 Sink */
    public final Map<String, TableSinkFunction> tableFunctionMap = new ConcurrentHashMap<>();
    /** 表名 -> 表信息 */
    public final Map<String, TableInfo> tableInfoMap = new ConcurrentHashMap<>();

    private volatile boolean closed = false;

    public DataBaseSinkFunction(DatabaseConfig sourceDB, DatabaseConfig targetDB,
                                DynamicShardedConnectionManager connMgr,
                                GroupedExecutorManager executorManager,
                                TableLevelQueueManager queueManager,
                                DatabaseMetadataService metadataService,
                                int batchSize, int maxWalRetries, String instanceKey,
                                String dbName, WatermarkAck ack,
                                Set<String> ignoreTables) {
        this.sourceDB = sourceDB;
        this.targetDB = targetDB;
        this.connMgr = connMgr;
        this.executorManager = executorManager;
        this.queueManager = queueManager;
        this.metadataService = metadataService;
        this.batchSize = batchSize;
        this.maxWalRetries = maxWalRetries;
        this.instanceKey = instanceKey;
        this.dbName = dbName;
        this.ack = ack;
        this.ignoreTables = ignoreTables == null ? java.util.Collections.emptySet() : ignoreTables;
    }

    /** 确保表算子存在（动态新增表）；忽略表中的表直接跳过 */
    public synchronized TableSinkFunction ensureTable(String tableName) {
        if (ignoreTables.contains(tableName)) {
            log.info("Table {}.{} is in ignore list, skip creating sink", sourceDB.getDatabaseName(), tableName);
            return null;
        }
        TableSinkFunction sink = tableFunctionMap.get(tableName);
        if (sink != null) {
            return sink;
        }
        TableInfo info = metadataService.getTableInfo(sourceDB, sourceDB.getDatabaseName(), tableName);
        if (info == null) {
            log.warn("Cannot resolve table info for {}.{}, skip", sourceDB.getDatabaseName(), tableName);
            return null;
        }
        if (!metadataService.ensureTargetTable(sourceDB, targetDB, tableName)) {
            log.error("Failed to ensure target table {}", tableName);
            return null;
        }
        String queueKey = instanceKey + "." + tableName;
        TableSinkFunction newSink = new TableSinkFunction(targetDB, info, connMgr, executorManager,
                queueManager.getOrCreateTableQueue(queueKey, 20000), batchSize, maxWalRetries,
                dbName, ack);
        newSink.start();
        tableFunctionMap.put(tableName, newSink);
        tableInfoMap.put(tableName, info);
        log.info("Table sink started for {}.{}", targetDB.getDatabaseName(), tableName);
        return newSink;
    }

    public void invoke(RowChange change) {
        if (closed) {
            return;
        }
        TableSinkFunction sink = tableFunctionMap.get(change.getTableName());
        if (sink == null) {
            sink = ensureTable(change.getTableName());
        }
        if (sink == null) {
            log.warn("No table sink for {}, drop event", change.getTableName());
            return;
        }
        sink.enqueue(change);
    }

    /**
     * 驱逐该表的 Sink（<b>不删除目标表</b>）。
     * <p>
     * 用途：DDL 结构变更后调用。目标表已由调用方按源端新结构重建，但 Sink 内部缓存的
     * {@link TableInfo} 仍是变更前的旧列集合，若不驱逐，
     * {@link TableSinkFunction#executeUpsert} 会用旧列名过滤，把 DDL 新增列的数据静默丢弃。
     * 驱逐后下一次 {@link #ensureTable} 会按最新结构重建 Sink。
     * </p>
     */
    public void evictTableSink(String tableName) {
        TableSinkFunction sink = tableFunctionMap.remove(tableName);
        if (sink != null) {
            try {
                sink.close();
            } catch (Exception e) {
                log.warn("Failed to close stale sink for table {}", tableName, e);
            }
        }
        tableInfoMap.remove(tableName);
        queueManager.removeTableQueue(instanceKey + "." + tableName);
        log.info("Evicted stale sink for table {} (structure changed), will rebuild on next ensureTable", tableName);
    }

    /** DDL: 新增表 */
    public void addNewTable(String tableName) {
        ensureTable(tableName);
    }

    /** DDL: 删除表 */
    public void removeTable(String tableName) {
        TableSinkFunction sink = tableFunctionMap.remove(tableName);
        if (sink != null) {
            sink.close();
        }
        tableInfoMap.remove(tableName);
        queueManager.removeTableQueue(instanceKey + "." + tableName);
        metadataService.dropTargetTable(targetDB, tableName);
        log.info("Removed table sink for {}", tableName);
    }

    /** 阻塞等待所有表队列处理完成（保证一致性后再统计 / 校验） */
    public void flush(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        for (TableSinkFunction sink : tableFunctionMap.values()) {
            sink.waitIdle(Math.max(1000, deadline - System.currentTimeMillis()));
        }
    }

    public void close() {
        closed = true;
        tableFunctionMap.forEach((name, sink) -> sink.close());
        tableFunctionMap.clear();
        tableInfoMap.clear();
        queueManager.removeAllQueuesForDatabase(instanceKey);
    }
}
