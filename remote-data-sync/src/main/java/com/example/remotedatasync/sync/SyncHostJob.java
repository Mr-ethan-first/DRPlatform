package com.example.remotedatasync.sync;

import com.example.remotedatasync.bean.DatabaseConfig;
import com.example.remotedatasync.bean.DatabaseMapping;
import com.example.remotedatasync.bean.DbTableIgnore;
import com.example.remotedatasync.bean.RowChange;
import com.example.remotedatasync.bean.TableInfo;
import com.example.remotedatasync.config.DRPlatformProperties;
import com.example.remotedatasync.enums.DelivationStatusEnum;
import com.example.remotedatasync.enums.SyncStateEnum;
import com.example.remotedatasync.manager.DynamicShardedConnectionManager;
import com.example.remotedatasync.manager.GroupedExecutorManager;
import com.example.remotedatasync.manager.TableLevelQueueManager;
import com.example.remotedatasync.po.SyncProgress;
import com.example.remotedatasync.po.SyncWatermark;
import com.example.remotedatasync.service.DatabaseMetadataService;
import com.example.remotedatasync.service.SyncWatermarkService;
import com.example.remotedatasync.sink.DataBaseSinkFunction;
import com.example.remotedatasync.sink.WatermarkAck;
import com.example.remotedatasync.util.MatchPattern;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 主机级同步作业（生产中心主机 -> 灾备中心主机，1:1）。
 * <p>
 * 每一轮扫描：发现源主机下所有用户库（排除系统库与忽略库）与所有表（排除忽略表），
 * 自动同步；新增的库 / 表在下一轮被实时纳入。DDL（新增表 / 删除表 / 表结构变更）亦自动同步，
 * 受 ignoreDdlTables（仅忽略 DDL）与 ignoreTables（DML+DDL 全忽略）控制。
 * </p>
 * <p>
 * <b>数据一致性模型（at-least-once + 幂等 = 绝对一致且不丢）</b>：
 * <ul>
 *   <li>水位（每表 update_time 续扫点）仅在<b>目标库批次事务成功提交并经 {@link #ack} 回调</b>后才推进；
 *       水位绝不会越过未落库的数据。</li>
 *   <li>水位持久化到 sync_watermark 表（每轮末尾落盘），断电 / 进程崩溃 / 灾备中心重启后从持久化水位
 *       做<b>增量续传</b>，不再全表重扫。</li>
 *   <li>目标库不可用 / 写入慢时，Sink 入队改为<b>背压</b>而非丢弃，水位不推进；恢复后继续，绝不丢数。</li>
 * </ul>
 *
 * @author 50707
 */
@Slf4j
public class SyncHostJob implements WatermarkAck {

    private final DatabaseMapping mapping;
    private final DRPlatformProperties props;
    private final DynamicShardedConnectionManager connMgr;
    private final GroupedExecutorManager executorManager;
    private final TableLevelQueueManager queueManager;
    private final DatabaseMetadataService metadataService;
    private final Map<String, SyncProgress> progressMap;
    private final SyncWatermarkService watermarkService;

    private final String instanceKey;
    private final List<String> ignoreDatabases;
    private final List<String> ignoreTables;
    private final List<String> ignoreDdlTables;
    private final List<DbTableIgnore> ignoreTablesByDb;
    private final List<DbTableIgnore> ignoreDdlTablesByDb;
    private final List<String> commonIgnoreTables;
    private final List<String> commonDdlIgnoreTables;
    private final List<com.example.remotedatasync.bean.TransformRule> transformRules;

    /** 每个库一个独立的库级 Sink（db -> sink），保证按真实库名解析表结构与写入 */
    private final Map<String, DataBaseSinkFunction> sinksByDb = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;

    /** 表级 update_time 水位（db.table -> 水位），用于增量同步（已 ack 推进） */
    private final Map<String, LocalDateTime> watermarks = new ConcurrentHashMap<>();
    /** 表级增量水位末位主键（db.table -> 主键值），与水位配合做 keyset 续扫，避免同时间戳大批量行被 LIMIT 截断漏扫 */
    private final Map<String, Object> watermarkKeys = new ConcurrentHashMap<>();
    /** 待持久化水位（acked 后构建，syncOneDatabase 末尾批量落盘到 sync_watermark 表） */
    private final Map<String, SyncWatermark> pendingWatermarks = new ConcurrentHashMap<>();
    /** 表结构指纹（db.table -> 列签名），用于 ALTER 检测 */
    private final Map<String, String> knownSignatures = new ConcurrentHashMap<>();
    /** 每个库当前已发现的表集合，用于 DROP 检测 */
    private final Map<String, Set<String>> knownTablesByDb = new ConcurrentHashMap<>();
    /** 待确认删除计数（db.table -> 主键 -> 连续「源端不存在」轮数），防误删 */
    private final Map<String, Map<String, Integer>> reconcileMiss = new ConcurrentHashMap<>();
    /** 已告警过的「秒级 update_time」表，避免每轮刷屏 */
    private final Set<String> warnedSecondGranularityUpdateTime = ConcurrentHashMap.newKeySet();
    /** 本作业是否由「主备切换」显式换源而来（换源后新源可能落后，需防误删） */
    private volatile boolean sourceSwitched = false;
    /** 换源后的删除对账静默期截止时间戳（仅换源时生效；崩溃重启同一源不设静默期） */
    private volatile long switchGraceUntil = 0L;

    public SyncHostJob(DatabaseMapping mapping, DRPlatformProperties props,
                       DynamicShardedConnectionManager connMgr, GroupedExecutorManager executorManager,
                       TableLevelQueueManager queueManager, DatabaseMetadataService metadataService,
                       Map<String, SyncProgress> progressMap, SyncWatermarkService watermarkService) {
        this.mapping = mapping;
        this.props = props;
        this.connMgr = connMgr;
        this.executorManager = executorManager;
        this.queueManager = queueManager;
        this.metadataService = metadataService;
        this.progressMap = progressMap;
        this.watermarkService = watermarkService;
        this.instanceKey = mapping.getInstanceKey();
        this.ignoreDatabases = mapping.getIgnoreDatabases() == null ? new ArrayList<>() : mapping.getIgnoreDatabases();
        this.ignoreTables = mapping.getIgnoreTables() == null ? new ArrayList<>() : mapping.getIgnoreTables();
        this.ignoreDdlTables = mapping.getIgnoreDdlTables() == null ? new ArrayList<>() : mapping.getIgnoreDdlTables();
        this.ignoreTablesByDb = mapping.getIgnoreTablesByDb() == null ? new ArrayList<>() : mapping.getIgnoreTablesByDb();
        this.ignoreDdlTablesByDb = mapping.getIgnoreDdlTablesByDb() == null ? new ArrayList<>() : mapping.getIgnoreDdlTablesByDb();
        this.commonIgnoreTables = mapping.getCommonIgnoreTables() == null ? new ArrayList<>() : mapping.getCommonIgnoreTables();
        this.commonDdlIgnoreTables = mapping.getCommonDdlIgnoreTables() == null ? new ArrayList<>() : mapping.getCommonDdlIgnoreTables();
        this.transformRules = mapping.getTransformRules() == null ? new ArrayList<>() : mapping.getTransformRules();
        // 启动时加载持久化水位（崩溃恢复 / 断点续传，避免全表重扫）
        loadPersistedWatermarks();
    }

    /**
     * 标记本作业为「主备切换后换源」：启动一段删除对账静默期，防止新源（备库）尚未追平时
     * 把「已同步但备库还没复制到」的数据误删。
     * <p>
     * 仅由 {@link DatabaseSyncManager#switchSourceToStandby(String)} 在主动换源时调用；
     * 崩溃/断电重启若仍指向同一源，则不设静默期（源数据未变，删除判定是权威的）。
     * </p>
     */
    public void markSourceSwitched() {
        this.sourceSwitched = true;
        this.switchGraceUntil = System.currentTimeMillis()
                + Math.max(0L, props.getEngine().getPostStartReconcileGraceMs());
        this.reconcileMiss.clear();
        // 换源后清空内存水位：与 DatabaseSyncManager 删除持久化水位配套，
        // 保证本作业立即对全库做一次完整重扫（而不只是等下次重启才生效）。
        this.watermarks.clear();
        this.watermarkKeys.clear();
        this.pendingWatermarks.clear();
        log.info("Source switched: delete-reconciliation grace until {} ({} ms); watermarks cleared for full rescan",
                switchGraceUntil, props.getEngine().getPostStartReconcileGraceMs());
    }

    public void start() {
        if (running) {
            return;
        }
        // 库级 Sink 采用惰性创建：每发现一个用户库，就为其创建一个绑定真实库名的 DataBaseSinkFunction
        this.running = true;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "SyncHostJob-" + instanceKey);
            t.setDaemon(true);
            return t;
        });
        long interval = Math.max(500, props.getEngine().getPollIntervalMs());
        scheduler.scheduleAtFixedRate(this::safeSync, 0, interval, TimeUnit.MILLISECONDS);
        log.info("SyncHostJob started for {}", instanceKey);
    }

    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        for (DataBaseSinkFunction s : sinksByDb.values()) {
            try {
                s.close();
            } catch (Exception e) {
                log.warn("close sink failed", e);
            }
        }
        sinksByDb.clear();
        log.info("SyncHostJob stopped for {}", instanceKey);
    }

    /** 按库惰性获取 / 创建库级 Sink（绑定真实库名，避免 db=null 无法解析表结构） */
    private DataBaseSinkFunction getSink(String db) {
        return sinksByDb.computeIfAbsent(db, k -> new DataBaseSinkFunction(
                mapping.toSourceDB(k), mapping.toTargetDB(k),
                connMgr, executorManager, queueManager, metadataService,
                props.getEngine().getBatchSize(), 5, instanceKey + "." + k,
                k, this,
                java.util.Collections.emptySet()));
    }

    /**
     * 判断某库某表是否应「DML + DDL 全忽略」。命中以下任一即忽略：
     * <ol>
     *   <li>旧式扁平 {@code ignoreTables}（支持「库.表」限定）；</li>
     *   <li>通用忽略 {@code commonIgnoreTables}（对所有库生效，按表名精确 / glob / re: 正则）；</li>
     *   <li>层级忽略 {@code ignoreTablesByDb}：库名命中且表名命中该库下的表规则。</li>
     * </ol>
     */
    private boolean isDmlIgnored(String db, String table) {
        if (MatchPattern.matchesTable(ignoreTables, db, table)) {
            return true;
        }
        if (commonIgnoreTables != null && MatchPattern.matchesAny(commonIgnoreTables, table)) {
            return true;
        }
        if (ignoreTablesByDb != null) {
            for (DbTableIgnore e : ignoreTablesByDb) {
                if (e == null || e.getDatabase() == null) {
                    continue;
                }
                if (MatchPattern.matchesAny(List.of(e.getDatabase()), db)
                        && e.getTables() != null && MatchPattern.matchesTable(e.getTables(), db, table)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 判断某库某表是否应「仅忽略 DDL（结构变更），但 DML 数据仍同步」。命中以下任一即忽略 DDL：
     * <ol>
     *   <li>旧式扁平 {@code ignoreDdlTables}（支持「库.表」限定）；</li>
     *   <li>通用 DDL 忽略 {@code commonDdlIgnoreTables}（对所有库生效）；</li>
     *   <li>层级忽略 {@code ignoreDdlTablesByDb}：库名命中且表名命中该库下的表规则。</li>
     * </ol>
     */
    private boolean isDdlIgnored(String db, String table) {
        if (MatchPattern.matchesTable(ignoreDdlTables, db, table)) {
            return true;
        }
        if (commonDdlIgnoreTables != null && MatchPattern.matchesAny(commonDdlIgnoreTables, table)) {
            return true;
        }
        if (ignoreDdlTablesByDb != null) {
            for (DbTableIgnore e : ignoreDdlTablesByDb) {
                if (e == null || e.getDatabase() == null) {
                    continue;
                }
                if (MatchPattern.matchesAny(List.of(e.getDatabase()), db)
                        && e.getTables() != null && MatchPattern.matchesTable(e.getTables(), db, table)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** 全量重置：停止 -> 清空水位与目标库 -> 重新启动（水位清空，下一轮全量重扫，幂等不丢） */
    public void fullReset() {
        log.info("Full reset for {}", instanceKey);
        stop();
        watermarks.clear();
        watermarkKeys.clear();
        pendingWatermarks.clear();
        knownSignatures.clear();
        knownTablesByDb.clear();
        reconcileMiss.clear();
        start();
    }

    private void safeSync() {
        if (!running) {
            return;
        }
        try {
            doSync();
        } catch (Exception e) {
            log.error("SyncHostJob cycle failed for {}", instanceKey, e);
        }
    }

    private void doSync() {
        DatabaseConfig sourceHost = mapping.toSourceHostConfig();
        DatabaseConfig targetHost = mapping.toTargetHostConfig();

        List<String> dbs;
        // 源库可达性预检（绕过连接池，5s 超时）：被停/断网时立即标记实例为 SUSPENDED，
        // 使监控 /sync/status 能感知异常（而非永远显示正常）。
        // 注意：getAllUserDatabases 内部会吞掉 SQLException 返回空列表，单靠它无法触发标记，
        // 因此这里显式用 testServerConnection 探测（每次轮询 1 次，开销极低）。
        if (!sourceHost.testServerConnection().isOk()) {
            log.warn("Source {} unreachable (connectivity probe), marking all instance databases SUSPENDED",
                    sourceHost.getHost());
            markInstanceSuspended("源库不可达: 连接探测失败");
            return;
        }
        try {
            dbs = metadataService.getAllUserDatabases(sourceHost, ignoreDatabases);
        } catch (Exception e) {
            // 源库不可达：将本实例下所有库的同步进度标记为中止，使监控能感知异常（而非永远显示正常）
            log.error("Source {} unreachable, marking all instance databases SUSPENDED", sourceHost.getHost(), e);
            markInstanceSuspended("源库不可达: " + e.getMessage());
            return;
        }
        for (String db : dbs) {
            if (!running) {
                return;
            }
            if (MatchPattern.matchesAny(ignoreDatabases, db)) {
                continue;
            }
            boolean ok;
            try {
                ok = syncOneDatabase(db, sourceHost, targetHost);
            } catch (Exception e) {
                // 单库同步失败不中断其他库的同步
                log.error("Sync database {} failed, continuing to next db", db, e);
                ok = false;
            }
            if (ok) {
                markDbNormal(db);
            } else {
                markDbSuspended(db, "同步失败（目标库不可达或表结构异常）");
            }
        }
    }

    /** 同步单个数据库（含建库、表扫描、DDL检测、数据同步、删除对账、进度更新）。返回是否成功同步。 */
    private boolean syncOneDatabase(String db, DatabaseConfig sourceHost, DatabaseConfig targetHost) {
        if (!metadataService.ensureTargetDatabase(targetHost, db)) {
            log.warn("Skip db {}: cannot ensure target database on {}", db, targetHost.getHost());
            return false;
        }
        DatabaseConfig sourceDB = mapping.toSourceDB(db);
        DatabaseConfig targetDB = mapping.toTargetDB(db);
        DataBaseSinkFunction sink = getSink(db);

        List<String> tables = metadataService.listTables(sourceDB, db);
        Set<String> current = new HashSet<>(tables);
        int tableCount = 0;
        long sourceRows = 0;
        long targetRows = 0;

        log.info("Start syncing database {} ({} tables)", db, tables.size());

        for (String table : tables) {
            if (!running) {
                return false;
            }
            if (isDmlIgnored(db, table)) {
                continue; // DML + DDL 全忽略
            }
            boolean ddlIgnored = isDdlIgnored(db, table);
            try {
                if (!ddlIgnored) {
                    detectAndApplyAlter(db, table, sourceDB, targetDB);
                }
                if (sink.ensureTable(table) == null) {
                    continue;
                }
                long synced = syncTable(sink, db, table, sourceDB, targetDB);
                sourceRows += synced;
                targetRows += countRows(targetDB, db, table);
                tableCount++;
            } catch (Exception e) {
                log.error("Sync table failed {}.{}", db, table, e);
            }
        }

        // DROP 检测：源库已不存在的表 -> 删除目标库对应表
        Set<String> prev = knownTablesByDb.get(db);
        if (prev != null) {
            for (String t : prev) {
                if (!current.contains(t) && !isDmlIgnored(db, t)) {
                    try {
                        sink.removeTable(t);
                        knownSignatures.remove(db + "." + t);
                        log.info("DDL DROP detected {}.{}, removed target table", db, t);
                    } catch (Exception e) {
                        log.warn("Failed to drop target table {}.{}", db, t, e);
                    }
                }
            }
        }
        knownTablesByDb.put(db, current);

        try {
            sink.flush(15000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 本轮已 ack 的水位落盘（崩溃恢复关键：重启后从持久化水位增量续传，不丢）
        persistWatermarks();
        updateProgress(db, tableCount, sourceRows, targetRows);
        log.info("Finished syncing database {} ({} tables, src={} rows, tgt={} rows)", db, tableCount, sourceRows, targetRows);
        return true;
    }

    /** ALTER 检测：源表结构变化则在目标端重建表并重置水位（忽略 DDL 的表跳过） */
    private void detectAndApplyAlter(String db, String table, DatabaseConfig sourceDB, DatabaseConfig targetDB) {
        TableInfo cur = metadataService.getTableInfo(sourceDB, db, table);
        if (cur == null) {
            return;
        }
        String sig = signature(cur);
        String prev = knownSignatures.get(db + "." + table);
        if (prev != null && !prev.equals(sig)) {
            // 崩溃安全的执行顺序：先让「需全量重扫」落盘，再动目标表。
            // 若顺序反过来（先 DROP 目标表再删水位），在两步之间崩溃会导致：
            // 目标表已被清空，而持久化水位仍是旧值 → 重启后走增量续传 → 历史数据永久缺失。
            watermarkService.delete(mapping.getSourceHost(), db, table);
            // 驱逐该表 Sink，否则其缓存的 TableInfo 仍是 DDL 之前的旧结构，
            // executeUpsert 会按旧列名集合过滤，把 DDL 新增列的数据静默丢弃。
            DataBaseSinkFunction dbSink = sinksByDb.get(db);
            if (dbSink != null) {
                dbSink.evictTableSink(table);
            }
            metadataService.dropTargetTable(targetDB, table);
            metadataService.ensureTargetTable(sourceDB, targetDB, table);
            watermarks.remove(db + "." + table);
            watermarkKeys.remove(db + "." + table);
            pendingWatermarks.remove(db + "." + table);
            reconcileMiss.remove(db + "." + table);
            log.info("DDL ALTER detected {}.{}, dropped watermark + evicted stale sink + recreated target table",
                    db, table);
        }
        knownSignatures.put(db + "." + table, sig);
    }

    /**
     * 单表增量/全量同步。
     * <ul>
     *   <li>有 update_time 且已有水位 → 增量：基于水位扫描，单批有 LIMIT 上限；</li>
     *   <li>否则 → 全量：<b>流式分页</b>扫描（{@link #streamScan}），逐页处理，内存占用恒定，
     *        即使千万级行也不会把整表一次性加载进 JVM 堆（修复百万数据 OOM / 丢失）。</li>
     * </ul>
     * 返回本周期扫描到的行数（用于进度展示）。
     */
    private long syncTable(DataBaseSinkFunction sink, String db, String table, DatabaseConfig sourceDB, DatabaseConfig targetDB) {
        TableInfo info = sink.tableInfoMap.get(table);
        if (info == null) {
            return 0;
        }
        boolean hasUpdateTime = info.getColumns().stream().anyMatch(c -> "update_time".equalsIgnoreCase(c));
        String wmKey = db + "." + table;
        LocalDateTime wm = watermarks.get(wmKey);
        boolean isFullSync = (wm == null);
        List<String> pkCols = info.getPrimaryKeyColumns();
        String pkCol = (pkCols != null && pkCols.size() == 1) ? pkCols.get(0) : null;
        long t0 = System.currentTimeMillis();
        long scanned;
        if (hasUpdateTime && wm != null) {
            // 增量扫描。基于 (update_time, 主键) 的 keyset 严格推进：水位绝不越过未落库数据。
            //
            // 【回看窗口】update_time 为秒级 DATETIME 时，同一秒内的二次修改不会推进时间戳，
            // 其 (update_time, pk) 恰等于已 ack 水位 → 纯前向键集扫描永远看不到该行，导致副本漂移。
            // 因此每轮先从「水位 - 回看窗口」重扫一遍（upsert 幂等，重读无副作用）。
            //
            // 【关键】重扫必须在<b>单轮内用独立游标连续推进</b>，直到追上原水位为止。
            // 若只把时间边界回退、而每轮仍以 LIMIT 取第一页，则同一时间戳下的大量行
            // （例如一次批量导入产生几千行相同 update_time）会永远只读到前 N 行，
            // 且其 ack 恰等于水位 → 水位无法推进 → <b>同步永久停滞</b>。
            int lookback = Math.max(0, props.getEngine().getIncrementalLookbackSeconds());
            warnIfSecondGranularityUpdateTime(db, table, info);
            Object wmKeyVal = watermarkKeys.get(wmKey);
            scanned = incrementalScanWithLookback(sink, db, table, info, sourceDB, pkCol,
                    hasUpdateTime, wm, wmKeyVal, lookback);
        } else {
            // 全量：流式分页，内存恒定
            scanned = streamScan(sourceDB, db, table, info,
                    page -> processRows(sink, db, table, info, page, hasUpdateTime, pkCol));
        }
        long elapsed = System.currentTimeMillis() - t0;
        if (scanned > 0 || elapsed > 1000) {
            log.info("Synced {}.{}: {} rows in {}ms ({} sync)", db, table, scanned, elapsed,
                    isFullSync ? "full" : "incremental");
        }
        // 大表首次全量同步跳过 reconcileDeletes：百万级行的对账耗时极长，
        // 且首次全量同步刚写入所有数据，目标端不存在多余行。后续增量同步时会补对账。
        if (isFullSync && scanned > 100_000) {
            log.info("Skipping reconcileDeletes for {}.{} (large table first sync: {} rows)", db, table, scanned);
        } else {
            reconcileDeletes(sink, db, table, sourceDB, targetDB, info);
        }
        return scanned;
    }

    /**
     * 带回看窗口的增量扫描。
     * <p>
     * 从「原水位 − {@code lookbackSeconds}、主键边界 −∞」开始，用<b>本轮独立游标</b>连续分页推进，
     * 直到读完（返回不足一页）或已追上原水位。之所以必须单轮连续推进，是因为同一时间戳下可能存在
     * 大量行：若每轮只取第一页，这些行会被反复读取而游标永远前移不了，最终导致同步停滞。
     * </p>
     * <p>最大扫描页数受 {@code maxPages} 限制，避免极端情况下单轮占用过久（超时则下轮继续，水位不推进亦不丢数）。</p>
     *
     * @return 本轮实际扫描并投递的行数
     */
    private long incrementalScanWithLookback(DataBaseSinkFunction sink, String db, String table, TableInfo info,
                                             DatabaseConfig sourceDB, String pkCol, boolean hasUpdateTime,
                                             LocalDateTime wm, Object wmKeyVal, int lookback) {
        int pageSize = props.getEngine().getBatchSize() * 5;
        int maxPages = 500; // 上限 500 页，兜底防止异常数据导致单轮过长
        LocalDateTime cursorTime = lookback > 0 ? wm.minusSeconds(lookback) : wm;
        Object cursorKey = lookback > 0 ? null : wmKeyVal;
        long total = 0;
        for (int page = 0; page < maxPages; page++) {
            List<Map<String, Object>> rows = selectChanged(sourceDB, db, table, info, cursorTime, cursorKey, pkCol);
            if (rows.isEmpty()) {
                break;
            }
            total += processRows(sink, db, table, info, rows, hasUpdateTime, pkCol);
            // 用本批最后一行的 (update_time, pk) 作为下一页游标
            Map<String, Object> lastRow = rows.get(rows.size() - 1);
            LocalDateTime lastTime = toLocalDateTime(lastRow.get("update_time"));
            if (lastTime == null) {
                break;
            }
            Object lastKey = pkCol != null ? lastRow.get(pkCol) : null;
            boolean reachedOldWatermark = lastTime.isAfter(wm)
                    || (lastTime.equals(wm) && (lastKey == null || compareKey(lastKey, wmKeyVal) >= 0));
            cursorTime = lastTime;
            cursorKey = lastKey;
            // 已追上原水位且本页读不满 → 后面没有更多行，结束本轮
            if (reachedOldWatermark && rows.size() < pageSize) {
                break;
            }
        }
        return total;
    }

    /** 单页行处理：字段转换 / 入队 upsert（携带续扫锚点，落库成功后由 Sink 经 ack 推进水位）；返回本页处理的行数 */
    private long processRows(DataBaseSinkFunction sink, String db, String table, TableInfo info,
                             List<Map<String, Object>> rows, boolean hasUpdateTime, String pkCol) {
        if (rows == null || rows.isEmpty()) {
            return 0;
        }
        for (Map<String, Object> row : rows) {
            applyTransforms(db, table, row, info.getPrimaryKeyColumns());
            RowChange change = new RowChange(RowChange.OpType.INSERT, table, row, info.getPrimaryKeyColumns());
            // 携带续扫锚点：落库成功后由 TableSinkFunction 经 WatermarkAck 回调推进水位。
            // 注意：此处不再“入队即推进水位”，水位只会在目标库事务提交成功后才推进 → 绝不越过未落库数据。
            if (hasUpdateTime) {
                change.setWatermarkTime(toLocalDateTime(row.get("update_time")));
                if (pkCol != null) {
                    change.setWatermarkKey(row.get(pkCol));
                }
            }
            sink.invoke(change);
        }
        return rows.size();
    }

    private static LocalDateTime toLocalDateTime(Object ut) {
        if (ut instanceof Timestamp) {
            return ((Timestamp) ut).toLocalDateTime();
        }
        if (ut instanceof LocalDateTime) {
            return (LocalDateTime) ut;
        }
        return null;
    }

    /**
     * 删除对账（最终一致性关键）：目标端存在但源端已不存在的行 → 发 DELETE。
     * <b>有界实现</b>：按主键分页拉取目标端 PK，每页分批回查源端是否存在，内存占用恒定，
     * 避免一次性把整表 PK 载入 JVM 堆（百万级行不 OOM）。
     *
     * <p><b>防误删保护（主备切换 / 崩溃重启场景关键）</b>：
     * <ol>
     *   <li><b>换源静默期</b>：仅当本作业由主备切换显式换源而来（{@link #markSourceSwitched()}），
     *       在 {@code postStartReconcileGraceMs} 内<b>完全不做</b>删除对账。切换到备库时备库可能落后于
     *       原主库，若立即对账会把「已同步但备库尚未复制到」的行误判为「源端已删」而永久删除。
     *       崩溃/断电重启若仍指向同一源则不设静默期（源数据未变，删除判定是权威的）。</li>
     *   <li><b>连续确认</b>：某主键还须<b>连续 {@code reconcileDeleteConfirmRounds} 轮</b>
     *       对账都未在源端出现，才真正发 DELETE；一旦重新出现即清零计数。</li>
     *   <li><b>失败安全</b>：待确认集合超过上限时<b>停止删除</b>（宁可不删，也不误删）。</li>
     * </ol></p>
     */
    private void reconcileDeletes(DataBaseSinkFunction sink, String db, String table, DatabaseConfig sourceDB, DatabaseConfig targetDB, TableInfo info) {
        if (info.getPrimaryKeyColumns() == null || info.getPrimaryKeyColumns().isEmpty()) {
            return;
        }
        // 1) 换源静默期：主备切换后新源可能落后，先不做删除对账
        if (sourceSwitched && System.currentTimeMillis() < switchGraceUntil) {
            log.debug("Skip reconcileDeletes for {}.{} within post-switch grace period", db, table);
            return;
        }
        String pk = info.getPrimaryKeyColumns().get(0);
        String tableKey = db + "." + table;
        Map<String, Integer> missCounter = reconcileMiss.computeIfAbsent(tableKey, k -> new ConcurrentHashMap<>());
        int confirmRounds = Math.max(1, props.getEngine().getReconcileDeleteConfirmRounds());
        int maxEntries = Math.max(1000, props.getEngine().getReconcileMissMaxEntries());
        // 本轮在源端确实存在的目标行 -> 清零其计数（恢复即不再删除）
        Set<String> presentNow = new HashSet<>();
        int pageSize = props.getEngine().getBatchSize() * 5;
        Object lastKey = null;
        while (true) {
            List<Object> targetPage = selectPksPaged(targetDB, db, table, pk, pageSize, lastKey);
            if (targetPage.isEmpty()) {
                break;
            }
            // 分批回查源端：这些 PK 是否仍存在（IN 列表按 1000 分片，避免超参上限）
            Set<Object> present = new HashSet<>();
            for (int i = 0; i < targetPage.size(); i += 1000) {
                List<Object> chunk = targetPage.subList(i, Math.min(i + 1000, targetPage.size()));
                present.addAll(selectPksIn(sourceDB, db, table, pk, chunk));
            }
            for (Object tp : targetPage) {
                String pkStr = String.valueOf(tp);
                if (present.contains(tp)) {
                    presentNow.add(pkStr);
                    continue;
                }
                // 2) 计数 +1，达到连续确认轮数才真正删除
                int rounds = missCounter.merge(pkStr, 1, Integer::sum);
                if (rounds < confirmRounds) {
                    log.debug("Hold pending delete {}.{} pk={} (seen missing {} round(s))",
                            db, table, pkStr, rounds);
                    continue;
                }
                if (missCounter.size() > maxEntries) {
                    // 3) 失败安全：跟踪集合过大时停止删除
                    log.warn("Skip deletes for {}.{}: pending-miss map exceeded {} entries", db, table, maxEntries);
                    missCounter.clear();
                    return;
                }
                Map<String, Object> after = new LinkedHashMap<>();
                after.put(pk, tp);
                // DELETE 事件不携带水位（watermarkTime=null），不会推进增量水位
                sink.invoke(new RowChange(RowChange.OpType.DELETE, table, after, info.getPrimaryKeyColumns()));
                missCounter.remove(pkStr);
            }
            if (targetPage.size() < pageSize) {
                break;
            }
            lastKey = targetPage.get(targetPage.size() - 1);
        }
        // 本轮已恢复存在的行清零计数，避免历史缺失导致误删
        missCounter.keySet().removeAll(presentNow);
    }

    /**
     * 流式全量扫描：按单列主键（keyset 分页）或 OFFSET 兜底分页，逐页交给 handler，
     * 内存占用恒定（每页约 batchSize*5 行），百万/千万级行也不会 OOM。返回扫描总行数。
     */
    private long streamScan(DatabaseConfig cfg, String db, String table, TableInfo info,
                            java.util.function.Consumer<List<Map<String, Object>>> pageHandler) {
        int pageSize = props.getEngine().getBatchSize() * 5;
        List<String> pks = info.getPrimaryKeyColumns();
        long total = 0;
        long lastLogTime = System.currentTimeMillis();
        if (pks != null && pks.size() == 1) {
            String pk = pks.get(0);
            Object lastKey = null;
            while (true) {
                String sql = "SELECT * FROM `" + db + "`.`" + table + "`"
                        + (lastKey == null ? "" : " WHERE `" + pk + "` > ?")
                        + " ORDER BY `" + pk + "` ASC LIMIT " + pageSize;
                List<Map<String, Object>> page = queryPage(cfg, sql, lastKey);
                if (page.isEmpty()) {
                    break;
                }
                pageHandler.accept(page);
                total += page.size();
                // 大表进度日志：每 10 万行或每 30 秒输出一次
                if (total % 100_000 < pageSize || System.currentTimeMillis() - lastLogTime > 30_000) {
                    log.info("streamScan progress {}.{}: {} rows scanned", db, table, total);
                    lastLogTime = System.currentTimeMillis();
                }
                if (page.size() < pageSize) {
                    break;
                }
                lastKey = page.get(page.size() - 1).get(pk);
                if (lastKey == null) {
                    break;
                }
            }
        } else {
            // 无单列主键：OFFSET 分页兜底（深翻页略慢，但内存仍恒定）
            long offset = 0;
            while (true) {
                String sql = "SELECT * FROM `" + db + "`.`" + table + "` LIMIT " + pageSize + " OFFSET " + offset;
                List<Map<String, Object>> page = queryRows(cfg, db, table, sql);
                if (page.isEmpty()) {
                    break;
                }
                pageHandler.accept(page);
                total += page.size();
                offset += page.size();
                if (page.size() < pageSize) {
                    break;
                }
            }
        }
        return total;
    }

    /**
     * 告警（每表仅一次）：检测到 {@code update_time} 为「秒级 DATETIME」时给出根治建议。
     * <p>
     * 秒级精度下，同一秒内的多次修改只有最后一次时间戳，键集 (update_time, pk) 无法区分 →
     * 除首轮外可能被水位跳过。已由「回看窗口」兜底，但根治方式是改用 {@code DATETIME(3)/(6)}。
     */
    private void warnIfSecondGranularityUpdateTime(String db, String table, TableInfo info) {
        String key = db + "." + table;
        if (!warnedSecondGranularityUpdateTime.add(key)) {
            return;
        }
        String defs = info.getColumnDefs();
        if (defs == null || defs.isEmpty()) {
            return;
        }
        for (String part : defs.split(";")) {
            if (part.startsWith("update_time:")) {
                String type = part.substring("update_time:".length());
                int idx = type.indexOf(':');
                if (idx > 0) {
                    type = type.substring(0, idx);
                }
                if ("datetime".equalsIgnoreCase(type.trim())) {
                    log.warn("Table {}.{} uses second-granularity DATETIME update_time; same-second repeated "
                            + "updates may be missed between rescans. Recommend DATETIME(3) or DATETIME(6) "
                            + "for precise incremental sync (mitigated by engine.incremental-lookback-seconds).",
                            db, table);
                }
                return;
            }
        }
    }

    /** 参数化分页查询（用于 keyset 流式扫描） */
    private List<Map<String, Object>> queryPage(DatabaseConfig cfg, String sql, Object lastKey) {
        List<Map<String, Object>> result = new ArrayList<>();
        Connection conn = null;
        try {
            conn = connMgr.getConnection(cfg);
            try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
                if (lastKey != null) {
                    ps.setObject(1, lastKey);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    result.addAll(extractRows(rs));
                }
            }
        } catch (SQLException e) {
            log.error("queryPage failed: {}", sql, e);
        } finally {
            closeQuietly(conn, cfg);
        }
        return result;
    }

    private List<Map<String, Object>> selectChanged(DatabaseConfig cfg, String db, String table, TableInfo info,
                                                    LocalDateTime since, Object wmKeyVal, String pkCol) {
        String sql;
        if (pkCol != null) {
            // keyset 续扫：(update_time, 主键) 联合推进，避免同时间戳大批量行被 LIMIT 截断漏扫
            sql = "SELECT * FROM `" + db + "`.`" + table + "` WHERE (`update_time` > ?) OR (`update_time` = ? AND `"
                    + pkCol + "` > ?) ORDER BY `update_time` ASC, `" + pkCol + "` ASC LIMIT "
                    + (props.getEngine().getBatchSize() * 5);
        } else {
            sql = "SELECT * FROM `" + db + "`.`" + table + "` WHERE `update_time` > ? ORDER BY `update_time` ASC LIMIT "
                    + (props.getEngine().getBatchSize() * 5);
        }
        List<Map<String, Object>> result = new ArrayList<>();
        Connection conn = null;
        try {
            conn = connMgr.getConnection(cfg);
            try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setObject(1, since);
                if (pkCol != null) {
                    ps.setObject(2, since);
                    ps.setObject(3, wmKeyVal);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    result.addAll(extractRows(rs));
                }
            }
        } catch (SQLException e) {
            log.error("selectChanged failed {}.{}", db, table, e);
        } finally {
            closeQuietly(conn, cfg);
        }
        return result;
    }

    private List<Map<String, Object>> queryRows(DatabaseConfig cfg, String db, String table, String sql) {
        List<Map<String, Object>> result = new ArrayList<>();
        Connection conn = null;
        try {
            conn = connMgr.getConnection(cfg);
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                result.addAll(extractRows(rs));
            }
        } catch (SQLException e) {
            log.error("queryRows failed: {}", sql, e);
        } finally {
            closeQuietly(conn, cfg);
        }
        return result;
    }

    private List<Map<String, Object>> extractRows(ResultSet rs) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        int colCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= colCount; i++) {
                row.put(rs.getMetaData().getColumnLabel(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }

    /** 分页拉取目标端 PK（keyset 分页，内存恒定） */
    private List<Object> selectPksPaged(DatabaseConfig cfg, String db, String table, String pk, int pageSize, Object lastKey) {
        List<Object> pks = new ArrayList<>();
        Connection conn = null;
        try {
            conn = connMgr.getConnection(cfg);
            String sql = "SELECT `" + pk + "` FROM `" + db + "`.`" + table + "`"
                    + (lastKey == null ? "" : " WHERE `" + pk + "` > ?")
                    + " ORDER BY `" + pk + "` ASC LIMIT " + pageSize;
            try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
                if (lastKey != null) {
                    ps.setObject(1, lastKey);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        pks.add(rs.getObject(1));
                    }
                }
            }
        } catch (SQLException e) {
            log.error("selectPksPaged failed {}.{}", db, table, e);
        } finally {
            closeQuietly(conn, cfg);
        }
        return pks;
    }

    /** 分批回查源端：给定 PK 集合里哪些仍存在（IN 列表按 1000 分片） */
    private List<Object> selectPksIn(DatabaseConfig cfg, String db, String table, String pk, List<Object> values) {
        List<Object> pks = new ArrayList<>();
        if (values == null || values.isEmpty()) {
            return pks;
        }
        Connection conn = null;
        try {
            conn = connMgr.getConnection(cfg);
            StringBuilder sb = new StringBuilder("SELECT `" + pk + "` FROM `" + db + "`.`" + table + "` WHERE `" + pk + "` IN (");
            for (int i = 0; i < values.size(); i++) {
                sb.append(i == 0 ? "?" : ",?");
            }
            sb.append(")");
            try (java.sql.PreparedStatement ps = conn.prepareStatement(sb.toString())) {
                for (int i = 0; i < values.size(); i++) {
                    ps.setObject(i + 1, values.get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        pks.add(rs.getObject(1));
                    }
                }
            }
        } catch (SQLException e) {
            log.error("selectPksIn failed {}.{}", db, table, e);
        } finally {
            closeQuietly(conn, cfg);
        }
        return pks;
    }

    private long countRows(DatabaseConfig cfg, String db, String table) {
        Connection conn = null;
        try {
            conn = connMgr.getConnection(cfg);
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(1) FROM `" + db + "`.`" + table + "`")) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            log.error("countRows failed {}.{}", db, table, e);
        } finally {
            closeQuietly(conn, cfg);
        }
        return -1;
    }

    /** 应用字段转换规则（跳过主键列，避免破坏幂等 upsert） */
    private void applyTransforms(String db, String table, Map<String, Object> row, List<String> pkCols) {
        if (transformRules.isEmpty() || row == null) {
            return;
        }
        Set<String> pkSet = pkCols == null ? java.util.Collections.emptySet() : new HashSet<>(pkCols);
        for (Map.Entry<String, Object> e : row.entrySet()) {
            String col = e.getKey();
            if (pkSet.contains(col)) {
                continue;
            }
            Object val = e.getValue();
            if (val == null) {
                continue;
            }
            String strVal = String.valueOf(val);
            for (com.example.remotedatasync.bean.TransformRule rule : transformRules) {
                if (rule.matches(db, table, col) && strVal.equals(rule.getSourceValue())) {
                    e.setValue(rule.getTargetValue());
                    log.debug("Transform applied {}.{}.{} : '{}' -> '{}'", db, table, col, rule.getSourceValue(), rule.getTargetValue());
                    break;
                }
            }
        }
    }

    private void updateProgress(String db, int tableCount, long sourceRows, long targetRows) {
        String key = mapping.getSourceHost() + "|" + db;
        SyncProgress p = progressMap.computeIfAbsent(key, k -> {
            SyncProgress sp = new SyncProgress();
            sp.setSourceIp(mapping.getSourceHost());
            sp.setSourceDbName(db);
            sp.setTargetIp(mapping.getTargetHost());
            sp.setTargetDbName(db);
            sp.setSyncStartTime(LocalDateTime.now());
            sp.setState(SyncStateEnum.FULL_SYNC.getCode());
            return sp;
        });
        p.setSourceDbName(db);
        p.setTargetDbName(db);
        p.setSourceIp(mapping.getSourceHost());
        p.setTargetIp(mapping.getTargetHost());
        p.setState(SyncStateEnum.SYNCING.getCode());
        p.setSourceBinlogFile("LOCAL-SYNC");
        p.setSourceBinlogTime(LocalDateTime.now());
        p.setSyncBinlogFile("LOCAL-SYNC");
        p.setSyncBinlogTime(LocalDateTime.now());
        p.setDeviationTimes(0L);
        p.setDeviationStatus(DelivationStatusEnum.NORMAL.getCode());
        p.setSuspensionReason(null);
        p.setProcessingMethod(null);
        p.setUpdateTime(LocalDateTime.now());
        p.setProcessingMethod("tables=" + tableCount + ",src=" + sourceRows + ",tgt=" + targetRows);
    }

    private String signature(TableInfo info) {
        // 必须包含列定义（类型/长度/可空性），否则 VARCHAR(64)->VARCHAR(128)、INT->BIGINT
        // 这类「列名未变但定义已变」的 DDL 完全无法被识别。
        return String.join(",", info.getColumns()) + "#"
                + String.join(",", info.getPrimaryKeyColumns()) + "#"
                + (info.getColumnDefs() == null ? "" : info.getColumnDefs());
    }

    /** 同步恢复正常：将该库进度复位为同步中 / 正常偏差（被 updateProgress 周期性覆盖，这里确保即时复位）。 */
    private void markDbNormal(String db) {
        SyncProgress p = progressMap.get(mapping.getSourceHost() + "|" + db);
        if (p != null) {
            p.setState(SyncStateEnum.SYNCING.getCode());
            p.setDeviationStatus(DelivationStatusEnum.NORMAL.getCode());
            p.setSuspensionReason(null);
            p.setUpdateTime(LocalDateTime.now());
        }
    }

    /** 同步异常：将该库进度标记为中止（SUSPENDED），使监控能感知源/目标不可达。 */
    private void markDbSuspended(String db, String reason) {
        String key = mapping.getSourceHost() + "|" + db;
        SyncProgress p = progressMap.computeIfAbsent(key, k -> {
            SyncProgress sp = new SyncProgress();
            sp.setSourceIp(mapping.getSourceHost());
            sp.setSourceDbName(db);
            sp.setTargetIp(mapping.getTargetHost());
            sp.setTargetDbName(db);
            sp.setSyncStartTime(LocalDateTime.now());
            return sp;
        });
        p.setState(SyncStateEnum.SUSPENDED.getCode());
        p.setDeviationStatus(DelivationStatusEnum.ABNORMAL.getCode());
        p.setSuspensionReason(reason);
        p.setUpdateTime(LocalDateTime.now());
    }

    /** 源库整体不可达：将本实例下所有库进度标记为中止。 */
    private void markInstanceSuspended(String reason) {
        String prefix = mapping.getSourceHost() + "|";
        for (Map.Entry<String, SyncProgress> e : progressMap.entrySet()) {
            if (e.getKey().startsWith(prefix)) {
                SyncProgress p = e.getValue();
                p.setState(SyncStateEnum.SUSPENDED.getCode());
                p.setDeviationStatus(DelivationStatusEnum.ABNORMAL.getCode());
                p.setSuspensionReason(reason);
                p.setUpdateTime(LocalDateTime.now());
            }
        }
    }

    // ===================== 水位加载 / 推进 / 持久化（崩溃恢复与绝对一致性的核心） =====================

    /** 启动时从持久化水位表加载已确认水位，实现崩溃恢复 / 断点续传（避免全表重扫） */
    private void loadPersistedWatermarks() {
        try {
            Map<String, SyncWatermark> loaded = watermarkService.loadAll();
            int applied = 0;
            for (SyncWatermark w : loaded.values()) {
                if (!mapping.getSourceHost().equals(w.getSourceIp())) {
                    continue; // 仅加载本映射
                }
                String key = w.getSourceDb() + "." + w.getTableName();
                watermarks.put(key, w.getWatermarkTime());
                if (w.getWatermarkKeyObj() != null) {
                    watermarkKeys.put(key, w.getWatermarkKeyObj());
                }
                applied++;
            }
            log.info("Loaded {} persisted watermark(s) for {} (crash recovery / incremental resume)", applied, instanceKey);
        } catch (Exception e) {
            log.warn("Failed to load persisted watermarks for {}, will full rescan: {}", instanceKey, e.getMessage());
        }
    }

    /**
     * 落库确认回调（WatermarkAck）：目标库批次事务成功提交后由 TableSinkFunction 调用。
     * <p>
     * 单调推进内存水位：仅当新水位严格大于旧水位（time 更大，或 time 相等且主键更大）才推进，
     * 保证水位<b>绝不回退</b>、<b>绝不越过未落库数据</b>。推进后记录到 pendingWatermarks 待持久化。
     * </p>
     */
    @Override
    public void ack(String db, String table, LocalDateTime maxWatermarkTime, Object lastWatermarkKey) {
        if (maxWatermarkTime == null) {
            return; // 纯 DELETE 批次（无增量行），不推进增量水位
        }
        String key = db + "." + table;
        LocalDateTime cur = watermarks.get(key);
        boolean advance;
        if (cur == null) {
            advance = true;
        } else if (maxWatermarkTime.isAfter(cur)) {
            advance = true;
        } else if (maxWatermarkTime.equals(cur) && lastWatermarkKey != null
                && compareKey(lastWatermarkKey, watermarkKeys.get(key)) > 0) {
            advance = true;
        } else {
            advance = false;
        }
        if (advance) {
            watermarks.put(key, maxWatermarkTime);
            if (lastWatermarkKey != null) {
                watermarkKeys.put(key, lastWatermarkKey);
            }
        }
        // 记录待持久化（即使未推进也覆盖，保证持久化表反映最新已确认水位）
        SyncWatermark w = new SyncWatermark();
        w.setSourceIp(mapping.getSourceHost());
        w.setSourceDb(db);
        w.setTableName(table);
        w.setWatermarkTime(maxWatermarkTime);
        w.setWatermarkKey(lastWatermarkKey == null ? null : String.valueOf(lastWatermarkKey));
        pendingWatermarks.put(key, w);
    }

    /** (update_time, pk) 续扫点的主键比较：数字按数值，其余按字符串（加载时已尽量还原为 Long） */
    private int compareKey(Object a, Object b) {
        if (a instanceof Long && b instanceof Long) {
            return Long.compare((Long) a, (Long) b);
        }
        if (a instanceof Number && b instanceof Number) {
            return Long.compare(((Number) a).longValue(), ((Number) b).longValue());
        }
        return String.valueOf(a).compareTo(String.valueOf(b));
    }

    /** 将本轮已 ack 的水位批量持久化到 sync_watermark（崩溃恢复关键：重启后增量续传，不丢） */
    public void persistWatermarks() {
        if (pendingWatermarks.isEmpty()) {
            return;
        }
        try {
            watermarkService.saveBatch(new ArrayList<>(pendingWatermarks.values()));
            pendingWatermarks.clear();
        } catch (Exception e) {
            log.warn("Persist watermarks failed for {}", instanceKey, e);
        }
    }

    private void closeQuietly(Connection conn, DatabaseConfig cfg) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignore) {
            }
            connMgr.releaseConnection();
        }
    }

    public String getInstanceKey() {
        return instanceKey;
    }
}
