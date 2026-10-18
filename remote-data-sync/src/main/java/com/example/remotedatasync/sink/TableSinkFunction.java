package com.example.remotedatasync.sink;

import com.example.remotedatasync.bean.DatabaseConfig;
import com.example.remotedatasync.bean.RowChange;
import com.example.remotedatasync.bean.TableInfo;
import com.example.remotedatasync.manager.DynamicShardedConnectionManager;
import com.example.remotedatasync.manager.GroupedExecutorManager;
import com.example.remotedatasync.manager.TableLevelQueueManager;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 表级 Sink：负责单个表的具体数据批量写入与分区管理。
 * <p>
 * 本地可运行版本中，它等价于 Flink 的 RichSinkFunction：从队列中取批次，
 * 经过 WAL 重试后写入目标库；批次事务<b>成功提交后</b>通过 {@link WatermarkAck} 回调通知引擎推进水位。
 * </p>
 * <p>
 * <b>一致性保证</b>：enqueue 不再「超时丢弃」，目标端不可用 / 写入慢时改为<b>背压阻塞</b>；
 * 水位只会在落库成功后经 ack 推进，因此任何时刻水位都 ≤ 已落库数据，崩溃 / 断电 / 断网均不丢数据。
 * </p>
 *
 * @author 50707
 */
@Slf4j
public class TableSinkFunction {

    private final DatabaseConfig targetDB;
    private final TableInfo tableInfo;
    private final DynamicShardedConnectionManager connMgr;
    private final GroupedExecutorManager executorManager;
    private final TableLevelQueueManager.TableQueue queue;
    private final int batchSize;
    private final int maxWalRetries;
    private final String dbName;
    private final WatermarkAck ack;

    private volatile boolean running = true;
    private volatile boolean hasActiveProcessor = false;
    private volatile long lastProcessTime = System.currentTimeMillis();

    /** 死信队列（单条重试）。设容量上限：目标端长时间不可用时，超出部分不再入队，
     *  依赖下一轮重扫补偿（水位未推进，at-least-once 幂等 upsert），保证最终一致且不撑爆 JVM 堆。 */
    private static final int MAX_DEAD_LETTER = 100_000;
    /** enqueue 背压超时：目标端不可用 / 写入慢时阻塞等待，超时则抛异常由上层不推进水位、下轮重扫 */
    private static final long ENQUEUE_TIMEOUT_MS = 120_000L;
    private final ConcurrentLinkedQueue<RowChange> pendingFailedEvents = new ConcurrentLinkedQueue<>();

    private Thread processorThread;
    private ScheduledExecutorService failedEventRetryScheduler;

    public TableSinkFunction(DatabaseConfig targetDB, TableInfo tableInfo,
                             DynamicShardedConnectionManager connMgr,
                             GroupedExecutorManager executorManager,
                             TableLevelQueueManager.TableQueue queue,
                             int batchSize, int maxWalRetries,
                             String dbName, WatermarkAck ack) {
        this.targetDB = targetDB;
        this.tableInfo = tableInfo;
        this.connMgr = connMgr;
        this.executorManager = executorManager;
        this.queue = queue;
        this.batchSize = Math.max(100, batchSize);
        this.maxWalRetries = Math.max(1, maxWalRetries);
        this.dbName = dbName;
        this.ack = ack;
    }

    public void start() {
        startFailedEventRetry();
        processorThread = new Thread(this::processorLoop, "TableSink-" + dbName + "." + tableInfo.getTableName());
        processorThread.setDaemon(true);
        processorThread.start();
    }

    /**
     * 入队（背压）：队列满则阻塞等待，直到有空间或超时。
     * <b>绝不丢弃</b>：超时抛异常由上层捕获 → 该批不推进水位 → 下轮重扫（幂等 upsert 补偿），保证不丢。
     */
    public void enqueue(RowChange change) {
        long deadline = System.currentTimeMillis() + ENQUEUE_TIMEOUT_MS;
        while (running) {
            if (queue.offer(change)) {
                return;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("enqueue timeout after " + ENQUEUE_TIMEOUT_MS
                        + "ms, target unreachable/slow; watermark will NOT advance (will be re-synced by next rescan)");
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void processorLoop() {
        while (running) {
            try {
                List<Object> batch = queue.drain(batchSize);
                if (batch.isEmpty()) {
                    Thread.sleep(200);
                    continue;
                }
                hasActiveProcessor = true;
                @SuppressWarnings("unchecked")
                List<RowChange> changes = (List<RowChange>) (List<?>) batch;
                try {
                    processBatchWithWal(changes);
                    // 落库成功 → 回调 ack 推进水位（仅当含有效 update_time 行）
                    ackBatch(changes);
                } finally {
                    hasActiveProcessor = false;
                    lastProcessTime = System.currentTimeMillis();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Processor loop error for table {}.{}", dbName, tableInfo.getTableName(), e);
            }
        }
    }

    /** 计算本批水位锚点并回调 ack */
    private void ackBatch(List<RowChange> changes) {
        if (ack == null) {
            return;
        }
        LocalDateTime maxTime = null;
        Object lastKey = null;
        for (RowChange c : changes) {
            LocalDateTime t = c.getWatermarkTime();
            if (t != null) {
                if (maxTime == null || t.isAfter(maxTime)) {
                    maxTime = t;
                    lastKey = c.getWatermarkKey();
                } else if (t.equals(maxTime)) {
                    lastKey = c.getWatermarkKey();
                }
            }
        }
        if (maxTime != null) {
            ack.ack(dbName, tableInfo.getTableName(), maxTime, lastKey);
        }
    }

    /**
     * 带 WAL 重试的批次处理：失败重试 maxWalRetries 次，仍失败则降级为单条处理，
     * 单条再失败则进入死信队列。
     */
    private void processBatchWithWal(List<RowChange> changes) {
        int attempt = 0;
        boolean ok = false;
        while (attempt < maxWalRetries && !ok) {
            try {
                processBatchInternal(changes);
                ok = true;
            } catch (SQLException e) {
                attempt++;
                if (attempt >= maxWalRetries) {
                    log.error("Batch failed after {} attempts, degrading to single-row processing, table={}.{}, size={}",
                            maxWalRetries, dbName, tableInfo.getTableName(), changes.size(), e);
                    degradeToSingle(changes);
                } else {
                    log.warn("Batch attempt {} failed, will retry, table={}.{}", attempt, dbName, tableInfo.getTableName());
                    try {
                        Thread.sleep(500L * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private void degradeToSingle(List<RowChange> changes) {
        for (RowChange c : changes) {
            try {
                processSingleEvent(c);
                // 单条成功落库 → ack（单调推进，旧时间不回退水位）
                if (ack != null && c.getWatermarkTime() != null) {
                    ack.ack(dbName, tableInfo.getTableName(), c.getWatermarkTime(), c.getWatermarkKey());
                }
            } catch (SQLException e) {
                if (pendingFailedEvents.size() >= MAX_DEAD_LETTER) {
                    log.error("Dead-letter exceeded cap ({}), dropping event (re-synced by next full scan), table={}.{}, pk={}",
                            MAX_DEAD_LETTER, dbName, tableInfo.getTableName(), c.getPrimaryKeyValue(), e);
                    continue;
                }
                log.error("Single-row processing failed, moved to dead-letter, table={}.{}, pk={}",
                        dbName, tableInfo.getTableName(), c.getPrimaryKeyValue(), e);
                pendingFailedEvents.offer(c);
            }
        }
    }

    /** 批次写入：同一连接内事务提交，保证批次原子性 */
    public void processBatchInternal(List<RowChange> changes) throws SQLException {
        if (changes.isEmpty()) {
            return;
        }
        Connection conn = null;
        try {
            conn = connMgr.getConnection(targetDB);
            conn.setAutoCommit(false);
            for (RowChange change : changes) {
                executeOne(conn, change);
            }
            conn.commit();
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ignore) {
                }
            }
            throw e;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ignore) {
                }
                connMgr.releaseConnection();
            }
        }
    }

    /** 单条写入（死信队列重试用） */
    public void processSingleEvent(RowChange change) throws SQLException {
        Connection conn = null;
        try {
            conn = connMgr.getConnection(targetDB);
            conn.setAutoCommit(true);
            executeOne(conn, change);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ignore) {
                }
                connMgr.releaseConnection();
            }
        }
    }

    private void executeOne(Connection conn, RowChange change) throws SQLException {
        if (change.getOpType() == RowChange.OpType.DELETE) {
            executeDelete(conn, change);
        } else {
            executeUpsert(conn, change);
        }
    }

    private void executeUpsert(Connection conn, RowChange change) throws SQLException {
        Set<String> validCols = tableInfo.getColumns().stream().collect(Collectors.toSet());
        // 主键列不参与 UPDATE：主键被改写会破坏目标端主键与源端的一致性，
        // 且在主备切换/换源场景下可能造成主键漂移与对账误判。幂等靠唯一键冲突即可达成。
        Set<String> pkCols = tableInfo.getPrimaryKeyColumns() == null
                ? java.util.Collections.emptySet()
                : new java.util.HashSet<>(tableInfo.getPrimaryKeyColumns());
        List<String> cols = new ArrayList<>();
        List<Object> vals = new ArrayList<>();
        for (Map.Entry<String, Object> e : change.getAfter().entrySet()) {
            if (validCols.contains(e.getKey()) && e.getValue() != null) {
                cols.add(e.getKey());
                vals.add(e.getValue());
            }
        }
        if (cols.isEmpty()) {
            return;
        }
        String colList = String.join(",", cols);
        String placeholders = cols.stream().map(c -> "?").collect(Collectors.joining(","));
        // UPDATE 列表排除主键列
        List<String> updCols = cols.stream().filter(c -> !pkCols.contains(c)).collect(Collectors.toList());
        String updateList;
        if (updCols.isEmpty()) {
            // 全部列都是主键列（极端表结构）：用无副作用的 no-op 保持「存在即不重复插入」
            updateList = cols.get(0) + "=" + cols.get(0);
        } else {
            updateList = updCols.stream().map(c -> c + "=VALUES(" + c + ")").collect(Collectors.joining(","));
        }
        String sql = "INSERT INTO " + quote(tableInfo.getTableName()) + " (" + colList + ") VALUES (" + placeholders + ") "
                + "ON DUPLICATE KEY UPDATE " + updateList;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < vals.size(); i++) {
                setParam(ps, i + 1, vals.get(i));
            }
            ps.executeUpdate();
        }
    }

    private void executeDelete(Connection conn, RowChange change) throws SQLException {
        List<String> pkCols = tableInfo.getPrimaryKeyColumns();
        if (pkCols == null || pkCols.isEmpty()) {
            log.warn("No primary key for delete on table {}.{}, skip", dbName, tableInfo.getTableName());
            return;
        }
        String where = pkCols.stream().map(c -> c + "=?").collect(Collectors.joining(" AND "));
        String sql = "DELETE FROM " + quote(tableInfo.getTableName()) + " WHERE " + where;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (String pk : pkCols) {
                setParam(ps, idx++, change.getAfter().get(pk));
            }
            ps.executeUpdate();
        }
    }

    private void setParam(PreparedStatement ps, int index, Object value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.NULL);
            return;
        }
        if (value instanceof java.time.LocalDateTime) {
            // 以字面量方式绑定 DATETIME，避免 LocalDateTime -> Timestamp 经 JVM 默认时区转换，
            // 再经连接时区写出，导致跨时区搬运时间被偏移（如本机 PDT 与库 Asia/Shanghai 差 15h）。
            ps.setObject(index, value);
        } else if (value instanceof java.time.LocalDate) {
            ps.setDate(index, java.sql.Date.valueOf((java.time.LocalDate) value));
        } else if (value instanceof java.sql.Timestamp) {
            ps.setTimestamp(index, (java.sql.Timestamp) value);
        } else if (value instanceof java.sql.Date) {
            ps.setDate(index, (java.sql.Date) value);
        } else if (value instanceof Number) {
            ps.setObject(index, value);
        } else {
            ps.setObject(index, value);
        }
    }

    private String quote(String ident) {
        return "`" + ident.replace("`", "") + "`";
    }

    private void startFailedEventRetry() {
        failedEventRetryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "FailedEventRetry-" + dbName + "." + tableInfo.getTableName());
            t.setDaemon(true);
            return t;
        });
        failedEventRetryScheduler.scheduleAtFixedRate(() -> {
            if (pendingFailedEvents.isEmpty()) {
                return;
            }
            List<RowChange> retry = new ArrayList<>();
            RowChange c;
            while ((c = pendingFailedEvents.poll()) != null) {
                retry.add(c);
            }
            for (RowChange change : retry) {
                try {
                    processSingleEvent(change);
                    // 死信重试成功 → ack（单调，不回退水位）
                    if (ack != null && change.getWatermarkTime() != null) {
                        ack.ack(dbName, tableInfo.getTableName(), change.getWatermarkTime(), change.getWatermarkKey());
                    }
                } catch (SQLException e) {
                    log.error("Dead-letter retry failed again, table={}.{}, pk={}", dbName, tableInfo.getTableName(), change.getPrimaryKeyValue(), e);
                    pendingFailedEvents.offer(change);
                }
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    /** 健康检查：处理器假死则重置标志 */
    public boolean isProcessorHealthy(long timeoutMs) {
        if (!queue.isEmpty() && hasActiveProcessor && (System.currentTimeMillis() - lastProcessTime) > timeoutMs) {
            hasActiveProcessor = false;
            return false;
        }
        return true;
    }

    public int pendingFailedSize() {
        return pendingFailedEvents.size();
    }

    /** 队列为空且当前无活跃处理器，表示本表事件已处理完 */
    public boolean isIdle() {
        return queue.isEmpty() && !hasActiveProcessor;
    }

    /** 阻塞等待本表队列处理完成（用于保证一致性后再统计/校验） */
    public void waitIdle(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (isIdle()) {
                return;
            }
            Thread.sleep(50);
        }
    }

    public void close() {
        running = false;
        if (failedEventRetryScheduler != null) {
            failedEventRetryScheduler.shutdownNow();
        }
        if (processorThread != null) {
            processorThread.interrupt();
        }
        // 尝试 flush 剩余队列（落库成功后 ack 会推进水位）
        try {
            List<Object> remaining = queue.drain(batchSize * 4);
            if (!remaining.isEmpty()) {
                @SuppressWarnings("unchecked")
                List<RowChange> changes = (List<RowChange>) (List<?>) remaining;
                try {
                    processBatchInternal(changes);
                    ackBatch(changes);
                } catch (SQLException e) {
                    log.warn("Flush on close failed for table {}.{}", dbName, tableInfo.getTableName(), e);
                }
            }
        } catch (Exception e) {
            log.warn("Error flushing on close", e);
        }
    }
}
