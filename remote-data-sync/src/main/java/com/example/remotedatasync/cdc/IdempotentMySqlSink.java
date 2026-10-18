package com.example.remotedatasync.cdc;

import com.example.remotedatasync.bean.DatabaseConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CDC 目标库表元数据（列类型），用于把 Debezium JSON 中的时序数值转换回字面量。
 * 每个源库一次性查询 information_schema.COLUMNS 加载（与懒建表同源，压力可忽略）。
 */
class TableTypes implements Serializable {

    private static final long serialVersionUID = 1L;

    /** table -> (column -> DATA_TYPE) */
    final Map<String, Map<String, String>> types = new ConcurrentHashMap<>();

    String dataType(String table, String column) {
        Map<String, String> cols = types.get(table);
        return cols == null ? null : cols.get(column);
    }

    boolean knows(String table) {
        return types.containsKey(table);
    }
}

/**
 * 幂等 MySQL Sink：把 binlog 行事件以「写=upsert、删=按行删除」语义应用到目标库。
 *
 * <p>一致性保证（Flink checkpoint 对齐）：
 * <ul>
 *   <li>{@link #snapshotState} 在 checkpoint 屏障到达时<b>同步 flush 所有缓冲</b>，
 *       屏障通过前全部落库 → 已确认的 binlog 位点永不越过未落库数据；</li>
 *   <li>崩溃后从 checkpoint/savepoint 恢复，source 重放位点之后的事件，
 *       upsert/delete 均按主键/全列等值幂等 → 不丢不重（最终一致）；</li>
 *   <li>同一表的事件经 keyBy 路由到同一 subtask，顺序严格保持；DDL 与行事件同键，
 *       DDL 执行前先 flush 该 subtask 全部待写数据，保证结构变更与数据的相对顺序。</li>
 * </ul>
 *
 * <p>目标表不存在时懒创建（从源库 SHOW CREATE TABLE 清洗后建表），无需启动期全量建表。
 *
 * @author 50707
 */
public class IdempotentMySqlSink extends org.apache.flink.streaming.api.functions.sink.RichSinkFunction<CdcEvent>
        implements org.apache.flink.streaming.api.checkpoint.CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(IdempotentMySqlSink.class);

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter DATETIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /** 目标库共享连接池：key = host:port/db（全 JVM 共享，各 subtask 复用） */
    private static final Map<String, DataSource> POOLS = new ConcurrentHashMap<>();

    private final DatabaseConfig sourceConfig;

    private final DatabaseConfig targetConfig;

    private final CdcFilter filter;

    private final String instanceKey;

    private final int batchSize;

    private final int poolSize;

    private final ZoneId serverZone;

    /** 字段内容转换规则（源值 → 目标值），与旧轮询引擎 SyncHostJob 语义一致 */
    private final java.util.List<com.example.remotedatasync.bean.TransformRule> transformRules;

    private transient DdlApplier ddlApplier;

    /** 待写缓冲：key = db.table|op|列签名 */
    private transient Map<String, List<CdcEvent>> buffer;

    /** 已确认存在的目标表（避免每批重复查 information_schema）。transient：序列化后由 open() 重新初始化 */
    private transient java.util.Set<String> ensuredTables;

    /** 已确认存在的目标库。transient：序列化后由 open() 重新初始化 */
    private transient java.util.Set<String> ensuredDatabases;

    /** 源库只读连接（懒建表/查列类型用，每 subtask 一个） */
    private transient Connection sourceConn;

    /** 每库列类型缓存。transient：序列化后由 open() 重新初始化 */
    private transient Map<String, TableTypes> typeCaches;

    private transient int appliedCount;

    public IdempotentMySqlSink(DatabaseConfig sourceConfig, DatabaseConfig targetConfig,
                               CdcFilter filter, String instanceKey, int batchSize, int poolSize, ZoneId serverZone) {
        this(sourceConfig, targetConfig, filter, instanceKey, batchSize, poolSize, serverZone,
                java.util.Collections.emptyList());
    }

    public IdempotentMySqlSink(DatabaseConfig sourceConfig, DatabaseConfig targetConfig,
                               CdcFilter filter, String instanceKey, int batchSize, int poolSize, ZoneId serverZone,
                               java.util.List<com.example.remotedatasync.bean.TransformRule> transformRules) {
        this.sourceConfig = sourceConfig;
        this.targetConfig = targetConfig;
        this.filter = filter;
        this.instanceKey = instanceKey;
        this.batchSize = batchSize;
        this.poolSize = poolSize;
        this.serverZone = serverZone;
        this.transformRules = transformRules == null
                ? java.util.Collections.emptyList()
                : new java.util.ArrayList<>(transformRules);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        ddlApplier = new DdlApplier();
        buffer = new LinkedHashMap<>();
        ensuredTables = ConcurrentHashMap.newKeySet();
        ensuredDatabases = ConcurrentHashMap.newKeySet();
        typeCaches = new ConcurrentHashMap<>();
        appliedCount = 0;
    }

    // ===================== 事件处理 =====================

    @Override
    public void invoke(CdcEvent event, Context context) throws Exception {
        if (!filter.accept(event)) {
            return;
        }
        if (event.isSchemaChange()) {
            // DDL：先落盘所有待写数据，再按顺序执行结构变更（保序）
            flushAll();
            applyDdl(event);
            return;
        }
        if (!event.isDataChange()) {
            return;
        }
        // 字段转换：写入目标库前把命中规则的源值替换为目标值（仅非主键列，避免破坏幂等 upsert）
        applyTransforms(event);
        CdcLagRegistry.update(instanceKey, event.eventTimeMs());
        String groupKey = groupKey(event);
        List<CdcEvent> list = buffer.computeIfAbsent(groupKey, k -> new ArrayList<>());
        list.add(event);
        if (list.size() >= batchSize) {
            flushGroup(groupKey);
        }
    }

    /** 对 after/before 行数据应用字段转换规则（语义同 SyncHostJob.applyTransforms） */
    private void applyTransforms(CdcEvent event) {
        if (transformRules.isEmpty()) {
            return;
        }
        String db = event.db();
        String table = event.table();
        applyTransforms(db, table, event.getAfter());
        applyTransforms(db, table, event.getBefore());
    }

    private void applyTransforms(String db, String table, Map<String, Object> row) {
        if (row == null || row.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Object> e : row.entrySet()) {
            Object val = e.getValue();
            if (val == null) {
                continue;
            }
            String strVal = String.valueOf(val);
            for (com.example.remotedatasync.bean.TransformRule rule : transformRules) {
                if (rule.matches(db, table, e.getKey()) && strVal.equals(rule.getSourceValue())) {
                    e.setValue(rule.getTargetValue());
                    LOG.debug("Transform applied {}.{}.{} : '{}' -> '{}'",
                            db, table, e.getKey(), rule.getSourceValue(), rule.getTargetValue());
                    break;
                }
            }
        }
    }

    private String groupKey(CdcEvent event) {
        Map<String, Object> row = "d".equals(event.getOp()) ? event.getBefore() : event.getAfter();
        String cols = row == null ? "-" : String.join(",", row.keySet());
        return event.db() + "." + event.table() + "|" + event.getOp() + "|" + cols.hashCode();
    }

    private void applyDdl(CdcEvent event) throws SQLException {
        String db = event.db();
        String table = event.getDdlTable();
        ensureTargetDatabase(db);
        try (Connection conn = targetConnection(db)) {
            ddlApplier.apply(conn, db, table, event.getDdl());
            // DDL 后列类型可能变化：失效缓存，下次按需重载
            TableTypes tt = typeCaches.get(db);
            if (tt != null && table != null) {
                tt.types.remove(table);
            }
            ensuredTables.remove(db + "." + table);
        }
    }

    // ===================== 落库 =====================

    private void flushAll() throws SQLException {
        if (buffer == null || buffer.isEmpty()) {
            return;
        }
        List<String> keys = new ArrayList<>(buffer.keySet());
        for (String k : keys) {
            flushGroup(k);
        }
    }

    private void flushGroup(String groupKey) throws SQLException {
        List<CdcEvent> events = buffer.remove(groupKey);
        if (events == null || events.isEmpty()) {
            return;
        }
        CdcEvent first = events.get(0);
        String db = first.db();
        String table = first.table();
        String op = first.getOp();
        ensureTargetTable(db, table);
        try (Connection conn = targetConnection(db)) {
            if ("d".equals(op)) {
                execDelete(conn, db, table, events);
            } else {
                execUpsert(conn, db, table, events);
            }
        }
        appliedCount += events.size();
        CdcLagRegistry.applied(instanceKey, appliedCount);
    }

    /** 写路径：INSERT ... ON DUPLICATE KEY UPDATE（快照 r / 插入 c / 更新 u 同路径，幂等） */
    private void execUpsert(Connection conn, String db, String table, List<CdcEvent> events) throws SQLException {
        Map<String, Object> sample = events.get(0).getAfter();
        if (sample == null || sample.isEmpty()) {
            return;
        }
        List<String> cols = new ArrayList<>(sample.keySet());
        String colList = cols.stream().map(c -> "`" + c + "`").reduce((a, b) -> a + "," + b).orElse("");
        String placeholders = cols.stream().map(c -> "?").reduce((a, b) -> a + "," + b).orElse("");
        String update = cols.stream().map(c -> "`" + c + "`=VALUES(`" + c + "`)").reduce((a, b) -> a + "," + b).orElse("");
        String sql = "INSERT INTO `" + db + "`.`" + table + "` (" + colList + ") VALUES (" + placeholders + ") "
                + "ON DUPLICATE KEY UPDATE " + update;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (CdcEvent e : events) {
                Map<String, Object> row = e.getAfter();
                int i = 1;
                for (String c : cols) {
                    setParam(ps, i++, row.get(c), db, table, c);
                }
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** 删路径：按删除前镜像全列 NULL 安全等值匹配（<=>），无需主键知识 */
    private void execDelete(Connection conn, String db, String table, List<CdcEvent> events) throws SQLException {
        Map<String, Object> sample = events.get(0).getBefore();
        if (sample == null || sample.isEmpty()) {
            return;
        }
        List<String> cols = new ArrayList<>(sample.keySet());
        StringBuilder where = new StringBuilder();
        for (int r = 0; r < events.size(); r++) {
            if (r > 0) {
                where.append(" OR ");
            }
            where.append("(");
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) {
                    where.append(" AND ");
                }
                where.append("`").append(cols.get(i)).append("`<=>?");
            }
            where.append(")");
        }
        String sql = "DELETE FROM `" + db + "`.`" + table + "` WHERE " + where;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (CdcEvent e : events) {
                for (String c : cols) {
                    setParam(ps, idx++, e.getBefore().get(c), db, table, c);
                }
            }
            ps.executeUpdate();
        }
    }

    /**
     * 参数绑定：Debezium JSON 中时序类型是数值（epoch 相关），需按列类型转回字面量；
     * 其余类型原样绑定（BigDecimal/Long/String/byte[] 均可被 MySQL 驱动直接处理）。
     */
    private void setParam(PreparedStatement ps, int idx, Object value, String db, String table, String column)
            throws SQLException {
        if (value == null) {
            ps.setObject(idx, null);
            return;
        }
        String type = columnType(db, table, column);
        if (type != null && value instanceof Number) {
            switch (type) {
                case "datetime":
                    ps.setString(idx, LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) value).longValue()), serverZone)
                            .format(DATETIME_FMT));
                    return;
                case "timestamp":
                    ps.setString(idx, LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) value).longValue()), serverZone)
                            .format(DATETIME_FMT));
                    return;
                case "date":
                    ps.setString(idx, LocalDate.ofEpochDay(((Number) value).longValue()).format(DATE_FMT));
                    return;
                case "time":
                    ps.setString(idx, LocalTime.ofSecondOfDay(((Number) value).longValue() / 1000).format(TIME_FMT));
                    return;
                default:
                    // bigint/int/decimal/float/double/year/bit 等：数值原样绑定
            }
        }
        ps.setObject(idx, value);
    }

    // ===================== 目标表确保 + 类型缓存 =====================

    /** 目标库缺失时自动创建（动态新增库场景：源库新建库后，目标端尚无该库） */
    private void ensureTargetDatabase(String db) throws SQLException {
        if (db == null || db.isEmpty() || ensuredDatabases.contains(db)) {
            return;
        }
        synchronized (ensuredDatabases) {
            if (ensuredDatabases.contains(db)) {
                return;
            }
            try (Connection conn = targetConnection(null); Statement st = conn.createStatement()) {
                st.execute("CREATE DATABASE IF NOT EXISTS `" + db + "` "
                        + "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci");
                LOG.info("CDC ensured target database exists: {}", db);
            }
            ensuredDatabases.add(db);
        }
    }

    private void ensureTargetTable(String db, String table) throws SQLException {
        String key = db + "." + table;
        if (ensuredTables.contains(key)) {
            return;
        }
        synchronized (ensuredTables) {
            if (ensuredTables.contains(key)) {
                return;
            }
            ensureTargetDatabase(db);
            boolean exists;
            try (Connection conn = targetConnection(db);
                 PreparedStatement ps = conn.prepareStatement(
                         "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?")) {
                ps.setString(1, db);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    exists = rs.getInt(1) > 0;
                }
            }
            if (!exists) {
                createTargetTableFromSource(db, table);
            }
            ensuredTables.add(key);
        }
    }

    /** 从源库读取建表语句，清洗后在目标库执行（一次性，仅目标表缺失时触发） */
    private void createTargetTableFromSource(String db, String table) throws SQLException {
        String ddl;
        try (Connection src = sourceConnection();
             Statement st = src.createStatement();
             ResultSet rs = st.executeQuery("SHOW CREATE TABLE `" + db + "`.`" + table + "`")) {
            if (!rs.next()) {
                throw new SQLException("Source table not found: " + db + "." + table);
            }
            ddl = rs.getString(2);
        }
        String cleaned = new DdlApplier().sanitize(ddl);
        if (cleaned == null) {
            throw new SQLException("Sanitized DDL is empty for " + db + "." + table);
        }
        try (Connection conn = targetConnection(db); Statement st = conn.createStatement()) {
            st.execute(cleaned);
        }
    }

    /** 查询（并缓存）源库列类型：每库一次 information_schema.COLUMNS */
    private String columnType(String db, String table, String column) {
        TableTypes tt = typeCaches.get(db);
        if (tt == null) {
            tt = new TableTypes();
            typeCaches.put(db, tt);
        }
        if (!tt.knows(table)) {
            loadTableTypes(db, tt);
        }
        return tt.dataType(table, column);
    }

    private void loadTableTypes(String db, TableTypes tt) {
        try (Connection src = sourceConnection();
             PreparedStatement ps = src.prepareStatement(
                     "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=?")) {
            ps.setString(1, db);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tt.types.computeIfAbsent(rs.getString(1), k -> new ConcurrentHashMap<>())
                            .put(rs.getString(2), rs.getString(3));
                }
            }
        } catch (SQLException e) {
            // 类型加载失败不阻断同步：退化为数值直绑（时序列可能按大整数落库，由对账发现）
        }
    }

    // ===================== 连接管理 =====================

    private Connection sourceConnection() throws SQLException {
        if (sourceConn == null || sourceConn.isClosed()) {
            sourceConn = DriverManager.getConnection(jdbcUrl(sourceConfig, null), sourceConfig.getUsername(),
                    sourceConfig.getPassword());
        }
        return sourceConn;
    }

    private Connection targetConnection(String db) throws SQLException {
        DataSource ds = POOLS.computeIfAbsent(poolKey(targetConfig, db), k -> createPool(targetConfig, db));
        return ds.getConnection();
    }

    private static String poolKey(DatabaseConfig cfg, String db) {
        return cfg.getHost() + ":" + cfg.getPort() + "/" + db;
    }

    private DataSource createPool(DatabaseConfig cfg, String db) {
        HikariConfig hc = new HikariConfig();
        // 优先用连接配置里指定的库；否则用事件携带的目标库（查询均用 db.table 全限定，库名仅作连接默认库）
        String connectDb = cfg.getDatabaseName() != null ? cfg.getDatabaseName() : db;
        hc.setJdbcUrl(jdbcUrl(cfg, connectDb));
        hc.setUsername(cfg.getUsername());
        hc.setPassword(cfg.getPassword());
        hc.setMaximumPoolSize(Math.max(1, poolSize));
        hc.setMinimumIdle(0);
        hc.setIdleTimeout(60_000);
        hc.setMaxLifetime(600_000);
        hc.setConnectionTimeout(10_000);
        hc.setPoolName("cdc-target-" + poolKey(cfg, db));
        return new HikariDataSource(hc);
    }

    /** 目标/源连接 URL：db 放在路径段，参数在 ? 之后（避免库名被拼进 query string 损坏 URL） */
    private String jdbcUrl(DatabaseConfig cfg, String db) {
        String params = "useUnicode=true&characterEncoding=utf8&serverTimezone=" + serverZone.getId()
                + "&allowPublicKeyRetrieval=true&useSSL=false&rewriteBatchedStatements=true";
        return String.format("jdbc:mysql://%s:%d/%s?%s",
                cfg.getHost(), cfg.getPort(), db == null ? "" : db, params);
    }

    // ===================== checkpoint 对齐 =====================

    @Override
    public void snapshotState(org.apache.flink.runtime.state.FunctionSnapshotContext context) throws Exception {
        // 屏障到达 = 上游事件已全部处理完毕：同步落盘后屏障才通过 → 位点不越过未落库数据
        flushAll();
    }

    @Override
    public void initializeState(org.apache.flink.runtime.state.FunctionInitializationContext context) throws Exception {
        // 无作业级状态（位点由 source 算子持有）；恢复时确保缓冲为空
        if (buffer == null) {
            buffer = new LinkedHashMap<>();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            flushAll();
        } catch (Exception e) {
            // 关闭期 flush 失败不抛出让作业正常结束
        }
        if (sourceConn != null) {
            try {
                sourceConn.close();
            } catch (SQLException ignore) {
            }
        }
        super.close();
    }

    /** 供上层在作业停止时统一关闭目标连接池 */
    public static void shutdownPools() {
        for (DataSource ds : POOLS.values()) {
            if (ds instanceof HikariDataSource) {
                ((HikariDataSource) ds).close();
            }
        }
        POOLS.clear();
    }
}
