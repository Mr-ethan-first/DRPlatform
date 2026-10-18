package com.example.remotedatasync.cdc;

import com.example.remotedatasync.bean.DatabaseMapping;
import com.example.remotedatasync.config.DRPlatformProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Flink CDC 作业管理器（Spring 单例）：
 * <ul>
 *   <li>按主机对维护 {@link FlinkCdcSyncJob}；</li>
 *   <li>作业状态（jobId / savepoint 路径）持久化到元库 {@code cdc_job_state}；</li>
 *   <li>启动时按「显式 savepoint → retained checkpoint → INITIAL」优先级恢复；</li>
 *   <li>优雅停机（@PreDestroy）对所有作业 stopWithSavepoint；</li>
 *   <li>主备切换：旧作业停并归档 savepoint，新源 INITIAL 全量重扫。</li>
 * </ul>
 *
 * @author 50707
 */
@Component
public class FlinkCdcJobManager {

    private static final Logger log = LoggerFactory.getLogger(FlinkCdcJobManager.class);

    private final DRPlatformProperties properties;

    private final JdbcTemplate jdbcTemplate;

    private final Map<String, FlinkCdcSyncJob> jobs = new ConcurrentHashMap<>();

    /** 巡检线程：周期发现源库新增库/表 + 作业失败自愈 */
    private java.util.concurrent.ScheduledExecutorService watchdog;

    /** 上次巡检到的源库表集合（instanceKey -> "db.table" 集合） */
    private final Map<String, java.util.Set<String>> capturedTables = new ConcurrentHashMap<>();

    /** 上次巡检触发重启的时间戳（毫秒），用于冷却，避免故障期反复重启 */
    private volatile long lastRestartMs = 0;

    /** 因「目标端缺表」触发全量重扫的次数（instanceKey -> 次数），防止重启风暴 */
    private final Map<String, Integer> missingTries = new ConcurrentHashMap<>();
    /** 源端疑似删除的表（db.table）：连续两个巡检周期都缺失才执行目标端 DROP，防瞬时误报 */
    private final Map<String, java.util.Set<String>> pendingRemoved = new ConcurrentHashMap<>();

    public FlinkCdcJobManager(DRPlatformProperties properties, JdbcTemplate jdbcTemplate) {
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void init() {
        CdcLagRegistry.configure(properties.getCdc().getLagWarnSeconds());
        ensureStateTable();
        startWatchdog();
    }

    // ===================== 巡检：动态新增库/表 + 失败自愈 =====================

    /** 拉起「映射存在但作业从未注册」的主机对（启动失败兜底），复用巡检冷却防风暴 */
    private void tryStartMissingJob(DatabaseMapping m, String key) {
        long cooldown = Math.max(0, properties.getCdc().getDiscoveryRestartCooldownSec()) * 1000L;
        if (cooldown > 0 && System.currentTimeMillis() - lastRestartMs < cooldown) {
            return;
        }
        lastRestartMs = System.currentTimeMillis();
        log.info("CDC watchdog found missing job for {} (previous start failed), trying startOrResume", key);
        try {
            startOrResume(m);
        } catch (Exception e) {
            log.warn("CDC watchdog startOrResume failed for {}: {}", key, e.getMessage());
        }
    }

    private void startWatchdog() {
        int interval = Math.max(0, properties.getCdc().getDiscoveryIntervalSec());
        if (interval == 0) {
            log.info("CDC discovery watchdog disabled (discovery-interval-sec=0)");
            return;
        }
        watchdog = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cdc-discovery-watchdog");
            t.setDaemon(true);
            return t;
        });
        watchdog.scheduleWithFixedDelay(this::watchOnce, interval, interval, java.util.concurrent.TimeUnit.SECONDS);
        log.info("CDC discovery watchdog started, interval={}s", interval);
    }

    private void watchOnce() {
        try {
            List<DatabaseMapping> mappings = properties.getMappings();
            if (mappings == null) {
                return;
            }
            for (DatabaseMapping m : mappings) {
                String key = m.getInstanceKey();
                FlinkCdcSyncJob job = jobs.get(key);
                if (job == null) {
                    // 0) 启动失败兜底：作业从未成功注册（如目标库故障期间应用启动、
                    //    executeAsync 前后即抛错）时 jobs 里没有它，若不在此拉起，
                    //    该映射将永久停同步（数据丢失 0 容忍不允许）。带冷却防风暴。
                    tryStartMissingJob(m, key);
                    continue;
                }
                // 1) 失败自愈：Flink 固定次数重启策略耗尽后作业会停在 FAILED，需本层拉起
                String state = safeStatus(job);
                if ("FAILED".equals(state) || "CANCELED".equals(state) || "FINISHED".equals(state)) {
                    // 终态失败一律「全新 INITIAL 重扫」而非从损坏的 checkpoint/savepoint 恢复：
                    // flink-cdc 在 binlog 流式阶段处理「删表/DROP DATABASE」这类移除已捕获表的 DDL 时，
                    // 其 MySqlSourceReader.fillMetadataForBinlogSplit 会因 split 元数据数组与表清单不一致
                    // 抛 IllegalArgumentException (fromIndex>toIndex) 而崩溃；从含该损坏 split 的
                    // checkpoint 恢复会反复崩溃直至彻底停同步。INITIAL 从「当前」库表快照重建 split，
                    // 起始 binlog 位点已越过触发崩溃的那条 DROP，从而彻底绕过该损坏状态。
                    restartJob(m, key, "job state=" + state, true);
                    continue;
                }
                // 2) 动态新增库/表：flink-cdc 运行期不探测，需本层巡检发现后重启纳入
                java.util.Set<String> current = discoverSourceTables(m);
                if (current == null || current.isEmpty()) {
                    continue;
                }
                java.util.Set<String> prev = capturedTables.get(key);
                if (prev == null) {
                    capturedTables.put(key, current);
                    continue;
                }
                java.util.Set<String> added = new java.util.HashSet<>(current);
                added.removeAll(prev);
                if (!added.isEmpty()) {
                    // 基线只在真正重启成功后才更新：否则冷却期被跳过的发现会永远丢失
                    missingTries.remove(key);
                    // 新表触发 forceInitial 重扫前，先冲刷「已确认缺失」的删除对账：
                    // 重扫只重建源端现存的表，若不清理，目标端将永久残留源端已删除的库/表
                    java.util.Set<String> pending = pendingRemoved.remove(key);
                    if (pending != null && !pending.isEmpty()) {
                        dropRemovedOnTarget(m, key, pending, prev);
                    }
                    // forceInitial=true：必须全量重扫，不能用 savepoint 恢复！
                    // flink-cdc 3.7 从 savepoint 恢复的 binlog split 只带旧表 schema，
                    // 且 reader 对二次赋 split 会跳过新表 schema 发现（discoverTableSchemasForBinlogSplit
                    // 的 SKIP 分支）→ 新表快照数据能到，但其后所有 binlog 事件（UPDATE/DELETE/DROP）
                    // 因缺 schema 被静默丢弃（作业无任何报错，实测 TGT 残留已删除的库）。
                    // INITIAL 重扫 = 新表连同全库快照 + 幂等 upsert，不重不丢。
                    restartJob(m, key, "newly added tables: " + added, true, current);
                    continue;
                }
                java.util.Set<String> removed = new java.util.HashSet<>(prev);
                removed.removeAll(current);
                if (removed.isEmpty()) {
                    pendingRemoved.remove(key);
                    if (!prev.equals(current)) {
                        capturedTables.put(key, current);
                    }
                } else {
                    // 2.5) 源端已删除的库/表 → 双周期确认后在目标端同步删除。
                    // flink-cdc 3.7 不把 DROP DATABASE/TABLE 作为 schema-change 事件向下转发
                    // （Debezium 收到即吞，实测日志：Received MySQL DDL event ... ddl=DROP DATABASE），
                    // 目标端会永久残留已删除的数据 → 与源端发散，0 容忍语义下必须对账删除。
                    // 安全护栏：① 只删本映射此前纳管（基线）且源端连续两个周期消失的表；
                    // ② 确认前不收缩基线（否则下一周期 removed 恒为空，确认永远不成立）；
                    // ③ 尊重 ignore 过滤；④ 整库删除时连空壳库一起清；⑤ 绝不触碰无关库表。
                    java.util.Set<String> lastSeen = pendingRemoved.get(key);
                    if (lastSeen == null) {
                        pendingRemoved.put(key, removed);
                    } else {
                        java.util.Set<String> confirmed = new java.util.HashSet<>(lastSeen);
                        confirmed.retainAll(removed);
                        if (!confirmed.isEmpty()) {
                            dropRemovedOnTarget(m, key, confirmed, prev);
                        }
                        pendingRemoved.put(key, new java.util.HashSet<>(removed));
                        capturedTables.put(key, current);
                    }
                    continue;
                }
                // 3) 源端有、目标端没有：
                //    - 空表：CDC 快照不会产生行事件，目标端永远不会建表 → 直接复制建表语句补齐；
                //    - 非空表：多因「首轮全量快照中途崩溃」，作业状态里没有这些表 → 全量重扫补齐。
                java.util.Set<String> onTarget = discoverTargetTables(m);
                if (onTarget == null) {
                    continue;
                }
                java.util.Set<String> missing = new java.util.HashSet<>(current);
                missing.removeAll(onTarget);
                if (missing.isEmpty()) {
                    missingTries.remove(key);
                    continue;
                }
                for (String t : missing) {
                    int dot = t.indexOf('.');
                    tryCreateEmptyTable(m, t.substring(0, dot), t.substring(dot + 1));
                }
                java.util.Set<String> nonEmptyMissing = new java.util.HashSet<>(missing);
                nonEmptyMissing.retainAll(nonEmptySourceTables(m));
                if (nonEmptyMissing.isEmpty()) {
                    continue;
                }
                int tries = missingTries.getOrDefault(key, 0);
                if (tries >= 3) {
                    log.warn("CDC target still missing {} for {}, skip further full-resync (already tried {} times)",
                            nonEmptyMissing, key, tries);
                    continue;
                }
                missingTries.put(key, tries + 1);
                restartJob(m, key, "target missing tables: " + nonEmptyMissing, true);
            }
        } catch (Throwable t) {
            log.warn("CDC watchdog iteration failed: {}", t.getMessage());
        }
    }

    private String safeStatus(FlinkCdcSyncJob job) {
        try {
            return job.status();
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }

    /** 扫描源主机当前用户库/表（排除系统库与 ignore 规则） */
    private java.util.Set<String> discoverSourceTables(DatabaseMapping mapping) {
        return discoverTables(mapping.getSourceHost(), mapping.getSourcePort(),
                mapping.getSourceUser(), mapping.getSourcePassword(), mapping);
    }

    /** 扫描目标主机已存在的用户库/表 */
    private java.util.Set<String> discoverTargetTables(DatabaseMapping mapping) {
        return discoverTables(mapping.getTargetHost(), mapping.getTargetPort(),
                mapping.getTargetUser(), mapping.getTargetPassword(), null);
    }

    private java.util.Set<String> discoverTables(String host, int port, String user, String password,
                                                 DatabaseMapping filterMapping) {
        String url = String.format(
                "jdbc:mysql://%s:%d/information_schema?useUnicode=true&characterEncoding=utf8"
                        + "&serverTimezone=UTC&allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=5000&socketTimeout=60000",
                host, port);
        java.util.Set<String> out = new java.util.HashSet<>();
        try (java.sql.Connection c = java.sql.DriverManager.getConnection(url, user, password);
             java.sql.PreparedStatement ps = c.prepareStatement(
                     "SELECT table_schema, table_name FROM information_schema.tables "
                             + "WHERE table_type='BASE TABLE' AND table_schema NOT IN "
                             + "('information_schema','mysql','performance_schema','sys')");
             java.sql.ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String db = rs.getString(1);
                if (filterMapping != null) {
                    if (ignored(filterMapping.getIgnoreDatabases(), db)) {
                        continue;
                    }
                    if (ignored(filterMapping.getIgnoreTables(), db + "." + rs.getString(2))
                            || ignored(filterMapping.getIgnoreTables(), rs.getString(2))) {
                        continue;
                    }
                }
                out.add(db + "." + rs.getString(2));
            }
            return out;
        } catch (Exception e) {
            log.debug("Discover tables failed on {}:{}: {}", host, port, e.getMessage());
            return null;
        }
    }

    /** 源端非空表集合（information_schema 估算值，仅用于判断「是否需要重扫补数据」） */
    private java.util.Set<String> nonEmptySourceTables(DatabaseMapping mapping) {
        String url = String.format(
                "jdbc:mysql://%s:%d/information_schema?useUnicode=true&characterEncoding=utf8"
                        + "&serverTimezone=UTC&allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=5000&socketTimeout=60000",
                mapping.getSourceHost(), mapping.getSourcePort());
        java.util.Set<String> out = new java.util.HashSet<>();
        try (java.sql.Connection c = java.sql.DriverManager.getConnection(
                url, mapping.getSourceUser(), mapping.getSourcePassword());
             java.sql.Statement st = c.createStatement();
             java.sql.ResultSet rs = st.executeQuery(
                     "SELECT table_schema, table_name FROM information_schema.tables "
                             + "WHERE table_type='BASE TABLE' AND table_schema NOT IN "
                             + "('information_schema','mysql','performance_schema','sys') "
                             + "AND (table_rows IS NULL OR table_rows > 0)")) {
            while (rs.next()) {
                out.add(rs.getString(1) + "." + rs.getString(2));
            }
            return out;
        } catch (Exception e) {
            log.debug("Discover non-empty source tables failed for {}: {}",
                    mapping.getInstanceKey(), e.getMessage());
            return java.util.Collections.emptySet();
        }
    }

    /**
     * 目标端缺表且源端为空表时直接按源表结构在目标端建表（CDC 不会产生行事件，
     * 若不补建，灾备库将缺少该表结构）。已有数据则不动，交给正常同步流程。
     */
    private void tryCreateEmptyTable(DatabaseMapping mapping, String db, String table) {
        String srcUrl = String.format(
                "jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"
                        + "&allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=5000&socketTimeout=60000",
                mapping.getSourceHost(), mapping.getSourcePort(), db);
        String tgtUrl = String.format(
                "jdbc:mysql://%s:%d?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"
                        + "&allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=5000&socketTimeout=60000",
                mapping.getTargetHost(), mapping.getTargetPort());
        String ddl = null;
        try (java.sql.Connection c = java.sql.DriverManager.getConnection(
                srcUrl, mapping.getSourceUser(), mapping.getSourcePassword());
             java.sql.Statement st = c.createStatement();
             java.sql.ResultSet rs = st.executeQuery("SHOW CREATE TABLE `" + table + "`")) {
            if (rs.next()) {
                ddl = rs.getString(2);
            }
        } catch (Exception e) {
            log.debug("Read source DDL failed for {}.{}: {}", db, table, e.getMessage());
            return;
        }
        if (ddl == null) {
            return;
        }
        String sanitized = new DdlApplier().sanitize(ddl);
        if (sanitized == null) {
            return;
        }
        try (java.sql.Connection c = java.sql.DriverManager.getConnection(
                tgtUrl, mapping.getTargetUser(), mapping.getTargetPassword());
             java.sql.Statement st = c.createStatement()) {
            st.execute("CREATE DATABASE IF NOT EXISTS `" + db + "` "
                    + "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci");
            st.execute("USE `" + db + "`");
            st.execute(sanitized);
            log.info("CDC created missing empty table on target: {}.{}", db, table);
        } catch (Exception e) {
            log.warn("Create missing table {}.{} on target failed: {}", db, table, e.getMessage());
        }
    }

    /**
     * 把源端已删除（且双周期确认）的库/表从目标端同步删除。
     * <p>只处理本映射此前纳管过的 {@code db.table}（来自基线 prev），绝不触碰
     * 目标端上与本映射无关的库/表；遵守 ignore 过滤规则；整库删除时连空壳库一起清。
     */
    private void dropRemovedOnTarget(DatabaseMapping m, String key,
                                     java.util.Set<String> confirmedRemoved,
                                     java.util.Set<String> prevBaseline) {
        String url = String.format(
                "jdbc:mysql://%s:%d?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"
                        + "&allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=5000&socketTimeout=60000",
                m.getTargetHost(), m.getTargetPort());
        // 按库聚合，判断「整库删除」：该库在基线里的表全部被删
        java.util.Map<String, java.util.Set<String>> byDb = new java.util.LinkedHashMap<>();
        for (String t : prevBaseline) {
            int dot = t.indexOf('.');
            if (dot > 0) {
                byDb.computeIfAbsent(t.substring(0, dot), k -> new java.util.HashSet<>()).add(t.substring(dot + 1));
            }
        }
        java.util.Set<String> handledDbs = new java.util.HashSet<>();
        try (java.sql.Connection c = java.sql.DriverManager.getConnection(
                url, m.getTargetUser(), m.getTargetPassword());
             java.sql.Statement st = c.createStatement()) {
            CdcFilter filter = new CdcFilter(m);
            for (String t : confirmedRemoved) {
                int dot = t.indexOf('.');
                if (dot <= 0) {
                    continue;
                }
                String db = t.substring(0, dot), table = t.substring(dot + 1);
                if (!filter.acceptDatabase(db) || !filter.acceptTable(db, table)) {
                    continue;
                }
                st.execute("DROP TABLE IF EXISTS `" + db + "`.`" + table + "`");
                log.info("CDC dropped removed table on target: {}.{} (source no longer has it)", db, table);
                handledDbs.add(db);
            }
            // 整库删除：基线里该库的表全部确认删除，且源端确无此库 → 删目标空壳库
            for (java.util.Map.Entry<String, java.util.Set<String>> e : byDb.entrySet()) {
                String db = e.getKey();
                if (!handledDbs.contains(db) || !e.getValue().stream().allMatch(
                        tb -> confirmedRemoved.contains(db + "." + tb))) {
                    continue;
                }
                if (!filter.acceptDatabase(db)) {
                    continue;
                }
                st.execute("DROP DATABASE IF EXISTS `" + db + "`");
                log.info("CDC dropped removed database on target: {} (source no longer has it)", db);
            }
            pendingRemoved.remove(key);
        } catch (Exception e) {
            log.warn("Drop removed tables on target failed for {}: {}", key, e.getMessage());
        }
    }

    private boolean ignored(List<String> patterns, String name) {
        if (patterns == null || name == null) {
            return false;
        }
        for (String p : patterns) {
            if (p == null || p.isEmpty()) {
                continue;
            }
            try {
                if (name.matches(FlinkCdcSyncJob.patternToRegex(p))) {
                    return true;
                }
            } catch (Exception ignore) {
                // 非法正则忽略
            }
        }
        return false;
    }

    /**
     * 停旧作业 → 重新拉起。
     *
     * @param forceInitial true 表示忽略 savepoint/checkpoint 做全量重扫（用于「源有目标无」的补齐场景：
     *                     作业状态里没有这些表，从位点恢复也不会再发现它们）
     */
    private synchronized void restartJob(DatabaseMapping mapping, String key, String reason,
                                         boolean forceInitial) {
        restartJob(mapping, key, reason, forceInitial, null);
    }

    private synchronized void restartJob(DatabaseMapping mapping, String key, String reason,
                                         boolean forceInitial, java.util.Set<String> newBaseline) {
        restartJob(mapping, key, reason, forceInitial, newBaseline, true);
    }

    /**
     * 配置热应用：对全部运行中作业做「savepoint 优雅停止 → 从 savepoint 恢复」，
     * 让新运行时参数（并行度/内存/池大小等）无损生效。显式绕过巡检冷却 ——
     * 这是用户主动操作，且 savepoint 前强制 flush、恢复不丢不重。
     *
     * @return instanceKey -> 结果（restarted / skipped(原因) / ERROR: 信息）
     */
    public synchronized Map<String, String> applyRuntimeConfig() {
        Map<String, String> results = new java.util.LinkedHashMap<>();
        List<DatabaseMapping> mappings = properties.getMappings();
        if (mappings == null) {
            return results;
        }
        for (DatabaseMapping m : mappings) {
            String key = m.getInstanceKey();
            if (!jobs.containsKey(key)) {
                continue;
            }
            try {
                restartJob(m, key, "runtime-config apply", false, null, false);
                results.put(key, "restarted");
            } catch (Exception e) {
                log.error("Runtime-config apply failed for {}: {}", key, e.getMessage());
                results.put(key, "ERROR: " + e.getMessage());
            }
        }
        return results;
    }

    private synchronized void restartJob(DatabaseMapping mapping, String key, String reason,
                                         boolean forceInitial, java.util.Set<String> newBaseline,
                                         boolean respectCooldown) {
        long cooldown = Math.max(0, properties.getCdc().getDiscoveryRestartCooldownSec()) * 1000L;
        if (respectCooldown && cooldown > 0 && System.currentTimeMillis() - lastRestartMs < cooldown) {
            log.info("CDC restart skipped for {} (cooldown), reason={}", key, reason);
            return;
        }
        if (newBaseline != null) {
            capturedTables.put(key, newBaseline);
        }
        lastRestartMs = System.currentTimeMillis();
        capturedTables.remove(key);
        log.info("CDC job restart triggered for {}: {}", key, reason);
        FlinkCdcSyncJob old = jobs.remove(key);
        if (old != null) {
            boolean stopClean = false;
            try {
                String sp = old.stopWithSavepoint();
                // 必须把 savepoint 路径写回元库：否则重启只能回退到可能已被清理的 retained checkpoint
                if (sp != null) {
                    persistState(key, null, sp, "STOPPED", null);
                    stopClean = true;
                }
            } catch (Exception e) {
                log.warn("Stop-with-savepoint failed during restart for {}: {}", key, e.getMessage());
            }
            if (!stopClean) {
                // savepoint 未成功 = 旧作业状态不可信（可能正处在崩溃循环中，其 checkpoint
                // 含不一致的 binlog split 元数据）。从坏 checkpoint 恢复的作业「不崩溃但
                // 静默丢数据事件」（实测 flink-cdc fillMetadataForBinlogSplit 越界 bug 的
                // 衍生形态），比崩溃更危险 → 强制全新 INITIAL 重扫（幂等 upsert 保证不重不丢）。
                forceInitial = true;
            }
            // 宽限等待：savepoint future 完成后 debezium binlog 连接的释放仍有数秒滞后，
            // 立即启动新作业会与残留连接抢 server-id/复制位点，触发崩溃-重启循环
            try {
                Thread.sleep(3000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            if (forceInitial) {
                startInitial(mapping);
            } else {
                startOrResume(mapping);
            }
        } catch (Exception e) {
            log.error("CDC job restart failed for {}: {}", key, e.getMessage());
        }
    }

    /** 元库状态表（缺失则建） */
    private void ensureStateTable() {
        try {
            jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS `cdc_job_state` ("
                    + "`instance_key` varchar(128) NOT NULL,"
                    + "`job_id` varchar(64) DEFAULT NULL,"
                    + "`savepoint_path` varchar(512) DEFAULT NULL,"
                    + "`state` varchar(32) DEFAULT NULL,"
                    + "`last_error` varchar(1024) DEFAULT NULL,"
                    + "`updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "PRIMARY KEY (`instance_key`)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        } catch (Exception e) {
            log.warn("Failed to ensure cdc_job_state table: {}", e.getMessage());
        }
    }

    // ===================== 作业生命周期 =====================

    /** 启动（或恢复）指定主机对的作业 */
    public synchronized void startOrResume(DatabaseMapping mapping) {
        startInternal(mapping, false);
    }

    /**
     * 强制全新 INITIAL 启动（忽略已有 savepoint/checkpoint）。
     * 用于配置热更新（ignore 规则序列化在 savepoint 的 sink 中，恢复会沿用旧配置）
     * 与主备切换（换源后旧位点不可用）。
     */
    public synchronized void startInitial(DatabaseMapping mapping) {
        startInternal(mapping, true);
    }

    private void startInternal(DatabaseMapping mapping, boolean forceInitial) {
        String key = mapping.getInstanceKey();
        if (jobs.containsKey(key)) {
            log.info("CDC job already exists for {}, skip", key);
            return;
        }
        FlinkCdcSyncJob job = new FlinkCdcSyncJob(key, mapping, properties.getCdc());
        String recorded = querySavepoint(key);
        String restorePath = forceInitial ? null : job.resolveRestorePath(recorded);
        try {
            job.start(restorePath);
            jobs.put(key, job);
            // 基线必须在作业启动这一刻取：否则「作业启动前后新建的表」会被误记为基线而永不发现
            java.util.Set<String> base = discoverSourceTables(mapping);
            if (base != null) {
                capturedTables.put(key, base);
            }
            persistState(key, job.getJobId(), job.getLastSavepointPath(), "RUNNING", null);
            log.info("CDC job started for {} (restore={}, baselineTables={})", key, restorePath,
                    base == null ? -1 : base.size());
        } catch (Exception e) {
            // 恢复路径失效（checkpoint 目录已被清理/损坏）时退化为全新 INITIAL，
            // 保证作业一定能起来（代价是一次全量重扫，优于彻底停同步）
            if (restorePath != null) {
                log.warn("CDC job start from restore path failed for {} ({}), retry INITIAL",
                        key, e.getMessage());
                try {
                    job.start(null);
                    jobs.put(key, job);
                    persistState(key, job.getJobId(), job.getLastSavepointPath(), "RUNNING", null);
                    log.info("CDC job started for {} with INITIAL fallback", key);
                    return;
                } catch (Exception e2) {
                    log.error("CDC job INITIAL fallback failed for {}: {}", key, e2.getMessage(), e2);
                    persistState(key, null, recorded, "FAILED", abbreviate(e2.getMessage()));
                    throw new RuntimeException("CDC job start failed for " + key + ": " + e2.getMessage(), e2);
                }
            }
            log.error("Failed to start CDC job for {}: {}", key, e.getMessage(), e);
            persistState(key, null, recorded, "FAILED", abbreviate(e.getMessage()));
            throw new RuntimeException("CDC job start failed for " + key + ": " + e.getMessage(), e);
        }
    }

    /** 停止作业并打 savepoint（优雅停机 / 移除映射） */
    public synchronized String stopWithSavepoint(String instanceKey) {
        FlinkCdcSyncJob job = jobs.remove(instanceKey);
        if (job == null) {
            return null;
        }
        try {
            String path = job.stopWithSavepoint();
            persistState(instanceKey, null, path, "STOPPED", null);
            return path;
        } catch (Exception e) {
            log.error("Failed to stop CDC job with savepoint for {}: {}", instanceKey, e.getMessage());
            job.cancel();
            persistState(instanceKey, null, null, "STOPPED", abbreviate(e.getMessage()));
            return null;
        }
    }

    /** 不停机触发 savepoint（API） */
    public String triggerSavepoint(String instanceKey) {
        FlinkCdcSyncJob job = jobs.get(instanceKey);
        if (job == null) {
            throw new IllegalStateException("CDC job not running: " + instanceKey);
        }
        try {
            String path = job.triggerSavepoint();
            persistState(instanceKey, job.getJobId(), path, "RUNNING", null);
            return path;
        } catch (Exception e) {
            throw new RuntimeException("Trigger savepoint failed for " + instanceKey + ": " + e.getMessage(), e);
        }
    }

    /**
     * 源端变更（主备切换 / 生产-灾备角色互换）后重建作业：
     * 旧作业停并归档 savepoint，新源以 <b>INITIAL</b> 全量重扫。
     * <p>
     * <b>绝不可用 {@link #startOrResume}</b>：它会从旧源的 savepoint 恢复 binlog 位点，
     * 而该位点属于旧实例（不同 server-id / 不同 binlog 文件），装到新源上会导致
     * 「从错位位点续读」——轻则漏数据，重则作业直接 FAILED。
     * 换源后旧位点一律作废，必须整库重新快照 + 重新拉 binlog。
     * </p>
     */
    public synchronized void onSourceSwitched(String oldInstanceKey, DatabaseMapping newMapping) {
        capturedTables.remove(oldInstanceKey);   // 换源后表集合基线失效
        missingTries.remove(oldInstanceKey);
        CdcLagRegistry.remove(oldInstanceKey);
        stopWithSavepoint(oldInstanceKey);
        startInitial(newMapping);
    }

    /** 移除映射：停作业（不保留 savepoint 供恢复，仅归档） */
    public synchronized void removeJob(String instanceKey) {
        stopWithSavepoint(instanceKey);
        CdcLagRegistry.remove(instanceKey);
    }

    @PreDestroy
    public void shutdown() {
        if (watchdog != null) {
            watchdog.shutdownNow();
        }
        for (String key : new java.util.ArrayList<>(jobs.keySet())) {
            try {
                stopWithSavepoint(key);
            } catch (Exception e) {
                log.warn("Graceful stop failed for {}: {}", key, e.getMessage());
            }
        }
        IdempotentMySqlSink.shutdownPools();
    }

    // ===================== 状态查询 =====================

    public Map<String, Object> status(String instanceKey) {
        FlinkCdcSyncJob job = jobs.get(instanceKey);
        // 注意：不能用 ConcurrentHashMap —— jobId / lastSavepoint 可能为 null，
        // CHM.put(k, null) 会直接抛 NPE，导致 /sync/cdc/jobs 500。
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("instanceKey", instanceKey);
        m.put("running", job != null);
        m.put("jobStatus", job == null ? "NOT_RUNNING" : job.status());
        m.put("jobId", job == null ? null : job.getJobId());
        m.put("lastSavepoint", job == null ? querySavepoint(instanceKey) : job.getLastSavepointPath());
        m.putAll(CdcLagRegistry.snapshot(instanceKey));
        return m;
    }

    public boolean isCdcMode() {
        return "flink-cdc".equalsIgnoreCase(properties.getSync().getMode());
    }

    public boolean hasJob(String instanceKey) {
        return jobs.containsKey(instanceKey);
    }

    /** 所有正在运行作业的实例 key（供状态 API 遍历） */
    public java.util.Set<String> runningInstanceKeys() {
        return new java.util.HashSet<>(jobs.keySet());
    }

    /** 批量状态：遍历给定映射，逐个返回 CDC 作业状态（未运行则标记 NOT_RUNNING） */
    public List<Map<String, Object>> allStatus(java.util.List<DatabaseMapping> mappings) {
        List<Map<String, Object>> out = new java.util.ArrayList<>();
        for (DatabaseMapping m : mappings) {
            String key = m.getInstanceKey();
            out.add(status(key));
        }
        return out;
    }

    // ===================== 元库持久化 =====================

    private String querySavepoint(String instanceKey) {
        try {
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                    "SELECT savepoint_path FROM cdc_job_state WHERE instance_key = ?", instanceKey);
            if (rows.isEmpty()) {
                return null;
            }
            Object v = rows.get(0).get("savepoint_path");
            return v == null ? null : v.toString();
        } catch (Exception e) {
            return null;
        }
    }

    private void persistState(String instanceKey, String jobId, String savepointPath, String state, String error) {
        try {
            jdbcTemplate.update(
                    "INSERT INTO cdc_job_state (instance_key, job_id, savepoint_path, state, last_error) "
                            + "VALUES (?, ?, ?, ?, ?) "
                            + "ON DUPLICATE KEY UPDATE job_id = VALUES(job_id), savepoint_path = VALUES(savepoint_path), "
                            + "state = VALUES(state), last_error = VALUES(last_error)",
                    instanceKey, jobId, savepointPath, state, error);
        } catch (Exception e) {
            log.warn("Failed to persist cdc_job_state for {}: {}", instanceKey, e.getMessage());
        }
    }

    private static String abbreviate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() > 1000 ? s.substring(0, 1000) : s;
    }
}
