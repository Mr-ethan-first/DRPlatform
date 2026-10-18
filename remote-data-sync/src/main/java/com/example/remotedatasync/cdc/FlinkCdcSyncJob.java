package com.example.remotedatasync.cdc;

import com.example.remotedatasync.bean.DatabaseConfig;
import com.example.remotedatasync.bean.DatabaseMapping;
import com.example.remotedatasync.config.DRPlatformProperties;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.time.Duration;
import java.util.Properties;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * 单个主机对的 Flink CDC 同步作业（binlog 全量 + 增量）。
 *
 * <p>生命周期：
 * <ul>
 *   <li>启动：有可用 savepoint → 从其恢复；否则有 retained checkpoint → 从其恢复；否则 INITIAL（快照+binlog）；</li>
 *   <li>优雅停机 / API 触发：{@link #stopWithSavepoint()}（停作业打点）/ {@link #triggerSavepoint()}（不停机打点）；</li>
 *   <li>崩溃（kill -9）：进程重启后由 {@link FlinkCdcJobManager} 从最近 checkpoint 目录恢复；</li>
 *   <li>主备切换：旧作业停并归档 savepoint，新源以 INITIAL 全量重扫（换源语义）。</li>
 * </ul>
 *
 * @author 50707
 */
public class FlinkCdcSyncJob {

    private static final Logger log = LoggerFactory.getLogger(FlinkCdcSyncJob.class);

    private final String instanceKey;

    private final DatabaseMapping mapping;

    private final DRPlatformProperties.Cdc cfg;

    private final DatabaseConfig sourceConfig;

    private final DatabaseConfig targetConfig;

    private final ZoneId serverZone;

    private final String checkpointDir;

    private final String savepointDir;

    private JobClient jobClient;

    private String jobId;

    private volatile String lastSavepointPath;

    public FlinkCdcSyncJob(String instanceKey, DatabaseMapping mapping, DRPlatformProperties.Cdc cfg) {
        this.instanceKey = instanceKey;
        this.mapping = mapping;
        this.cfg = cfg;
        this.sourceConfig = toConfig(mapping.getSourceHost(), mapping.getSourcePort(),
                mapping.getSourceUser(), mapping.getSourcePassword(), null);
        this.targetConfig = toConfig(mapping.getTargetHost(), mapping.getTargetPort(),
                mapping.getTargetUser(), mapping.getTargetPassword(), null);
        this.serverZone = ZoneId.of(cfg.getServerTimeZone());
        String dirName = safeDirName(instanceKey);
        this.checkpointDir = cfg.getSavepointPath() + File.separator + dirName + File.separator + "checkpoints";
        this.savepointDir = cfg.getSavepointPath() + File.separator + dirName + File.separator + "savepoints";
    }

    /**
     * instanceKey（形如 {@code hostA->hostB}）直接作目录名在 Windows 上非法
     * （{@code >} 是保留字符，且 {@link File#toURI()} 会把特殊字符编码成 %XX，
     * 导致 Flink 落盘目录与返回路径不一致）。统一替换为安全字符。
     */
    static String safeDirName(String instanceKey) {
        return instanceKey == null ? "default" : instanceKey.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static DatabaseConfig toConfig(String host, int port, String user, String password, String db) {
        DatabaseConfig c = new DatabaseConfig();
        c.setHost(host);
        c.setPort(port);
        c.setUsername(user);
        c.setPassword(password);
        c.setDatabaseName(db);
        return c;
    }

    public String getInstanceKey() {
        return instanceKey;
    }

    public String getJobId() {
        return jobId;
    }

    public String getLastSavepointPath() {
        return lastSavepointPath;
    }

    // ===================== 启动 / 恢复 =====================

    /**
     * 启动作业。
     *
     * @param restorePath savepoint/checkpoint 恢复路径（null = INITIAL 全量）
     */
    public synchronized void start(String restorePath) throws Exception {
        if (jobClient != null) {
            throw new IllegalStateException("Job already running for " + instanceKey);
        }
        Configuration conf = new Configuration();
        conf.setString("parallelism.default", String.valueOf(Math.max(1, cfg.getParallelism())));
        conf.setString("execution.checkpointing.dir", toUri(checkpointDir));
        conf.setString("state.checkpoints.dir", toUri(checkpointDir));
        conf.setString("state.savepoints.dir", toUri(savepointDir));
        conf.setString("state.backend", "hashmap");
        conf.setString("taskmanager.memory.process.size", cfg.getTaskmanagerMemory());
        // 进程内自动重启（从最近 checkpoint 恢复），减少 downtime；进程崩溃由外层重启 + savepoint 恢复接管
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        // 固定延迟重启：次数给足（目标库短暂不可达等故障可自愈），
        // 耗尽后由 FlinkCdcJobManager 的巡检线程兜底拉起作业
        conf.setString("restart-strategy.fixed-delay.attempts", "20");
        conf.setString("restart-strategy.fixed-delay.delay", "5s");
        if (restorePath != null && !restorePath.isEmpty()) {
            conf.setString("execution.savepoint.path", restorePath);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);

        // 用编程式 API 显式开启 checkpoint（Flink 2.x 的 execution.checkpointing.interval 是 Duration 类型，
        // 用裸字符串配置会被静默忽略导致不触发 checkpoint，而本 sink 的缓冲依赖 checkpoint 屏障对齐落库）。
        env.enableCheckpointing(cfg.getCheckpointIntervalMs());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 作业取消后仍保留外部化 checkpoint：动态新增库/表巡检会「打 savepoint 停作业 → 重启恢复」，
        // 若取消即清理 checkpoint，重启时回退到的 chk 目录已被删除会直接启动失败。
        // （Flink 2.x 的 CheckpointConfig 已无 setExternalizedCheckpointCleanup，用配置键设置）
        conf.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");

        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname(mapping.getSourceHost())
                .port(mapping.getSourcePort())
                .databaseList(databaseIncludePatterns().toArray(new String[0]))
                // flink-cdc 3.7 的 MySqlSourceConfig 对 tableList 做 checkNotNull，
                // 不设置会在构造时 NPE。匹配「任意库.任意表」，与 databaseList 的负向断言组合即可
                // 实现「全部用户库全表、排除 ignore-databases」的语义（debezium 两个过滤器为 AND）。
                .tableList(".*\\..*")
                .username(mapping.getSourceUser())
                .password(mapping.getSourcePassword())
                .serverId(String.valueOf(cfg.getServerIdBase() + Math.abs(instanceKey.hashCode() % 10000)))
                .serverTimeZone(cfg.getServerTimeZone())
                .splitSize(cfg.getSnapshotChunkSize())
                .fetchSize(cfg.getFetchSize())
                .connectionPoolSize(cfg.getSourceConnectionPoolSize())
                .connectTimeout(Duration.ofSeconds(30))
                .connectMaxRetries(3)
                .startupOptions(StartupOptions.initial())
                .heartbeatInterval(Duration.ofSeconds(Math.max(1, cfg.getHeartbeatIntervalSec())), false)
                .includeSchemaChanges(cfg.isIncludeSchemaChanges())
                .scanNewlyAddedTableEnabled(cfg.isScanNewlyAddedTable())
                // 让 Debezium 把 DECIMAL/NUMERIC 以字符串形式输出（默认 precise 模式会输出 base64 字节，
                // 导致目标库写入报 "Incorrect decimal value"）。字符串模式可被 JDBC 直接按 BigDecimal 绑定。
                .debeziumProperties(cdcDbzProperties())
                .deserializer(new JsonDebeziumDeserializationSchema(false))
                .build();

        DataStream<String> raw = env.fromSource(source,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "mysql-cdc-source")
                .uid("cdc-source-" + instanceKey);

        SingleOutputStreamOperator<CdcEvent> events = raw
                .map(new JsonToCdcEventFunction())
                .uid("cdc-parse-" + instanceKey)
                .name("parse-debezium-json");

        CdcFilter filter = new CdcFilter(mapping);
        events.filter(filter::accept)
                .uid("cdc-filter-" + instanceKey)
                .name("cdc-filter")
                .keyBy(CdcEvent::tableKey)
                .addSink(new IdempotentMySqlSink(sourceConfig, targetConfig, filter, instanceKey,
                        cfg.getSinkBatchSize(), cfg.getSinkPoolSize(), serverZone,
                        mapping.getTransformRules()))
                .uid("cdc-sink-" + instanceKey)
                .name("idempotent-mysql-sink");

        String jobName = "cdc-" + instanceKey;
        this.jobClient = env.executeAsync(jobName);
        this.jobId = jobClient.getJobID().toString();
        log.info("CDC job started: {} jobId={} restore={}", jobName, jobId, restorePath);
    }

    // ===================== savepoint =====================

    /** 停止作业并打 savepoint（返回 savepoint 路径） */
    public synchronized String stopWithSavepoint() throws Exception {
        if (jobClient == null) {
            return lastSavepointPath;
        }
        CompletableFuture<String> f = jobClient.stopWithSavepoint(false, toUri(savepointDir),
                SavepointFormatType.DEFAULT);
        String path = toLocalPath(f.get(120, TimeUnit.SECONDS));
        lastSavepointPath = path;
        jobClient = null;
        jobId = null;
        log.info("CDC job stopped with savepoint: {} -> {}", instanceKey, path);
        return path;
    }

    /** 不停机触发 savepoint（用于周期性备份位点） */
    public synchronized String triggerSavepoint() throws Exception {
        if (jobClient == null) {
            return lastSavepointPath;
        }
        CompletableFuture<String> f = jobClient.triggerSavepoint(toUri(savepointDir), SavepointFormatType.DEFAULT);
        String path = toLocalPath(f.get(120, TimeUnit.SECONDS));
        lastSavepointPath = path;
        log.info("CDC savepoint triggered: {} -> {}", instanceKey, path);
        return path;
    }

    public synchronized void cancel() {
        if (jobClient != null) {
            jobClient.cancel();
            jobClient = null;
            jobId = null;
        }
    }

    public boolean isRunning() {
        return jobClient != null;
    }

    /** 作业状态（RUNNING/FAILED/CANCELED/…） */
    public String status() {
        if (jobClient == null) {
            return "NOT_RUNNING";
        }
        try {
            return jobClient.getJobStatus().get(10, TimeUnit.SECONDS).name();
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }

    // ===================== 恢复路径发现 =====================

    /**
     * 找到最近一次可用的恢复路径：优先显式 savepoint，其次 retained checkpoint。
     *
     * @param recordedSavepoint 元库记录的 savepoint 路径（可为 null）
     */
    public String resolveRestorePath(String recordedSavepoint) {
        if (recordedSavepoint != null && !recordedSavepoint.isEmpty()
                && new File(recordedSavepoint).exists()
                && new File(recordedSavepoint, "_metadata").exists()) {
            return recordedSavepoint;
        }
        return findLatestCheckpoint();
    }

    /** retained checkpoint 目录布局与 savepoint 相同（含 _metadata），可直接作为恢复路径 */
    public String findLatestCheckpoint() {
        File base = new File(checkpointDir);
        File[] jobDirs = base.listFiles(File::isDirectory);
        if (jobDirs == null) {
            return null;
        }
        String best = null;
        long bestSeq = Long.MIN_VALUE;
        for (File jd : jobDirs) {
            File[] chks = jd.listFiles(f -> f.isDirectory() && f.getName().startsWith("chk-"));
            if (chks == null) {
                continue;
            }
            // 按 chk-<序号> 的数值排序（字符串排序会把 chk-9 排到 chk-45 之后，选到旧位点）
            Arrays.sort(chks, (a, b) -> Long.compare(seqOf(b), seqOf(a)));
            for (File chk : chks) {
                if (new File(chk, "_metadata").exists()) {
                    long seq = seqOf(chk);
                    if (seq > bestSeq) {
                        bestSeq = seq;
                        best = chk.getAbsolutePath();
                    }
                    break;
                }
            }
        }
        return best;
    }

    /** 解析 chk-<N> 目录名中的序号（解析失败返回 -1，排最后） */
    private static long seqOf(File chkDir) {
        try {
            return Long.parseLong(chkDir.getName().substring("chk-".length()));
        } catch (Exception e) {
            return -1L;
        }
    }

    public String getCheckpointDir() {
        return checkpointDir;
    }

    public String getSavepointDir() {
        return savepointDir;
    }

    // ===================== 过滤正则构造 =====================

    /**
     * 构造 flink-cdc 的 databaseList 包含正则：全部用户库减去 ignore-databases。
     * 形如 {@code ^(?!(?:day02|rfm_db)$).*$}（每条忽略规则转成正则后并入负向先行断言）。
     */
    List<String> databaseIncludePatterns() {
        List<String> ignores = mapping.getIgnoreDatabases();
        if (ignores == null || ignores.isEmpty()) {
            return List.of(".*");
        }
        List<String> parts = new ArrayList<>();
        for (String p : ignores) {
            if (p == null || p.isEmpty()) {
                continue;
            }
            parts.add(patternToRegex(p));
        }
        if (parts.isEmpty()) {
            return List.of(".*");
        }
        return List.of("^(?!(?:" + String.join("|", parts) + ")$).*$");
    }

    /** 与 MatchPattern 一致的三态规则 → 正则片段（精确/通配符/re: 正则） */
    static String patternToRegex(String pattern) {
        if (pattern.startsWith("re:")) {
            return pattern.substring(3);
        }
        if (pattern.indexOf('*') >= 0 || pattern.indexOf('?') >= 0) {
            return "^" + pattern.replace("\\", "\\\\").replace(".", "\\.")
                    .replace("*", ".*").replace("?", ".") + "$";
        }
        return Pattern.quote(pattern);
    }

    private static String toUri(String path) {
        return new File(path).toURI().toString();
    }

    /** Flink 返回的 savepoint/checkpoint 路径是 file:/ URI，统一转回本地文件路径 */
    private static String toLocalPath(String uriOrPath) {
        if (uriOrPath == null || uriOrPath.isEmpty()) {
            return uriOrPath;
        }
        try {
            java.net.URI uri = java.net.URI.create(uriOrPath);
            if ("file".equalsIgnoreCase(uri.getScheme())) {
                return new File(uri).getAbsolutePath();
            }
        } catch (Exception ignore) {
            // 非合法 URI，按原始路径处理
        }
        return uriOrPath;
    }

    /** 传给 Debezium 的连接器属性（与 MySqlSourceConfigFactory 内置默认值合并，不覆盖） */
    private static Properties cdcDbzProperties() {
        Properties p = new Properties();
        p.setProperty("decimal.handling.mode", "string");
        return p;
    }
}
