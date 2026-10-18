package com.example.remotedatasync.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 同步引擎运行时动态配置服务。
 * <p>
 * 目标：<b>服务器性能越好，同步越快</b> —— 并行度、TaskManager 内存、快照分片、
 * sink 批大小/连接池、源端连接池等核心吞吐参数全部可通过 API 动态调整，
 * 不同性能的服务器按其硬件选择不同档位（见 {@link #presets()}）。
 * </p>
 * <p>
 * <b>数据丢失 0 容忍的硬约束</b>（校验层强制，无法通过 API 绕过）：
 * <ul>
 *   <li>checkpoint 间隔下限 1000ms，且不允许停用 —— binlog 位点随 checkpoint 持久化，
 *       停用 = 崩溃后从全量快照重放，不可接受；</li>
 *   <li>一致性机制（barrier 前强制 flush + 幂等 upsert + keyBy 保序）与调参正交，
 *       任意档位下均为 at-least-once + 幂等 = 不丢不重；</li>
 *   <li>并行度仅影响 split/事件的处理并行数，不改变 checkpoint 语义。</li>
 * </ul>
 * </p>
 * <p>
 * 实现说明：Flink 作业的 env 配置在作业创建时读取，因此动态修改对
 * <b>新启动/重启的作业</b>生效；提供 {@code apply=true} 触发「savepoint 优雅停止 →
 * 从 savepoint 恢复」循环，让运行中作业无损切换到新参数（flush 先于 barrier，
 * savepoint 恢复不丢不重）。
 * </p>
 */
@Slf4j
@Component
public class RuntimeConfigService {

    /** 运行时覆盖项持久化文件（重启不丢，随工作目录） */
    static final java.nio.file.Path CONFIG_FILE =
            java.nio.file.Paths.get("geodrsync-runtime-config.json").toAbsolutePath();

    private static final Pattern TM_MEMORY = Pattern.compile("^(\\d+)[mM]$");
    private static final Pattern TIME_ZONE = Pattern.compile("^[A-Za-z_]+(/[A-Za-z0-9_+-]+)?$");

    /** 可通过 API 修改的参数集合（持久化快照中的其余键为只读展示项，加载时忽略） */
    private static final java.util.Set<String> MUTABLE_KEYS = java.util.Set.of(
            "parallelism", "taskmanagerMemory", "snapshotChunkSize", "fetchSize",
            "sinkBatchSize", "sinkPoolSize", "sourceConnectionPoolSize",
            "checkpointIntervalMs", "heartbeatIntervalSec", "serverTimeZone",
            "discoveryIntervalSec", "discoveryRestartCooldownSec", "lagWarnSeconds");

    private final DRPlatformProperties properties;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RuntimeConfigService(DRPlatformProperties properties) {
        this.properties = properties;
    }

    /** 启动时加载持久化的运行时覆盖项（晚于 yml 绑定，覆盖生效） */
    @PostConstruct
    public void loadOnStartup() {
        File f = CONFIG_FILE.toFile();
        if (!f.exists()) {
            log.info("Runtime config file not found, using yml defaults: {}", CONFIG_FILE);
            return;
        }
        try {
            String json = new String(Files.readAllBytes(CONFIG_FILE), StandardCharsets.UTF_8);
            Map<String, Object> overrides = objectMapper.readValue(json, Map.class);
            // 持久化文件是「生效值快照」，可能含只读展示键（如 savepointPath）或历史版本的
            // 未知键 —— 启动加载时宽容忽略，仅取可变键；API 修改路径（applyOverrides）仍严格校验
            overrides.keySet().removeIf(k -> !MUTABLE_KEYS.contains(k));
            if (!overrides.isEmpty()) {
                applyOverrides(overrides);
            }
            log.info("Runtime config loaded from {}: {}", CONFIG_FILE.getFileName(), overrides);
        } catch (Exception e) {
            // 配置文件损坏不阻断启动，回退 yml 默认值
            log.error("Failed to load runtime config {}: {}", CONFIG_FILE, e.getMessage());
        }
    }

    /** 当前生效的全部可调参数 */
    public Map<String, Object> snapshot() {
        DRPlatformProperties.Cdc c = properties.getCdc();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("parallelism", c.getParallelism());
        m.put("parallelismEffective", c.resolveParallelism());
        m.put("cpuCores", Runtime.getRuntime().availableProcessors());
        m.put("taskmanagerMemory", c.getTaskmanagerMemory());
        m.put("snapshotChunkSize", c.getSnapshotChunkSize());
        m.put("fetchSize", c.getFetchSize());
        m.put("sinkBatchSize", c.getSinkBatchSize());
        m.put("sinkPoolSize", c.getSinkPoolSize());
        m.put("sourceConnectionPoolSize", c.getSourceConnectionPoolSize());
        m.put("checkpointIntervalMs", c.getCheckpointIntervalMs());
        m.put("heartbeatIntervalSec", c.getHeartbeatIntervalSec());
        m.put("serverTimeZone", c.getServerTimeZone());
        m.put("discoveryIntervalSec", c.getDiscoveryIntervalSec());
        m.put("discoveryRestartCooldownSec", c.getDiscoveryRestartCooldownSec());
        m.put("lagWarnSeconds", c.getLagWarnSeconds());
        m.put("savepointPath", c.getSavepointPath());
        return m;
    }

    /** 各参数的合法区间（供前端/运维参考，也作为校验依据） */
    public Map<String, Object> bounds() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("parallelism", "0~64（0=按 CPU 内核自动适配，>0 为显式配置）");
        m.put("taskmanagerMemory", "512m~16384m（整数+m）");
        m.put("snapshotChunkSize", "1000~200000");
        m.put("fetchSize", "100~100000");
        m.put("sinkBatchSize", "1~20000");
        m.put("sinkPoolSize", "1~64（每目标库连接池上限）");
        m.put("sourceConnectionPoolSize", "1~64");
        m.put("checkpointIntervalMs", "1000~600000（下限 1s，数据丢失 0 容忍，不允许停用）");
        m.put("heartbeatIntervalSec", "1~120");
        m.put("serverTimeZone", "IANA 时区名，如 UTC / Asia/Shanghai");
        m.put("discoveryIntervalSec", "0~3600（0=关闭巡检）");
        m.put("discoveryRestartCooldownSec", "5~3600");
        m.put("lagWarnSeconds", "1~86400");
        return m;
    }

    /** 服务器档位预设：性能越好档位越高，同步越快（一键应用） */
    public Map<String, Map<String, Object>> presets() {
        Map<String, Map<String, Object>> p = new LinkedHashMap<>();
        p.put("small_2c4g", preset(1, "512m", 8096, 1024, 250, 2, 2, 5000));
        p.put("medium_4c8g", preset(2, "1024m", 8096, 2048, 500, 2, 4, 3000));
        p.put("large_8c16g", preset(4, "2048m", 16384, 4096, 1000, 4, 8, 2000));
        p.put("xlarge_16c32g", preset(8, "4096m", 32768, 8192, 2000, 8, 12, 1000));
        return p;
    }

    private Map<String, Object> preset(int parallelism, String tm, int chunk, int fetch,
                                       int batch, int sinkPool, int srcPool, long ckptMs) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("parallelism", parallelism);
        m.put("taskmanagerMemory", tm);
        m.put("snapshotChunkSize", chunk);
        m.put("fetchSize", fetch);
        m.put("sinkBatchSize", batch);
        m.put("sinkPoolSize", sinkPool);
        m.put("sourceConnectionPoolSize", srcPool);
        m.put("checkpointIntervalMs", ckptMs);
        return m;
    }

    /**
     * 校验并应用运行时覆盖项。
     *
     * @param changes 参数名 -> 新值（只传需要修改的字段）
     * @return 实际生效的字段列表
     * @throws IllegalArgumentException 任何字段非法（含未知字段名）时抛出，整体不生效
     */
    public List<String> applyOverrides(Map<String, Object> changes) {
        if (changes == null || changes.isEmpty()) {
            throw new IllegalArgumentException("变更内容为空");
        }
        // 先整体校验（通过后再写入），保证要么全生效要么全不动
        Map<String, Object> validated = new LinkedHashMap<>();
        DRPlatformProperties.Cdc c = properties.getCdc();

        for (Map.Entry<String, Object> e : changes.entrySet()) {
            String k = e.getKey();
            Object v = e.getValue();
            switch (k) {
                case "parallelism" -> validated.put(k, intInRange(k, v, 0, 64));
                case "taskmanagerMemory" -> validated.put(k, tmMemory(v));
                case "snapshotChunkSize" -> validated.put(k, intInRange(k, v, 1000, 200000));
                case "fetchSize" -> validated.put(k, intInRange(k, v, 100, 100000));
                case "sinkBatchSize" -> validated.put(k, intInRange(k, v, 1, 20000));
                case "sinkPoolSize" -> validated.put(k, intInRange(k, v, 1, 64));
                case "sourceConnectionPoolSize" -> validated.put(k, intInRange(k, v, 1, 64));
                case "checkpointIntervalMs" -> validated.put(k, intInRange(k, v, 1000, 600000));
                case "heartbeatIntervalSec" -> validated.put(k, intInRange(k, v, 1, 120));
                case "serverTimeZone" -> {
                    String tz = String.valueOf(v).trim();
                    if (!TIME_ZONE.matcher(tz).matches()) {
                        throw err(k, v, "IANA 时区名（如 UTC / Asia/Shanghai）");
                    }
                    validated.put(k, tz);
                }
                case "discoveryIntervalSec" -> validated.put(k, intInRange(k, v, 0, 3600));
                case "discoveryRestartCooldownSec" -> validated.put(k, intInRange(k, v, 5, 3600));
                case "lagWarnSeconds" -> validated.put(k, intInRange(k, v, 1, 86400));
                default -> throw new IllegalArgumentException(
                        "未知参数: " + k + "，可选参数见 GET /sync/runtime-config");
            }
        }
        // 校验全部通过，写入（Cdc bean 为可变单例，Flink 作业创建时读取）
        for (Map.Entry<String, Object> e : validated.entrySet()) {
            String k = e.getKey();
            Object v = e.getValue();
            switch (k) {
                case "parallelism" -> c.setParallelism((Integer) v);
                case "taskmanagerMemory" -> c.setTaskmanagerMemory((String) v);
                case "snapshotChunkSize" -> c.setSnapshotChunkSize((Integer) v);
                case "fetchSize" -> c.setFetchSize((Integer) v);
                case "sinkBatchSize" -> c.setSinkBatchSize((Integer) v);
                case "sinkPoolSize" -> c.setSinkPoolSize((Integer) v);
                case "sourceConnectionPoolSize" -> c.setSourceConnectionPoolSize((Integer) v);
                case "checkpointIntervalMs" -> c.setCheckpointIntervalMs((Integer) v);
                case "heartbeatIntervalSec" -> c.setHeartbeatIntervalSec((Integer) v);
                case "serverTimeZone" -> c.setServerTimeZone((String) v);
                case "discoveryIntervalSec" -> c.setDiscoveryIntervalSec((Integer) v);
                case "discoveryRestartCooldownSec" -> c.setDiscoveryRestartCooldownSec((Integer) v);
                case "lagWarnSeconds" -> c.setLagWarnSeconds((Integer) v);
                default -> { /* unreachable */ }
            }
        }
        persist(validated);
        return new ArrayList<>(validated.keySet());
    }

    /** 持久化当前生效值（存快照而非增量，保证幂等覆盖） */
    private synchronized void persist(Map<String, Object> validated) {
        try {
            Map<String, Object> full = snapshot();
            File f = CONFIG_FILE.toFile();
            if (f.getParentFile() != null) {
                //noinspection ResultOfMethodCallIgnored
                f.getParentFile().mkdirs();
            }
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(full);
            Files.write(CONFIG_FILE, json.getBytes(StandardCharsets.UTF_8));
            log.info("Runtime config updated {} -> {}", validated, CONFIG_FILE);
        } catch (Exception e) {
            log.error("Failed to persist runtime config: {}", e.getMessage());
        }
    }

    private int intInRange(String key, Object v, int min, int max) {
        int i;
        try {
            i = v instanceof Number n ? n.intValue() : Integer.parseInt(String.valueOf(v).trim());
        } catch (NumberFormatException nfe) {
            throw err(key, v, min + "~" + max + " 的整数");
        }
        if (i < min || i > max) {
            throw err(key, v, min + "~" + max + " 的整数");
        }
        return i;
    }

    private String tmMemory(Object v) {
        String s = String.valueOf(v).trim();
        java.util.regex.Matcher m = TM_MEMORY.matcher(s);
        if (!m.matches()) {
            throw err("taskmanagerMemory", v, "512m~16384m（如 1024m / 4096m）");
        }
        int mb = Integer.parseInt(m.group(1));
        if (mb < 512 || mb > 16384) {
            throw err("taskmanagerMemory", v, "512m~16384m（如 1024m / 4096m）");
        }
        return mb + "m";
    }

    private IllegalArgumentException err(String key, Object v, String expect) {
        return new IllegalArgumentException("参数 " + key + "=" + v + " 非法，要求: " + expect);
    }
}
