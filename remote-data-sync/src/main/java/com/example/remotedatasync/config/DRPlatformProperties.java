package com.example.remotedatasync.config;

import com.example.remotedatasync.bean.DatabaseMapping;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * DRPlatform 业务配置绑定（前缀 DRPlatform）。
 *
 * @author 50707
 */
@Data
@Component
@ConfigurationProperties(prefix = "drplatform")
public class DRPlatformProperties {

    /** 是否灾备中心模式 */
    private boolean standbyMode = true;

    /** Flink 集群相关配置（本地默认不启用） */
    private Flink flink = new Flink();

    /** 内嵌同步引擎参数 */
    private Engine engine = new Engine();

    /** 同步引擎模式：flink-cdc（默认，binlog 全量+增量）| polling（轮询兜底，仅用于无 REPLICATION 权限的源） */
    private Sync sync = new Sync();

    /** Flink CDC（binlog 同步引擎）参数 */
    private Cdc cdc = new Cdc();

    /** 连接池上限 */
    private Connection connection = new Connection();

    /** 主备切换（HA）相关配置 */
    private Ha ha = new Ha();

    /** 数据库映射列表 */
    private List<DatabaseMapping> mappings = new ArrayList<>();

    @Data
    public static class Flink {
        private boolean enabled = false;
        private int parallelism = 2;
        private long checkpointIntervalMs = 30000;
        private String savepointPath = "/data/DRPlatform/savepoint";
    }

    @Data
    public static class Engine {
        private long pollIntervalMs = 2000;
        private int batchSize = 1000;
        private int maxQueueSize = 20000;
        private long reconcileIntervalMs = 10000;
        private int deviationTimeoutSec = 300;

        /**
         * 删除对账「连续确认轮数」：目标端某主键需连续 N 轮对账都不存在于源端，才真正发 DELETE。
         * 默认 3（轮询 2s，约 6s），用于规避异步复制延迟/网络抖动导致的「误删已同步数据」。
         */
        private int reconcileDeleteConfirmRounds = 3;

        /**
         * 作业启动（含主备切换、崩溃重启）后的删除对账静默期（毫秒）。
         * 切换到备库时备库可能落后于原主库，静默期内不做任何删除对账，
         * 给备库追平 + 本轮全量/增量续扫完成的时间，避免把「已同步但备库尚未复制到」的数据误删。
         */
        private long postStartReconcileGraceMs = 180_000L;

        /**
         * 删除对账「待确认主键」的最大跟踪条数，超出则停止删除（fail-safe：不删而非误删）。
         */
        private int reconcileMissMaxEntries = 200_000;

        /**
         * 增量扫描回看窗口（秒，默认 10）。
         * <p>
         * 表的 {@code update_time} 若为秒级 DATETIME，则<b>同一秒内的第二次修改不会推进 update_time</b>；
         * 而增量扫描以 {@code (update_time, 主键)} 为键集，该行的键恰好等于已 ack 水位 → 该行再也扫不到，
         * 灾备副本会长期漂移。每轮从「水位 - 回看窗口」重新扫描即可幂等地捕获这类同秒更新
         * （upsert 幂等，重读无副作用）。
         * </p>
         * <p>
         * <b>根治建议</b>：源表 {@code update_time} 使用 {@code DATETIME(3)/(6)} 亚秒精度，
         * 从根本上避免同秒多次修改不可见；本参数作为兜底。
         * </p>
         */
        private int incrementalLookbackSeconds = 10;
    }

    @Data
    public static class Sync {
        /** flink-cdc = binlog 全量+增量（默认）；polling = 旧轮询引擎（兜底） */
        private String mode = "flink-cdc";
    }

    @Data
    public static class Cdc {
        /** checkpoint 间隔（毫秒）。binlog 位点随 checkpoint 持久化，崩溃后从最近 checkpoint 续传 */
        private long checkpointIntervalMs = 3000;
        /** savepoint 根目录（每主机对一个子目录） */
        private String savepointPath = "savepoints";
        /**
         * Flink 作业并行度（binlog 读取段并行度）。
         * <b>0 = 自动适配</b>：按 CPU 内核数取值（下限 1、上限 64），
         * 服务器核越多同步越快；&gt;0 则为显式配置，优先生效。
         */
        private int parallelism = 0;
        /** 内嵌 TaskManager 进程内存（MiniCluster 与 JM 同 JVM，需留足 JVM 堆） */
        private String taskmanagerMemory = "1024m";

        /**
         * 解析实际生效的并行度：显式配置（&gt;0）优先；未配置（&lt;=0）按 CPU 内核自动适配，
         * 夹在 [1, 64] 内防极端机器（如 128 核超线程）把内嵌 MiniCluster 撑爆。
         */
        public int resolveParallelism() {
            if (parallelism > 0) {
                return Math.min(parallelism, 64);
            }
            return Math.min(64, Math.max(1, Runtime.getRuntime().availableProcessors()));
        }
        /** 快照分片大小（行）：影响快照阶段并行切片粒度 */
        private int snapshotChunkSize = 8096;
        /** 快照读取 fetch size */
        private int fetchSize = 2048;
        /**
         * 源库服务器时区。必须与源库实际时区一致，否则 DATETIME/TIMESTAMP 会整体偏移。
         * 容器化 MySQL 通常是 UTC；Linux 主机部署通常是 Asia/Shanghai。
         */
        private String serverTimeZone = "UTC";
        /** 心跳间隔（秒）：空闲时也推进 checkpoint 位点，保证崩溃恢复粒度 */
        private int heartbeatIntervalSec = 3;
        /** 是否将 binlog 中的 DDL 事件纳入流（用于同步表结构变更） */
        private boolean includeSchemaChanges = true;
        /** 是否自动发现源库新增表 */
        private boolean scanNewlyAddedTable = true;
        /** sink 批大小（行）：达到即 flush，兼顾延迟与吞吐 */
        private int sinkBatchSize = 500;
        /** 每个目标库的共享连接池上限（Hikari，全作业共享，空闲自动回收） */
        private int sinkPoolSize = 2;
        /** 源侧 flink-cdc 内部连接池大小 */
        private int sourceConnectionPoolSize = 4;
        /** 复制连接 server-id 基址（每映射在此基础上派生唯一值） */
        private int serverIdBase = 60000;
        /** 延迟告警阈值（秒）：事件时间与落库时间差超过即告警 */
        private int lagWarnSeconds = 10;
        /**
         * 源库结构巡检间隔（秒）：周期扫描源库用户库/表，发现<b>新增库或新增表</b>时
         * 自动打 savepoint 并重启作业，把新库表纳入同步。
         * <p>
         * flink-cdc 的 scan-newly-added-table 只在作业启动/恢复时探测一次，
         * 运行期不会自动发现，因此必须由本层巡检补偿。设为 0 表示关闭巡检。
         */
        private int discoveryIntervalSec = 20;
        /** 巡检触发重启的最小间隔（秒）：避免故障期频繁重启 */
        private int discoveryRestartCooldownSec = 60;
    }

    @Data
    public static class Connection {
        private int maxPoolSize = 32;
        private int maxGlobalConnections = 200;
    }

    @Data
    public static class Ha {
        /** 检测到主备切换记录后，是否自动将同步源切到备库（默认 false，由人工 / API 触发，避免误切） */
        private boolean switchAutoResync = false;

        /**
         * 切换时是否自动对齐新源的 AUTO_INCREMENT（默认 true）。
         * 备库提升为新主库时其自增位点可能低于灾备目标已存在的主键，新写入会复用旧 id，
         * 与目标端「不同业务行同主键」冲突并被 upsert 覆盖。开启后切换会自动推进新源自增位点。
         */
        private boolean autoAlignSourceAutoIncrement = true;

        /**
         * 主备切换后是否强制对新源做一次<b>全量重扫</b>（默认 true）。
         * <p>
         * 水位表以 {@code SOURCE_IP} 为键、不含端口，主备同主机不同端口切换时会复用旧水位，
         * 新源中 {@code update_time ≤ 旧水位} 的行会被跳过。若新源不是旧源的严格超集
         * （备库独有行、异步复制尚未追平等），这些行将永远无法同步 → 数据缺失。
         * 开启后切换会清空该源水位并重扫一遍，由幂等 upsert 保证不重不丢。
         * </p>
         * <p>
         * 代价：大表切换后需完整扫描一次。若表规模极大且可接受"增量续传 + 人工核对"，
         * 可设为 false。
         * </p>
         */
        private boolean fullResyncAfterSwitch = true;
    }
}
