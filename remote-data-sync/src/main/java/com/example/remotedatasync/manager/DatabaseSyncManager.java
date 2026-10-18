package com.example.remotedatasync.manager;

import com.example.remotedatasync.bean.DatabaseConfig;
import com.example.remotedatasync.bean.DatabaseMapping;
import com.example.remotedatasync.bean.TableInfo;
import com.example.remotedatasync.cdc.FlinkCdcJobManager;
import com.example.remotedatasync.common.ConnectionTestResult;
import com.example.remotedatasync.config.DRPlatformProperties;
import com.example.remotedatasync.dto.DatabaseMappingVO;
import com.example.remotedatasync.dto.MappingBatchResult;
import com.example.remotedatasync.dto.SyncProgressQuery;
import com.example.remotedatasync.po.SyncProgress;
import com.example.remotedatasync.service.DatabaseMetadataService;
import com.example.remotedatasync.service.SyncWatermarkService;
import com.example.remotedatasync.common.CryptoUtil;
import com.example.remotedatasync.sync.SyncHostJob;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.springframework.util.StringUtils;

/**
 * 同步服务总控：负责启动 / 停止 / 重启主机对同步任务，维护内存进度与运行作业。
 * 支持页面动态新增 / 移除同步主机对（并持久化到本地文件，重启后自动恢复）。
 * <p>
 * 模型为<b>主机对</b>（源主机 -> 目标主机，1:1）；每个主机对启动一个 SyncHostJob，
 * 自动发现并同步该源主机下所有用户库与表（忽略项除外）。
 * </p>
 *
 * @author 50707
 */
@Slf4j
@Component
public class DatabaseSyncManager {

    /** 内存同步进度缓存：sourceHost|sourceDbName -> SyncProgress */
    public static final Map<String, SyncProgress> syncProgressMap = new ConcurrentHashMap<>();

    /** 动态映射持久化文件（页面新增的映射，重启后自动加载） */
    private static final String DYNAMIC_MAPPINGS_FILE = "DRPlatform-dynamic-mappings.json";

    private final DRPlatformProperties properties;
    private final DynamicShardedConnectionManager connMgr;
    private final GroupedExecutorManager executorManager;
    private final TableLevelQueueManager queueManager;
    private final DatabaseMetadataService metadataService;
    private final SyncWatermarkService watermarkService;
    private final FlinkCdcJobManager cdcJobManager;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** 运行中的同步作业：instanceKey(主机对) -> SyncHostJob */
    private final Map<String, SyncHostJob> runningJobs = new ConcurrentHashMap<>();

    /** 通过页面动态新增的映射 key 集合（用于持久化与来源标记） */
    private final Set<String> dynamicKeys = ConcurrentHashMap.newKeySet();

    /** 增删映射时的互斥锁 */
    private final Object mappingsLock = new Object();

    public DatabaseSyncManager(DRPlatformProperties properties,
                               DynamicShardedConnectionManager connMgr,
                               GroupedExecutorManager executorManager,
                               TableLevelQueueManager queueManager,
                               DatabaseMetadataService metadataService,
                               SyncWatermarkService watermarkService,
                               FlinkCdcJobManager cdcJobManager) {
        this.properties = properties;
        this.connMgr = connMgr;
        this.executorManager = executorManager;
        this.queueManager = queueManager;
        this.metadataService = metadataService;
        this.watermarkService = watermarkService;
        this.cdcJobManager = cdcJobManager;
    }

    /** 是否 binlog（Flink CDC）模式：零轮询查询源库，全量+增量 + savepoint 断点恢复 */
    private boolean isCdcMode() {
        return cdcJobManager.isCdcMode();
    }

    @PostConstruct
    public void startSyncTask() {
        if (!properties.isStandbyMode()) {
            log.warn("This server is not a disaster recovery center, sync service will not start.");
            return;
        }
        loadDynamicMappings();

        List<DatabaseMapping> mappings = properties.getMappings();
        if (mappings == null || mappings.isEmpty()) {
            log.error("Database mapping configuration is empty, please check backup configuration.");
            return;
        }
        log.info("Starting sync task, parsed host-pair mapping count: {} (mode={})",
                mappings.size(), isCdcMode() ? "flink-cdc" : "polling");
        Set<String> started = new HashSet<>();
        for (DatabaseMapping mapping : mappings) {
            String key = mapping.getInstanceKey();
            if (!started.add(key)) {
                log.warn("Duplicate host-pair mapping skipped: {}", key);
                continue;
            }
            try {
                if (isCdcMode()) {
                    cdcJobManager.startOrResume(mapping);
                } else {
                    startHostJob(mapping);
                }
            } catch (Exception e) {
                log.error("Failed to start sync for mapping {}", key, e);
            }
        }
        log.info("Sync task started, running cdc jobs: {}, polling jobs: {}",
                isCdcMode() ? mappings.size() : 0, isCdcMode() ? 0 : runningJobs.size());
    }

private SyncHostJob startHostJob(DatabaseMapping mapping) {
     SyncHostJob job = new SyncHostJob(mapping, properties, connMgr, executorManager,
             queueManager, metadataService, syncProgressMap, watermarkService);
     job.start();
     runningJobs.put(job.getInstanceKey(), job);
     return job;
 }

 /**
  * 按当前引擎模式启动映射对应的同步作业。
  * <p>
  * 动态新增映射必须走这里而不是直接 {@link #startHostJob}：CDC 模式下若额外起一个轮询作业，
  * 同一条链路会同时存在 binlog 与水位轮询两条写入通道，互相覆盖且白白消耗源库连接。
  * </p>
  */
 private void startJobForMapping(DatabaseMapping mapping) {
     if (isCdcMode()) {
         cdcJobManager.startInitial(mapping);
         log.info("Started CDC job for dynamically added mapping {}", mapping.getInstanceKey());
     } else {
         startHostJob(mapping);
     }
 }

    /** 重新同步指定主机对（全量重置） */
    public Map<String, List<SyncProgressQuery>> resyncDatabases(List<SyncProgressQuery> databaseList) {
        Map<String, List<SyncProgressQuery>> result = new HashMap<>();
        List<SyncProgressQuery> success = new ArrayList<>();
        List<SyncProgressQuery> failed = new ArrayList<>();
        for (SyncProgressQuery q : databaseList) {
            DatabaseMapping mapping = findMappingByIp(q.getIp());
            if (mapping == null) {
                log.warn("No mapping for ip={}", q.getIp());
                failed.add(q);
                continue;
            }
            SyncHostJob job = runningJobs.get(mapping.getInstanceKey());
            try {
                if (isCdcMode()) {
                    // CDC 模式全量重扫：停旧作业（归档 savepoint）→ 全新 INITIAL（快照+binlog）
                    String key = mapping.getInstanceKey();
                    cdcJobManager.stopWithSavepoint(key);
                    cdcJobManager.startInitial(mapping);
                } else {
                    job = runningJobs.get(mapping.getInstanceKey());
                    if (job == null) {
                        startHostJob(mapping);
                    } else {
                        job.fullReset();
                    }
                }
                success.add(q);
            } catch (Exception e) {
                log.error("Resync failed for {}", mapping.getInstanceKey(), e);
                failed.add(q);
            }
        }
        if (!success.isEmpty()) {
            result.put("success", success);
        }
        if (!failed.isEmpty()) {
            result.put("failed", failed);
        }
        return result;
    }

    /** 精确查找主机对映射（按源/目标 IP 匹配；dbName 仅作兼容，不再用于匹配） */
    public DatabaseMapping findMapping(String ip, String dbName) {
        return findMappingByIp(ip);
    }

    /** 在锁内对 mappings 做快照，避免读路径与写路径并发导致 ConcurrentModificationException。 */
    private List<DatabaseMapping> snapshotMappings() {
        synchronized (mappingsLock) {
            return new ArrayList<>(properties.getMappings());
        }
    }

    public DatabaseMapping findMappingByIp(String ip) {
        if (ip == null) {
            return null;
        }
        return snapshotMappings().stream()
                .filter(m -> ip.equals(m.getSourceHost()) || ip.equals(m.getTargetHost()))
                .findFirst().orElse(null);
    }

    public DatabaseMapping findByKey(String instanceKey) {
        if (instanceKey == null || properties.getMappings() == null) {
            return null;
        }
        List<DatabaseMapping> mappings = snapshotMappings();
        DatabaseMapping exact = mappings.stream()
                .filter(m -> instanceKey.equals(m.getInstanceKey()))
                .findFirst().orElse(null);
        if (exact != null) {
            return exact;
        }
        // 兼容旧格式入参（"ip->ip"，不含端口）：仅在能唯一确定一条链路时才接受，
        // 避免同主机多端口环境（容器化验证环境）下误匹配到另一条链路。
        List<DatabaseMapping> legacy = mappings.stream()
                .filter(m -> instanceKey.equals(m.getHostOnlyKey()))
                .collect(Collectors.toList());
        return legacy.size() == 1 ? legacy.get(0) : null;
    }

    /**
     * 页面动态新增同步主机对：去重 + 启动同步作业 + 持久化。
     */
    public String addMapping(DatabaseMapping mapping) {
        String key = mapping.getInstanceKey();
        synchronized (mappingsLock) {
            if (findByKey(key) != null) {
                throw new IllegalStateException("MAPPING_ALREADY_EXISTS:" + key);
            }
            startJobForMapping(mapping);
            if (!containsByKey(key)) {
                properties.getMappings().add(mapping);
            }
            dynamicKeys.add(key);
            persistDynamicMappings();
            log.info("Added dynamic host-pair mapping {} and started sync job", key);
            return key;
        }
    }

    /**
     * 批量新增同步主机对：逐个去重 + 启动 + 持久化。
     * <p>
     * 新增的 mapping 自动绑定当前操作用户 ID（从 UserContext 获取），
     * 实现配置按用户区分。userId 为 null 的 mapping 对所有用户可见。
     * </p>
     */
    public MappingBatchResult addMappings(List<DatabaseMapping> mappings) {
        MappingBatchResult result = new MappingBatchResult();
        if (mappings == null || mappings.isEmpty()) {
            return result;
        }
        // 绑定当前操作用户 ID
        Long currentUserId = com.example.remotedatasync.common.UserContext.getUserId();
        synchronized (mappingsLock) {
            for (DatabaseMapping mapping : mappings) {
                String key = mapping.getInstanceKey();
                if (findByKey(key) != null) {
                    result.getSkipped().add(key);
                    continue;
                }
                try {
                    // 设置所属用户 ID（实现配置按用户区分）
                    if (mapping.getUserId() == null) {
                        mapping.setUserId(currentUserId);
                    }
                    startJobForMapping(mapping);
                    if (!containsByKey(key)) {
                        properties.getMappings().add(mapping);
                    }
                    dynamicKeys.add(key);
                    result.getCreated().add(key);
                } catch (Exception e) {
                    log.error("Failed to add mapping {}", key, e);
                    result.getFailed().add(key + ": " + e.getMessage());
                }
            }
            persistDynamicMappings();
        }
        log.info("Batch add mappings done: created={}, skipped={}, failed={}, userId={}",
                result.getCreated().size(), result.getSkipped().size(), result.getFailed().size(), currentUserId);
        return result;
    }

    private boolean containsByKey(String instanceKey) {
        return properties.getMappings().stream().anyMatch(m -> instanceKey.equals(m.getInstanceKey()));
    }

    /** 移除指定主机对：停止作业 + 关闭连接池 + 从列表移除 + 持久化 */
    public void removeMapping(String instanceKey) {
        synchronized (mappingsLock) {
            DatabaseMapping mapping = findByKey(instanceKey);
            if (mapping == null) {
                throw new IllegalStateException("MAPPING_NOT_FOUND:" + instanceKey);
            }
            SyncHostJob job = runningJobs.remove(instanceKey);
            if (job != null) {
                job.stop();
            }
            if (isCdcMode()) {
                cdcJobManager.removeJob(instanceKey);
            }
            // 修复连接池泄漏：按主机清理所有分片池（含按库名创建的池），而非仅清理空库名的池
            connMgr.removeConnectionPoolsByHost(mapping.getSourceHost(), mapping.getSourcePort());
            connMgr.removeConnectionPoolsByHost(mapping.getTargetHost(), mapping.getTargetPort());
            properties.getMappings().removeIf(m -> instanceKey.equals(m.getInstanceKey()));
            dynamicKeys.remove(instanceKey);
            persistDynamicMappings();
            log.info("Removed host-pair mapping {}", instanceKey);
        }
    }

    /**
     * 重新加载指定主机对的配置并重建同步作业（配置热生效）。
     * <p>
     * 用于修改 ignoreDatabases / ignoreTables / transformRules 等配置后的热生效：
     * 停止旧作业 → 清理旧连接池 → 用最新 mapping 对象创建新 SyncHostJob 并启动。
     * 新作业的 watermarks / knownSignatures 从空白开始（等价于 fullReset 的效果），
     * 下一轮扫描自动全量重扫并重建水位，依靠幂等 upsert 保证数据最终一致。
     * </p>
     *
     * @param instanceKey 主机对实例 key（sourceHost->targetHost）
     * @return 重建是否成功
     */
    public boolean reloadMapping(String instanceKey) {
        synchronized (mappingsLock) {
            DatabaseMapping mapping = findByKey(instanceKey);
            if (mapping == null) {
                throw new IllegalStateException("MAPPING_NOT_FOUND:" + instanceKey);
            }
            // 1. 停止旧作业
            SyncHostJob oldJob = runningJobs.remove(instanceKey);
            if (oldJob != null) {
                oldJob.stop();
            }
            if (isCdcMode()) {
                cdcJobManager.removeJob(instanceKey);
            }
            // 2. 清理旧连接池（含按库名分片的池）
            connMgr.removeConnectionPoolsByHost(mapping.getSourceHost(), mapping.getSourcePort());
            connMgr.removeConnectionPoolsByHost(mapping.getTargetHost(), mapping.getTargetPort());
            // 3. 用最新 mapping 对象重建作业（ignore 等配置从 mapping 重新读取）
            try {
                if (isCdcMode()) {
                    // CDC 模式下 ignore 规则序列化在 savepoint 的 sink 里，必须全新 INITIAL 才能热生效
                    cdcJobManager.startInitial(mapping);
                } else {
                    startHostJob(mapping);
                }
                log.info("Reloaded host-pair mapping {} with latest config", instanceKey);
                return true;
            } catch (Exception e) {
                log.error("Failed to reload mapping {}", instanceKey, e);
                return false;
            }
        }
    }

    /**
     * 主备切换：将某主机对的同步源在「主库」与「备库」之间<b>来回切换</b>，并重建作业。
     * <p>
     * <b>可反复切换</b>：本方法执行「源↔备源」地址交换 —— 原 {@code sourceHost/sourcePort}
     * 落到 {@code sourceStandbyHost/sourceStandbyPort}，原备源成为新源。因此连续调用即可在主备之间
     * 往返任意多次（切过去后再调用即切回），无需额外的 switch-back 接口。
     * </p>
     * <p>
     * 切换时依次完成：停止旧作业 → 释放旧源连接池 → 交换源地址 →
     * <b>主键对齐</b>（{@link #alignSourceAutoIncrement}，防新源复用目标端已存在的主键）→
     * 用新源重建作业 → 标记换源（删除对账进入静默期，防误删落后的新源）。
     * </p>
     *
     * @param oldInstanceKey 切换前的实例 key
     * @return 切换是否成功
     */
    public boolean switchSourceToStandby(String oldInstanceKey) {
        synchronized (mappingsLock) {
            DatabaseMapping mapping = findByKey(oldInstanceKey);
            if (mapping == null) {
                throw new IllegalStateException("MAPPING_NOT_FOUND:" + oldInstanceKey);
            }
            if (mapping.getSourceStandbyHost() == null) {
                throw new IllegalStateException("NO_STANDBY_CONFIGURED:" + oldInstanceKey);
            }
            String oldKey = mapping.getInstanceKey();
            // 1. 停止旧作业并释放旧源连接池（instanceKey 含端口，换源后 key 会变，必须用旧 key 停）
            SyncHostJob oldJob = runningJobs.remove(oldInstanceKey);
            if (oldJob != null) {
                oldJob.stop();
            }
            if (isCdcMode()) {
                // binlog 模式：旧作业停并归档 savepoint（换源后旧位点不可用，仅作审计留存）
                cdcJobManager.stopWithSavepoint(oldInstanceKey);
            }
            connMgr.removeConnectionPoolsByHost(mapping.getSourceHost(), mapping.getSourcePort());

            // 2. 交换源地址与备源地址（使切换可反复来回执行）
            String prevHost = mapping.getSourceHost();
            int prevPort = mapping.getSourcePort();
            mapping.setSourceHost(mapping.getSourceStandbyHost());
            mapping.setSourcePort(mapping.getSourceStandbyPort());
            mapping.setSourceStandbyHost(prevHost);
            mapping.setSourceStandbyPort(prevPort);
            log.info("Source switched for {}: {}:{} -> {}:{} (previous source kept as standby)",
                    oldKey, prevHost, prevPort, mapping.getSourceHost(), mapping.getSourcePort());

            // 3. 主键对齐：新源自增位点必须高于灾备目标已存在的主键，否则新写入会复用旧 id，
            //    与目标端「不同业务行但同主键」的记录冲突，导致 upsert 互相覆盖。
            try {
                alignSourceAutoIncrement(mapping);
            } catch (Exception e) {
                log.warn("Auto-increment alignment skipped for {}: {}", oldKey, e.getMessage());
            }

            // 4. 用新源地址重建作业并标记换源
            try {
                if (isCdcMode()) {
                    // binlog 模式：新源以 INITIAL 全量重扫（快照+binlog），旧水位概念不适用
                    cdcJobManager.startInitial(mapping);
                    log.info("Switched source successfully for {} (new instanceKey={}, CDC mode: initial rescan)",
                            oldKey, mapping.getInstanceKey());
                } else {
                    SyncHostJob newJob = startHostJob(mapping);
                    // 强制全量重扫：新源可能不是旧源的严格超集，必须完整扫一遍才能保证不漏。
                    // 换源前已先对齐自增位点，故重扫不会产生主键覆盖。
                    if (properties.getHa().isFullResyncAfterSwitch()) {
                        watermarkService.deleteBySource(mapping.getSourceHost());
                        log.info("Cleared watermarks of new source {} -> full rescan will start from scratch",
                                mapping.getSourceHost());
                    }
                    newJob.markSourceSwitched();
                    log.info("Switched source successfully for {} (new instanceKey={})",
                            oldKey, mapping.getInstanceKey());
                }
            } catch (Exception e) {
                log.error("Failed to start job after switching source for {}", oldKey, e);
                return false;
            }
            persistRoleChange(mapping);
            return true;
        }
    }

    /**
     * 生产中心 ↔ 灾备中心 角色互换（<b>灾备切生产 / 生产切灾备</b>）。
     * <p>
     * 与 {@link #switchSourceToStandby}（同一中心内部的主库↔备库）不同，本方法交换的是
     * <b>同步方向</b>：原灾备中心（target）提升为新的生产中心（source），原生产中心降级为新的灾备中心。
     * 典型用法：
     * <ul>
     *   <li><b>灾备切生产（failover）</b>：生产中心整体故障，灾备接管写入，反向同步保留原生产数据；</li>
     *   <li><b>生产切灾备（failback）</b>：原生产恢复后，再次调用本方法切回原方向。</li>
     * </ul>
     * </p>
     * <p>
     * <b>可反复调用（可逆）</b>：方法执行「源↔目标」整体互换（host/port/user/password），
     * 而备源地址保持不变（仍挂在源侧），因此连续调用两次必然回到初始状态。
     * </p>
     * <p>
     * 切换步骤与 {@link #switchSourceToStandby} 一致：停旧作业 → 释放两端连接池 →
     * 互换角色 → 主键对齐 → 用新方向重建作业。
     * </p>
     *
     * @param instanceKey 切换前的实例 key
     * @return 切换是否成功
     */
    public boolean reverseDirection(String instanceKey) {
        synchronized (mappingsLock) {
            DatabaseMapping mapping = findByKey(instanceKey);
            if (mapping == null) {
                throw new IllegalStateException("MAPPING_NOT_FOUND:" + instanceKey);
            }
            String oldKey = mapping.getInstanceKey();
            String prevSrc = mapping.getSourceHost() + ":" + mapping.getSourcePort();
            String prevTgt = mapping.getTargetHost() + ":" + mapping.getTargetPort();

            // 1. 停止旧作业。注意必须用「旧 key」：互换后 instanceKey 会变成反向值。
            SyncHostJob oldJob = runningJobs.remove(oldKey);
            if (oldJob != null) {
                oldJob.stop();
            }
            // 2. 释放两端连接池：源与目标角色都会变，缓存的连接配置全部失效
            connMgr.removeConnectionPoolsByHost(mapping.getSourceHost(), mapping.getSourcePort());
            connMgr.removeConnectionPoolsByHost(mapping.getTargetHost(), mapping.getTargetPort());

            // 3. 互换源 / 目标（含账号密码）。备源地址保持不变 —— 它代表「生产中心侧的备用节点」，
            //    且保持不变才能让本方法调用两次后回到初始状态。
            String sh = mapping.getSourceHost();
            int sp = mapping.getSourcePort();
            String su = mapping.getSourceUser();
            String sw = mapping.getSourcePassword();
            mapping.setSourceHost(mapping.getTargetHost());
            mapping.setSourcePort(mapping.getTargetPort());
            mapping.setSourceUser(mapping.getTargetUser());
            mapping.setSourcePassword(mapping.getTargetPassword());
            mapping.setTargetHost(sh);
            mapping.setTargetPort(sp);
            mapping.setTargetUser(su);
            mapping.setTargetPassword(sw);
            log.info("Direction reversed for {}: {} (prod) -> {} (dr)  ==>  new prod={} , new dr={}",
                    oldKey, prevSrc, prevTgt, mapping.getSourceHost() + ":" + mapping.getSourcePort(),
                    mapping.getTargetHost() + ":" + mapping.getTargetPort());

            // 4. 主键对齐：新的生产中心（原灾备）自增位点可能落后于新的灾备中心（原生产）已有的主键，
            //    否则新写入会复用旧 id 造成 upsert 互相覆盖。
            try {
                alignSourceAutoIncrement(mapping);
            } catch (Exception e) {
                log.warn("Auto-increment alignment skipped for {}: {}", oldKey, e.getMessage());
            }

            // 5. 用新方向重建作业（CDC 走 INITIAL：反向后旧 binlog 位点对新源完全无意义）
            try {
                if (isCdcMode()) {
                    cdcJobManager.onSourceSwitched(oldKey, mapping);
                    log.info("Reversed direction successfully for {} (new instanceKey={}, CDC mode: initial rescan)",
                            oldKey, mapping.getInstanceKey());
                } else {
                    SyncHostJob newJob = startHostJob(mapping);
                    if (properties.getHa().isFullResyncAfterSwitch()) {
                        watermarkService.deleteBySource(mapping.getSourceHost());
                    }
                    newJob.markSourceSwitched();
                    log.info("Reversed direction successfully for {} (new instanceKey={})",
                            oldKey, mapping.getInstanceKey());
                }
            } catch (Exception e) {
                log.error("Failed to start job after reversing direction for {}", oldKey, e);
                return false;
            }
            persistRoleChange(mapping);
            return true;
        }
    }

    /**
     * 持久化「换源 / 换向」后的运行时角色。
     * <p>
     * 不持久化的话，应用重启（或演练中的断电重启）会直接按 yml 退回原始方向 ——
     * 在真实的容灾切换中这会表现为「故障切换被静默回滚」，是本类故障里危害最大的一种。
     * 故一旦发生角色变更，就把该链路的最新方向写入动态映射文件，重启后优先于 yml 生效。
     * </p>
     */
    private void persistRoleChange(DatabaseMapping mapping) {
        dynamicKeys.add(mapping.getInstanceKey());
        // 清理已不存在于 mappings 中的历史 key（换向后旧 key 已失效），避免集合无限增长
        dynamicKeys.removeIf(k -> properties.getMappings().stream()
                .noneMatch(m -> k.equals(m.getInstanceKey())));
        persistDynamicMappings();
    }

    /**
     * 主键对齐：确保新源的 AUTO_INCREMENT 高于灾备目标中已存在的主键。
     * <p>
     * <b>为什么必要</b>：主库故障后备库提升为新主库时，其自增位点可能低于灾备目标已同步过来的主键
     * （例如「主库有、备库尚未复制到」的那批行已存在于灾备目标）。此时新源的新写入会复用这些 id，
     * 而 upsert 以主键作为冲突键 → <b>不同业务行会互相覆盖</b>，造成真实数据丢失。
     * </p>
     * <p>
     * 对每个「单列主键」的用户表，比较目标端 MAX(主键) 与新源 MAX(主键)，
     * 若目标端更大，则在新源执行 {@code ALTER TABLE ... AUTO_INCREMENT = 目标MAX+1}。
     * 该操作只改元数据、不锁数据行，是 DBA 在备库提升时的标准动作。
     * </p>
     * <p>
     * 可通过 {@code drplatform.ha.auto-align-source-autoincrement=false} 关闭。
     * 任一表失败只记录告警，不阻断切换（失败即退化为原有行为）。
     * </p>
     */
    private void alignSourceAutoIncrement(DatabaseMapping mapping) {
        if (!properties.getHa().isAutoAlignSourceAutoIncrement()) {
            log.info("Auto-increment alignment disabled by config, skip");
            return;
        }
        DatabaseConfig newSourceHost = mapping.toSourceHostConfig();
        DatabaseConfig targetHost = mapping.toTargetHostConfig();
        List<String> dbs;
        try {
            dbs = metadataService.getAllUserDatabases(newSourceHost, mapping.getIgnoreDatabases());
        } catch (Exception e) {
            log.warn("alignSourceAutoIncrement: cannot list source databases: {}", e.getMessage());
            return;
        }
        int aligned = 0;
        for (String db : dbs) {
            if (!StringUtils.hasText(db)) {
                continue;
            }
            List<String> tables;
            try {
                tables = metadataService.listTables(mapping.toSourceDB(db), db);
            } catch (Exception e) {
                continue;
            }
            DatabaseConfig targetDB = mapping.toTargetDB(db);
            for (String table : tables) {
                try {
                    TableInfo tInfo = metadataService.getTableInfo(targetDB, db, table);
                    if (tInfo == null || tInfo.getPrimaryKeyColumns() == null
                            || tInfo.getPrimaryKeyColumns().size() != 1) {
                        continue; // 仅处理单列主键表
                    }
                    String pk = tInfo.getPrimaryKeyColumns().get(0);
                    Long targetMax = queryMaxPk(targetHost, db, table, pk);
                    if (targetMax == null) {
                        continue;
                    }
                    Long sourceMax = queryMaxPk(mapping.toSourceDB(db), db, table, pk);
                    if (sourceMax != null && sourceMax >= targetMax) {
                        continue; // 新源主键已领先，无需调整
                    }
                    long next = targetMax + 1;
                    execOnSource(mapping.toSourceDB(db),
                            "ALTER TABLE `" + db + "`.`" + table + "` AUTO_INCREMENT = " + next);
                    aligned++;
                    log.info("Aligned AUTO_INCREMENT for {}.{} on new source: {} (target MAX({})={}, source MAX={})",
                            db, table, next, pk, targetMax, sourceMax);
                } catch (Exception e) {
                    log.warn("alignSourceAutoIncrement failed for {}.{}: {}", db, table, e.getMessage());
                }
            }
        }
        log.info("Auto-increment alignment finished for {}: {} table(s) adjusted",
                newSourceHost.getHost(), aligned);
    }

    /** 查询某表指定列的最大值（列不存在/表不存在返回 null） */
    private Long queryMaxPk(DatabaseConfig cfg, String db, String table, String col) {
        Connection conn = null;
        try {
            conn = connMgr.getConnection(cfg);
            try (java.sql.PreparedStatement ps = conn.prepareStatement(
                    "SELECT MAX(`" + col + "`) FROM `" + db + "`.`" + table + "`")) {
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        long v = rs.getLong(1);
                        return rs.wasNull() ? null : v;
                    }
                }
            }
        } catch (Exception e) {
            return null;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ignore) {
                }
                connMgr.releaseConnection();
            }
        }
        return null;
    }

    /** 在源库执行一条 DDL（用于 AUTO_INCREMENT 对齐） */
    private void execOnSource(DatabaseConfig cfg, String sql) throws SQLException {
        Connection conn = null;
        try {
            conn = connMgr.getConnection(cfg);
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
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

    /**
     * 运行时更新指定主机对的配置字段（ignoreDatabases / ignoreTables / transformRules 等）。
     * <p>
     * 仅更新内存中的 mapping 对象，不持久化到 yml。更新后需调用
     * {@link #reloadMapping(String)} 重建作业才能让新配置对同步引擎生效。
     * 动态新增的 mapping 会同时持久化到 JSON 文件。
     * </p>
     *
     * @param instanceKey 主机对实例 key
     * @param configUpdate 包含待更新字段的 Map（支持 ignoreDatabases, ignoreTables,
     *                     ignoreDdlTables, commonIgnoreTables, commonDdlIgnoreTables,
     *                     transformRules, ignoreTablesByDb, ignoreDdlTablesByDb）
     * @return 更新是否成功
     */
    @SuppressWarnings("unchecked")
    public boolean updateMappingConfig(String instanceKey, Map<String, Object> configUpdate) {
        synchronized (mappingsLock) {
            DatabaseMapping mapping = findByKey(instanceKey);
            if (mapping == null) {
                throw new IllegalStateException("MAPPING_NOT_FOUND:" + instanceKey);
            }
            boolean changed = false;
            // 逐字段更新（仅更新提供的字段，未提供的保持不变）
            if (configUpdate.containsKey("ignoreDatabases")) {
                mapping.setIgnoreDatabases(toStringList(configUpdate.get("ignoreDatabases")));
                changed = true;
            }
            if (configUpdate.containsKey("ignoreTables")) {
                mapping.setIgnoreTables(toStringList(configUpdate.get("ignoreTables")));
                changed = true;
            }
            if (configUpdate.containsKey("ignoreDdlTables")) {
                mapping.setIgnoreDdlTables(toStringList(configUpdate.get("ignoreDdlTables")));
                changed = true;
            }
            if (configUpdate.containsKey("commonIgnoreTables")) {
                mapping.setCommonIgnoreTables(toStringList(configUpdate.get("commonIgnoreTables")));
                changed = true;
            }
            if (configUpdate.containsKey("commonDdlIgnoreTables")) {
                mapping.setCommonDdlIgnoreTables(toStringList(configUpdate.get("commonDdlIgnoreTables")));
                changed = true;
            }
            if (configUpdate.containsKey("transformRules")) {
                Object val = configUpdate.get("transformRules");
                if (val instanceof List) {
                    List<com.example.remotedatasync.bean.TransformRule> rules = new ArrayList<>();
                    for (Object item : (List<?>) val) {
                        if (item instanceof Map) {
                            Map<String, Object> m = (Map<String, Object>) item;
                            com.example.remotedatasync.bean.TransformRule rule = new com.example.remotedatasync.bean.TransformRule();
                            if (m.containsKey("dbName")) rule.setDbName((String) m.get("dbName"));
                            if (m.containsKey("tableName")) rule.setTableName((String) m.get("tableName"));
                            if (m.containsKey("fieldName")) rule.setFieldName((String) m.get("fieldName"));
                            if (m.containsKey("sourceValue")) rule.setSourceValue((String) m.get("sourceValue"));
                            if (m.containsKey("targetValue")) rule.setTargetValue((String) m.get("targetValue"));
                            rules.add(rule);
                        }
                    }
                    mapping.setTransformRules(rules);
                    changed = true;
                }
            }
            if (configUpdate.containsKey("ignoreTablesByDb")) {
                Object val = configUpdate.get("ignoreTablesByDb");
                if (val instanceof List) {
                    List<com.example.remotedatasync.bean.DbTableIgnore> ignores = new ArrayList<>();
                    for (Object item : (List<?>) val) {
                        if (item instanceof Map) {
                            Map<String, Object> m = (Map<String, Object>) item;
                            com.example.remotedatasync.bean.DbTableIgnore ignore = new com.example.remotedatasync.bean.DbTableIgnore();
                            if (m.containsKey("database")) ignore.setDatabase((String) m.get("database"));
                            if (m.containsKey("tables")) ignore.setTables(toStringList(m.get("tables")));
                            ignores.add(ignore);
                        }
                    }
                    mapping.setIgnoreTablesByDb(ignores);
                    changed = true;
                }
            }
            if (configUpdate.containsKey("ignoreDdlTablesByDb")) {
                Object val = configUpdate.get("ignoreDdlTablesByDb");
                if (val instanceof List) {
                    List<com.example.remotedatasync.bean.DbTableIgnore> ignores = new ArrayList<>();
                    for (Object item : (List<?>) val) {
                        if (item instanceof Map) {
                            Map<String, Object> m = (Map<String, Object>) item;
                            com.example.remotedatasync.bean.DbTableIgnore ignore = new com.example.remotedatasync.bean.DbTableIgnore();
                            if (m.containsKey("database")) ignore.setDatabase((String) m.get("database"));
                            if (m.containsKey("tables")) ignore.setTables(toStringList(m.get("tables")));
                            ignores.add(ignore);
                        }
                    }
                    mapping.setIgnoreDdlTablesByDb(ignores);
                    changed = true;
                }
            }
            // 动态 mapping 持久化到 JSON 文件
            if (changed && dynamicKeys.contains(instanceKey)) {
                persistDynamicMappings();
            }
            log.info("Updated mapping config for {} (changed={})", instanceKey, changed);
            return changed;
        }
    }

    /** 将 Object 转为 List<String>，兼容 List<String> 和 List<Object> */
    @SuppressWarnings("unchecked")
    private List<String> toStringList(Object val) {
        if (val == null) {
            return new ArrayList<>();
        }
        if (val instanceof List) {
            List<String> result = new ArrayList<>();
            for (Object item : (List<?>) val) {
                result.add(String.valueOf(item));
            }
            return result;
        }
        return new ArrayList<>();
    }

    /** 连接测试透传：返回源/目标两端的测试结果（服务端级，不依赖具体库名） */
    public ConnectionTestResult testSource(DatabaseMapping mapping) {
        return mapping.toSourceHostConfig().testServerConnection();
    }

    public ConnectionTestResult testTarget(DatabaseMapping mapping) {
        return mapping.toTargetHostConfig().testServerConnection();
    }

    /** 按源主机连接信息列出可同步的数据库（排除系统库与忽略库） */
    public List<String> listSourceDatabases(DatabaseConfig cfg) {
        return metadataService.getAllUserDatabases(cfg, List.of());
    }

    /** 当前所有映射的展示对象（密码脱敏，含运行状态与来源） */
    public List<DatabaseMappingVO> listMappingsVO() {
        return listMappingsVO(null);
    }

    /**
     * 按用户过滤的映射展示对象。
     * <p>
     * 返回 userId 为 null（全局/yml 配置）+ userId 等于指定用户的映射。
     * 实现配置按用户区分：用户只能看到全局映射和自己创建的映射。
     * </p>
     *
     * @param userId 当前登录用户 ID（null 表示不过滤，返回全部）
     */
    public List<DatabaseMappingVO> listMappingsVO(Long userId) {
        List<DatabaseMapping> mappings = snapshotMappings();
        if (mappings.isEmpty()) {
            return new ArrayList<>();
        }
        return mappings.stream()
                .filter(m -> userId == null || m.getUserId() == null || userId.equals(m.getUserId()))
                .map(m -> DatabaseMappingVO.from(m, runningJobs.containsKey(m.getInstanceKey()),
                        dynamicKeys.contains(m.getInstanceKey()) ? "dynamic" : "yaml"))
                .collect(Collectors.toList());
    }

    /** 根据 IP 获取该 IP 下的用户数据库列表（去重） */
    public List<String> getDatabasesByIp(String ips) {
        List<String> result = new ArrayList<>();
        List<String> ipList = new ArrayList<>();
        if (ips != null && !ips.isEmpty()) {
            for (String s : ips.split(",")) {
                ipList.add(s.trim());
            }
        }
        for (DatabaseMapping mapping : snapshotMappings()) {
            if (!ipList.isEmpty() && !ipList.contains(mapping.getSourceHost()) && !ipList.contains(mapping.getTargetHost())) {
                continue;
            }
            List<String> dbs = metadataService.getAllUserDatabases(mapping.toSourceHostConfig(), mapping.getIgnoreDatabases());
            result.addAll(dbs);
        }
        return result.stream().distinct().collect(Collectors.toList());
    }

    public List<DatabaseMapping> getMappings() {
        return snapshotMappings();
    }

    public int runningJobCount() {
        return runningJobs.size();
    }

    /** 启动时加载页面动态新增的映射（重启恢复），并解密其中的数据库密码。 */
    private void loadDynamicMappings() {
        File f = new File(DYNAMIC_MAPPINGS_FILE);
        if (!f.exists()) {
            return;
        }
        try {
            List<DatabaseMapping> loaded = objectMapper.readValue(f, new TypeReference<List<DatabaseMapping>>() {});
            for (DatabaseMapping m : loaded) {
                // 解密持久化的密码（历史明文值自动兼容）
                m.setSourcePassword(CryptoUtil.decrypt(m.getSourcePassword()));
                m.setTargetPassword(CryptoUtil.decrypt(m.getTargetPassword()));
                // 同一条物理链路只保留一份配置：yml 里是原始方向，动态文件里是「换源/换向后」的方向，
                // 后者优先。若不去重，重启后会同时启动正反两个作业，双向互相覆盖写入。
                String link = m.getLinkKey();
                properties.getMappings().removeIf(x -> link.equals(x.getLinkKey())
                        && !m.getInstanceKey().equals(x.getInstanceKey()));
                if (findByKey(m.getInstanceKey()) == null) {
                    properties.getMappings().add(m);
                }
                dynamicKeys.add(m.getInstanceKey());
            }
            // 清理失效的历史 key（换向后旧方向的 key 已不在 mappings 中）
            dynamicKeys.removeIf(k -> properties.getMappings().stream()
                    .noneMatch(m -> k.equals(m.getInstanceKey())));
            log.info("Loaded {} dynamic mapping(s) from {}", loaded.size(), f.getAbsolutePath());
        } catch (Exception e) {
            log.warn("Failed to load dynamic mappings from {}: {}", f.getAbsolutePath(), e.getMessage());
        }
    }

    /** 持久化页面动态新增的映射到本地 JSON 文件（密码加密，避免明文落盘）。 */
    private void persistDynamicMappings() {
        try {
            List<DatabaseMapping> toSave = properties.getMappings().stream()
                    .filter(m -> dynamicKeys.contains(m.getInstanceKey()))
                    .map(m -> {
                        // 写盘前克隆并加密密码字段；内存中的真实密码保持不变（同步连接需要）
                        DatabaseMapping copy = objectMapper.convertValue(m, DatabaseMapping.class);
                        copy.setSourcePassword(CryptoUtil.encrypt(m.getSourcePassword()));
                        copy.setTargetPassword(CryptoUtil.encrypt(m.getTargetPassword()));
                        return copy;
                    })
                    .collect(Collectors.toList());
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(DYNAMIC_MAPPINGS_FILE), toSave);
            log.debug("Persisted {} dynamic mapping(s) to {}", toSave.size(), DYNAMIC_MAPPINGS_FILE);
        } catch (Exception e) {
            log.warn("Failed to persist dynamic mappings: {}", e.getMessage());
        }
    }

    @PreDestroy
    public void shutdown() {
        runningJobs.values().forEach(SyncHostJob::stop);
        runningJobs.clear();
        queueManager.TABLE_QUEUES.forEach((k, q) -> q.clear());
        queueManager.TABLE_QUEUES.clear();
        executorManager.shutdownAll();
        connMgr.shutdownAll();
        log.info("DatabaseSyncManager shut down");
    }
}
