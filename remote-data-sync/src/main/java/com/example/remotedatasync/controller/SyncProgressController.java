package com.example.remotedatasync.controller;

import com.example.remotedatasync.bean.DatabaseConfig;
import com.example.remotedatasync.bean.DatabaseMapping;
import com.example.remotedatasync.common.BusinessException;
import com.example.remotedatasync.common.ConnectionTestResult;
import com.example.remotedatasync.common.DRPlatformErrorCodeEnum;
import com.example.remotedatasync.common.InternationalizationEnum;
import com.example.remotedatasync.common.PageRespVO;
import com.example.remotedatasync.common.Result;
import com.example.remotedatasync.common.annotation.OperationLogAnnotation;
import com.example.remotedatasync.dto.DatabaseMappingVO;
import com.example.remotedatasync.dto.MappingBatchResult;
import com.example.remotedatasync.dto.SyncMappingRequestDTO;
import com.example.remotedatasync.dto.SourceTablesRequestDTO;
import com.example.remotedatasync.dto.SyncProgressPageQuery;
import com.example.remotedatasync.dto.SyncProgressQuery;
import com.example.remotedatasync.dto.SyncProgressVO;
import com.example.remotedatasync.manager.DatabaseSyncManager;
import com.example.remotedatasync.service.DatabaseMetadataService;
import com.example.remotedatasync.service.SyncProgressService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 同步进度管理 REST 接口。
 *
 * @author 50707
 */
@Slf4j
@RestController
@RequestMapping("/sync")
@Validated
public class SyncProgressController {

    private final SyncProgressService syncProgressService;
    private final DatabaseSyncManager databaseSyncManager;
    private final DatabaseMetadataService metadataService;
    private final com.example.remotedatasync.cdc.FlinkCdcJobManager cdcJobManager;

    public SyncProgressController(SyncProgressService syncProgressService, DatabaseSyncManager databaseSyncManager,
                                  DatabaseMetadataService metadataService,
                                  com.example.remotedatasync.cdc.FlinkCdcJobManager cdcJobManager) {
        this.syncProgressService = syncProgressService;
        this.databaseSyncManager = databaseSyncManager;
        this.metadataService = metadataService;
        this.cdcJobManager = cdcJobManager;
    }

    /** 分页查询同步进度 */
    @PostMapping("/db/list")
    public PageRespVO<SyncProgressVO> queryPage(@RequestBody @Valid SyncProgressPageQuery query) {
        try {
            PageRespVO<SyncProgressVO> result = syncProgressService.queryPage(query);
            if (result == null || CollectionUtils.isEmpty(result.getResults())) {
                return result;
            }
            for (SyncProgressVO vo : result.getResults()) {
                vo.setSuspensionReason(InternationalizationEnum.getMessageByCode(vo.getSuspensionReason()));
                vo.setProcessingMethod(InternationalizationEnum.getMessageByCode(vo.getProcessingMethod()));
            }
            return result;
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to query sync progress list", e);
            throw new BusinessException(DRPlatformErrorCodeEnum.GET_SYNC_PROGRESS_LIST_FAILED.getCode(),
                    DRPlatformErrorCodeEnum.GET_SYNC_PROGRESS_LIST_FAILED.getMessage());
        }
    }

    /** 重新同步数据库（全量重置） */
    @OperationLogAnnotation(type = "RESYNC", desc = "全量重置同步")
    @PostMapping("/resyncDatabases")
    public Result<Map<String, List<SyncProgressQuery>>> resyncDatabases(@RequestBody @Valid List<SyncProgressQuery> databaseList) {
        Map<String, List<SyncProgressQuery>> result;
        try {
            result = syncProgressService.resyncDatabases(databaseList);
        } catch (Exception e) {
            log.error("Failed to resync databases", e);
            throw new BusinessException(DRPlatformErrorCodeEnum.RESYNC_FAILED.getCode(),
                    DRPlatformErrorCodeEnum.RESYNC_FAILED.getMessage());
        }
        return Result.success(result);
    }

    /** 获取 IP 列表（生产中心 / 灾备中心） */
    @GetMapping("/ipList")
    public List<Map<String, String>> ipList() {
        List<Map<String, String>> result = new ArrayList<>();
        for (DatabaseMapping dbMapping : databaseSyncManager.getMappings()) {
            Map<String, String> sourceMap = new HashMap<>();
            sourceMap.put("ip", dbMapping.getSourceIP());
            sourceMap.put("type", InternationalizationEnum.translate(InternationalizationEnum.ACTIVE));
            Map<String, String> targetMap = new HashMap<>();
            targetMap.put("ip", dbMapping.getTargetIP());
            targetMap.put("type", InternationalizationEnum.translate(InternationalizationEnum.STANDBY));
            result.add(sourceMap);
            result.add(targetMap);
        }
        return result;
    }

    /**
     * 同步连接测试：对源库（生产中心）与目标库（灾备中心）服务端分别做连通性测试。
     * 不依赖具体库名，仅验证账号/密码/网络是否可达。返回两端结果便于页面定位问题。
     */
    @OperationLogAnnotation(type = "TEST_CONNECTION", desc = "测试数据库连接")
    @PostMapping("/mapping/test")
    public ResponseEntity<Result<Map<String, ConnectionTestResult>>> testMapping(@RequestBody @Valid SyncMappingRequestDTO dto) {
        DatabaseConfig sourceCfg = new DatabaseConfig(dto.getSourceHost(), dto.getSourcePort(),
                dto.getSourceUser(), dto.getSourcePassword(), "");
        DatabaseConfig targetCfg = new DatabaseConfig(dto.getTargetHost(), dto.getTargetPort(),
                dto.getTargetUser(), dto.getTargetPassword(), "");
        ConnectionTestResult source = sourceCfg.testServerConnection();
        ConnectionTestResult target = targetCfg.testServerConnection();
        Map<String, ConnectionTestResult> data = new LinkedHashMap<>();
        data.put("source", source);
        data.put("target", target);
        if (!source.isOk() || !target.isOk()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Result.error(DRPlatformErrorCodeEnum.DB_CONNECTION_TEST_FAILED.getCode(),
                            "连接测试未通过（源库:" + (source.isOk() ? "OK" : "失败") + "，目标库:" + (target.isOk() ? "OK" : "失败") + "）", data));
        }
        return ResponseEntity.ok(Result.success(data));
    }

    /**
     * 按源库连接信息列出可同步的数据库（服务端自动排除 MySQL 系统库）。
     * 供页面"忽略整库"多选下拉使用。
     */
    @PostMapping("/sourceDatabases")
    public Result<List<String>> sourceDatabases(@RequestBody SyncMappingRequestDTO dto) {
        // sourceDatabases 只需源库信息，手动校验源字段（不校验目标字段）
        if (dto == null || !StringUtils.hasText(dto.getSourceHost())) {
            throw new BusinessException(DRPlatformErrorCodeEnum.PARAM_ERROR.getCode(), "源主机不能为空");
        }
        if (!StringUtils.hasText(dto.getSourceUser())) {
            throw new BusinessException(DRPlatformErrorCodeEnum.PARAM_ERROR.getCode(), "源库账号不能为空");
        }
        if (!StringUtils.hasText(dto.getSourcePassword())) {
            throw new BusinessException(DRPlatformErrorCodeEnum.PARAM_ERROR.getCode(), "源库密码不能为空");
        }
        DatabaseConfig sourceCfg = new DatabaseConfig(dto.getSourceHost(), dto.getSourcePort(),
                dto.getSourceUser(), dto.getSourcePassword(), "");
        ConnectionTestResult sourceTest = sourceCfg.testServerConnection();
        if (!sourceTest.isOk()) {
            throw new BusinessException(DRPlatformErrorCodeEnum.DB_CONNECTION_TEST_FAILED.getCode(),
                    "源库连接测试未通过: " + sourceTest.getMessage());
        }
        List<String> dbs = databaseSyncManager.listSourceDatabases(sourceCfg);
        return Result.success(dbs);
    }

    /**
     * 按源库连接信息 + 库名，列出该库下所有用户表（服务端自动排除系统表 / 视图）。
     * 供页面"按库忽略表"多选下拉使用（下拉选择或手动正则均可基于该清单）。
     */
    @PostMapping("/sourceTables")
    public Result<List<String>> sourceTables(@RequestBody @Valid SourceTablesRequestDTO dto) {
        DatabaseConfig sourceCfg = new DatabaseConfig(dto.getSourceHost(), dto.getSourcePort(),
                dto.getSourceUser(), dto.getSourcePassword(), dto.getDatabase());
        List<String> tables = metadataService.listTables(sourceCfg, dto.getDatabase());
        return Result.success(tables);
    }

    /**
     * 页面批量新增同步映射：根据源/目标连接 + 选中的多个源库，扇出为多个映射并开始同步。
     * 支持忽略表与字段转换规则。已存在的映射自动跳过。
     */
    @OperationLogAnnotation(type = "MAPPING_ADD", desc = "新增同步映射")
    @PostMapping("/mapping/add")
    public Result<MappingBatchResult> addMapping(@RequestBody @Valid SyncMappingRequestDTO dto) {
        try {
            // 目标库连通性预检：不可达时直接拒绝，避免创建无效映射
            DatabaseConfig targetCfg = new DatabaseConfig(dto.getTargetHost(), dto.getTargetPort(),
                    dto.getTargetUser(), dto.getTargetPassword(), "");
            ConnectionTestResult targetTest = targetCfg.testServerConnection();
            if (!targetTest.isOk()) {
                throw new BusinessException(DRPlatformErrorCodeEnum.DB_CONNECTION_TEST_FAILED.getCode(),
                        "目标库连接测试未通过: " + targetTest.getMessage());
            }
            List<DatabaseMapping> mappings = dto.toMappings();
            if (mappings.isEmpty()) {
                throw new BusinessException(DRPlatformErrorCodeEnum.PARAM_ERROR.getCode(), "未解析到有效的同步主机对");
            }
            MappingBatchResult result = databaseSyncManager.addMappings(mappings);
            if (!result.getCreated().isEmpty()) {
                return Result.success(result);
            }
            // 全部跳过或全部失败
            if (!result.getFailed().isEmpty()) {
                throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(),
                        "同步映射创建失败：" + String.join("; ", result.getFailed()));
            }
            // 全部已存在
            return Result.error(DRPlatformErrorCodeEnum.MAPPING_ALREADY_EXISTS.getCode(),
                    "所选数据库均已存在同步映射（已跳过）", result);
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to add mappings", e);
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(),
                    DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getMessage());
        }
    }

    /** 查询当前所有已配置的同步映射（含 YAML 配置与页面动态新增，密码已脱敏） */
    @GetMapping("/mappings")
    public Result<List<DatabaseMappingVO>> listMappings() {
        // 按用户过滤：用户只能看到全局映射（yml 配置）和自己创建的映射
        Long userId = com.example.remotedatasync.common.UserContext.getUserId();
        return Result.success(databaseSyncManager.listMappingsVO(userId));
    }

    /** 移除页面动态新增的同步映射（停止作业并释放连接池） */
    @OperationLogAnnotation(type = "MAPPING_REMOVE", desc = "移除同步映射")
    @PostMapping("/mapping/remove")
    public Result<Void> removeMapping(@RequestBody Map<String, String> body) {
        String instanceKey = body == null ? null : body.get("instanceKey");
        if (instanceKey == null || instanceKey.isEmpty()) {
            throw new BusinessException(DRPlatformErrorCodeEnum.PARAM_ERROR.getCode(), "instanceKey 不能为空");
        }
        try {
            databaseSyncManager.removeMapping(instanceKey);
            return Result.success();
        } catch (IllegalStateException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("MAPPING_NOT_FOUND")) {
                throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_REMOVE_NOT_FOUND.getCode(),
                        DRPlatformErrorCodeEnum.MAPPING_REMOVE_NOT_FOUND.getMessage());
            }
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(), e.getMessage());
        } catch (Exception e) {
            log.error("Failed to remove mapping {}", instanceKey, e);
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(),
                    DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getMessage());
        }
    }

    /**
     * 重新加载指定主机对的配置并重建同步作业（配置热生效）。
     * <p>
     * 场景：通过 {@code /sync/mapping/add} 或直接修改 application.yml 后，
     * 需要让新的 ignoreDatabases / ignoreTables / transformRules 等配置立即生效，
     * 无需重启服务。调用此接口会停止旧作业、清理旧连接池、用最新 mapping 重建作业。
     * </p>
     * <p>
     * 注意：yml 中静态配置的 mapping 修改后，需配合 {@code /sync/mapping/update} 接口
     * 更新内存中的 mapping 对象，再调用本接口生效。动态新增的 mapping 可直接调用本接口。
     * </p>
     */
    @OperationLogAnnotation(type = "MAPPING_RELOAD", desc = "重载映射配置")
    @PostMapping("/mapping/reload")
    public Result<Map<String, Object>> reloadMapping(@RequestBody Map<String, String> body) {
        String instanceKey = body == null ? null : body.get("instanceKey");
        if (instanceKey == null || instanceKey.isEmpty()) {
            throw new BusinessException(DRPlatformErrorCodeEnum.PARAM_ERROR.getCode(), "instanceKey 不能为空");
        }
        try {
            boolean ok = databaseSyncManager.reloadMapping(instanceKey);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("instanceKey", instanceKey);
            data.put("reloaded", ok);
            if (ok) {
                return Result.success(data);
            }
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(),
                    "重新加载映射配置失败");
        } catch (IllegalStateException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("MAPPING_NOT_FOUND")) {
                throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_REMOVE_NOT_FOUND.getCode(),
                        DRPlatformErrorCodeEnum.MAPPING_REMOVE_NOT_FOUND.getMessage());
            }
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(), e.getMessage());
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to reload mapping {}", instanceKey, e);
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(),
                    DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getMessage());
        }
    }

    /**
     * 更新指定主机对的配置（运行时修改 ignoreDatabases / ignoreTables 等字段）。
     * <p>
     * 修改后需调用 {@code /sync/mapping/reload} 让新配置对同步作业生效。
     * 仅支持动态新增的 mapping（yml 配置的 mapping 需改 yml 后重启）。
     * </p>
     */
    @OperationLogAnnotation(type = "MAPPING_UPDATE", desc = "更新映射配置")
    @PostMapping("/mapping/update")
    public Result<Map<String, Object>> updateMapping(@RequestBody Map<String, Object> body) {
        String instanceKey = body == null ? null : (String) body.get("instanceKey");
        if (instanceKey == null || instanceKey.isEmpty()) {
            throw new BusinessException(DRPlatformErrorCodeEnum.PARAM_ERROR.getCode(), "instanceKey 不能为空");
        }
        try {
            boolean ok = databaseSyncManager.updateMappingConfig(instanceKey, body);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("instanceKey", instanceKey);
            data.put("updated", ok);
            return Result.success(data);
        } catch (IllegalStateException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("MAPPING_NOT_FOUND")) {
                throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_REMOVE_NOT_FOUND.getCode(),
                        DRPlatformErrorCodeEnum.MAPPING_REMOVE_NOT_FOUND.getMessage());
            }
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(), e.getMessage());
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to update mapping {}", instanceKey, e);
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(),
                    DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getMessage());
        }
    }

    /**
     * 主备切换：将指定主机对的同步源在「主库」与「备库」之间来回切换（需先配置 sourceStandbyHost）。
     * <p>
     * 该接口执行「源↔备源」地址交换，因此<b>可反复调用</b>：连续调用即可在主备之间往返任意多次。
     * 切换时自动对齐新源自增位点并进入删除对账静默期，保证数据绝对一致且不丢。
     * </p>
     */
    @OperationLogAnnotation(type = "MAPPING_SWITCH", desc = "主备切换源地址")
    @PostMapping("/mapping/switchSource")
    public Result<Map<String, Object>> switchSource(@RequestBody Map<String, String> body) {
        String instanceKey = body == null ? null : body.get("instanceKey");
        if (instanceKey == null || instanceKey.isEmpty()) {
            throw new BusinessException(DRPlatformErrorCodeEnum.PARAM_ERROR.getCode(), "instanceKey 不能为空");
        }
        try {
            boolean ok = databaseSyncManager.switchSourceToStandby(instanceKey);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("switched", ok);
            if (ok) {
                DatabaseMapping m = databaseSyncManager.findByKey(instanceKey);
                if (m != null) {
                    data.put("sourceHost", m.getSourceHost());
                    data.put("sourcePort", m.getSourcePort());
                    data.put("standbyHost", m.getSourceStandbyHost());
                    data.put("standbyPort", m.getSourceStandbyPort());
                }
            }
            return Result.success(data);
        } catch (IllegalStateException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("MAPPING_NOT_FOUND")) {
                throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_REMOVE_NOT_FOUND.getCode(),
                        DRPlatformErrorCodeEnum.MAPPING_REMOVE_NOT_FOUND.getMessage());
            }
            if (e.getMessage() != null && e.getMessage().startsWith("NO_STANDBY_CONFIGURED")) {
                throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(),
                        "该映射未配置备源地址(sourceStandbyHost)");
            }
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(), e.getMessage());
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to switch source for {}", instanceKey, e);
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(),
                    DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getMessage());
        }
    }

    /**
     * 生产中心 ↔ 灾备中心角色互换（灾备切生产 / 生产切灾备）。
     * <p>
     * 与 {@code /sync/mapping/switchSource}（同中心内部主库↔备库）不同，本接口交换的是
     * <b>同步方向</b>：原灾备中心提升为生产中心，原生产中心降级为灾备中心。
     * 连续调用两次回到初始方向，因此「灾备切生产」与「生产切灾备」共用这一个接口。
     * </p>
     */
    @OperationLogAnnotation(type = "MAPPING_REVERSE", desc = "生产/灾备中心角色互换")
    @PostMapping("/mapping/reverse")
    public Result<Map<String, Object>> reverseDirection(@RequestBody Map<String, String> body) {
        String instanceKey = body == null ? null : body.get("instanceKey");
        if (instanceKey == null || instanceKey.isEmpty()) {
            throw new BusinessException(DRPlatformErrorCodeEnum.PARAM_ERROR.getCode(), "instanceKey 不能为空");
        }
        try {
            boolean ok = databaseSyncManager.reverseDirection(instanceKey);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("reversed", ok);
            if (ok) {
                DatabaseMapping m = databaseSyncManager.findByKey(instanceKey);
                if (m == null) {
                    // 换向后 instanceKey 已变化，按新方向重新定位（端口信息随之互换）
                    m = databaseSyncManager.getMappings().stream()
                            .filter(x -> x.getLinkKey().equals(instanceKey)
                                    || instanceKey.equals(x.getHostOnlyKey()))
                            .findFirst().orElse(null);
                }
                if (m != null) {
                    data.put("instanceKey", m.getInstanceKey());
                    data.put("sourceHost", m.getSourceHost());
                    data.put("sourcePort", m.getSourcePort());
                    data.put("targetHost", m.getTargetHost());
                    data.put("targetPort", m.getTargetPort());
                }
            }
            return Result.success(data);
        } catch (IllegalStateException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("MAPPING_NOT_FOUND")) {
                throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_REMOVE_NOT_FOUND.getCode(),
                        DRPlatformErrorCodeEnum.MAPPING_REMOVE_NOT_FOUND.getMessage());
            }
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(), e.getMessage());
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to reverse direction for {}", instanceKey, e);
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(),
                    DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getMessage());
        }
    }

    /** 根据 IP 获取其下用户数据库列表 */
    @GetMapping("/databases/{ips}")
    public List<String> getDatabasesByIp(@PathVariable String ips) {
        return syncProgressService.getDatabasesByIp(ips);
    }

    /** 根据 ID 查询同步详情 */
    @GetMapping("/{id}")
    public Result<SyncProgressVO> getById(@PathVariable Long id) {
        SyncProgressVO result = syncProgressService.getById(id);
        if (result == null) {
            throw new BusinessException(DRPlatformErrorCodeEnum.DATA_NOT_FOUND.getCode(),
                    DRPlatformErrorCodeEnum.DATA_NOT_FOUND.getMessage());
        }
        result.setSuspensionReason(InternationalizationEnum.getMessageByCode(result.getSuspensionReason()));
        result.setProcessingMethod(InternationalizationEnum.getMessageByCode(result.getProcessingMethod()));
        return Result.success(result);
    }

    /** 获取同步总状态 */
    @GetMapping("/status")
    public Map<String, Object> status() {
        try {
            return syncProgressService.status();
        } catch (Exception e) {
            log.error("Failed to retrieve overall sync status", e);
            DRPlatformErrorCodeEnum.throwException(DRPlatformErrorCodeEnum.GET_SYNC_PROGRESS_ALL_STATE_FAILED);
            return new HashMap<>();
        }
    }

    // ===================== Flink CDC 实时状态 / 运维 =====================

    /**
     * 返回所有主机对的 Flink CDC 作业实时状态：运行态、jobId、最近 savepoint、
     * 实时同步延迟（lagSeconds）、是否超阈值（lagging）、累计应用条数。
     * 前端据此展示「实时同步延迟 < 10s」达标情况。
     */
    @GetMapping("/cdc/jobs")
    public Result<List<Map<String, Object>>> cdcJobs() {
        try {
            if (!cdcJobManager.isCdcMode()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("mode", "polling");
                m.put("cdcEnabled", false);
                return Result.success(List.of(m));
            }
            return Result.success(cdcJobManager.allStatus(databaseSyncManager.getMappings()));
        } catch (Exception e) {
            log.error("Failed to list CDC jobs", e);
            throw new BusinessException(DRPlatformErrorCodeEnum.GET_SYNC_PROGRESS_LIST_FAILED.getCode(),
                    "获取 CDC 作业状态失败: " + e.getMessage());
        }
    }

    /**
     * 手动触发 savepoint（不停机打点，归档当前 binlog 位点）。
     * 用于计划内维护前的位点备份，与 @PreDestroy / 移除映射的 stopWithSavepoint 互补。
     */
    @OperationLogAnnotation(type = "CDC_SAVEPOINT", desc = "触发 CDC savepoint")
    @PostMapping("/cdc/savepoint/{instanceKey}")
    public Result<Map<String, Object>> triggerCdcSavepoint(@PathVariable String instanceKey) {
        try {
            String path = cdcJobManager.triggerSavepoint(instanceKey);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("instanceKey", instanceKey);
            m.put("savepointPath", path);
            return Result.success(m);
        } catch (IllegalStateException e) {
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(), e.getMessage());
        } catch (Exception e) {
            log.error("Failed to trigger CDC savepoint for {}", instanceKey, e);
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(),
                    "触发 CDC savepoint 失败: " + e.getMessage());
        }
    }

    /**
     * 停止指定主机对 CDC 作业并打 savepoint（优雅停机；重启后由元库记录的位点恢复）。
     */
    @OperationLogAnnotation(type = "CDC_STOP", desc = "停止 CDC 作业并打 savepoint")
    @PostMapping("/cdc/stop/{instanceKey}")
    public Result<Map<String, Object>> stopCdcJob(@PathVariable String instanceKey) {
        try {
            String path = cdcJobManager.stopWithSavepoint(instanceKey);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("instanceKey", instanceKey);
            m.put("savepointPath", path);
            m.put("stopped", true);
            return Result.success(m);
        } catch (Exception e) {
            log.error("Failed to stop CDC job for {}", instanceKey, e);
            throw new BusinessException(DRPlatformErrorCodeEnum.MAPPING_ADD_FAILED.getCode(),
                    "停止 CDC 作业失败: " + e.getMessage());
        }
    }
}
