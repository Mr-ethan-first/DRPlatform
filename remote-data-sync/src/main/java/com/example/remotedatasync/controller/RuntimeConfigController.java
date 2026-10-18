package com.example.remotedatasync.controller;

import com.example.remotedatasync.common.BusinessException;
import com.example.remotedatasync.common.DRPlatformErrorCodeEnum;
import com.example.remotedatasync.common.Result;
import com.example.remotedatasync.config.RuntimeConfigService;
import com.example.remotedatasync.cdc.FlinkCdcJobManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 同步引擎运行时动态配置 REST 接口。
 * <p>
 * 用途：不同性能的服务器选择不同档位参数，实现「服务器性能越好，同步越快」。
 * 所有参数经服务端强校验（checkpoint 间隔禁止停用等数据丢失 0 容忍约束内建）。
 * </p>
 *
 * @author 50707
 */
@Slf4j
@RestController
@RequestMapping("/sync/runtime-config")
@Validated
public class RuntimeConfigController {

    private final RuntimeConfigService runtimeConfigService;
    private final FlinkCdcJobManager cdcJobManager;

    public RuntimeConfigController(RuntimeConfigService runtimeConfigService,
                                   FlinkCdcJobManager cdcJobManager) {
        this.runtimeConfigService = runtimeConfigService;
        this.cdcJobManager = cdcJobManager;
    }

    /** 查看当前生效参数 + 合法区间 + 档位预设 */
    @GetMapping
    public Result<Map<String, Object>> get() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("effective", runtimeConfigService.snapshot());
        m.put("bounds", runtimeConfigService.bounds());
        m.put("presets", runtimeConfigService.presets());
        m.put("note", "POST 修改后对新启动/重启的作业生效；传 apply=true 可对运行中作业做 savepoint 无损热应用");
        return Result.success(m);
    }

    /**
     * 修改运行时参数。
     *
     * @param body  参数名 -> 新值（只传需要修改的字段；整体校验，任一非法则全部不生效）
     * @param apply true 时对全部运行中作业执行「savepoint 停止 → savepoint 恢复」，无损切换到新参数
     */
    @PostMapping
    public Result<Map<String, Object>> update(@RequestBody Map<String, Object> body,
                                              @RequestParam(defaultValue = "false") boolean apply) {
        java.util.List<String> changed;
        try {
            changed = runtimeConfigService.applyOverrides(body);
        } catch (IllegalArgumentException e) {
            throw new BusinessException(DRPlatformErrorCodeEnum.PARAM_ERROR.getCode(), e.getMessage());
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("changed", changed);
        resp.put("effective", runtimeConfigService.snapshot());
        if (apply) {
            Map<String, String> jobs = cdcJobManager.applyRuntimeConfig();
            resp.put("jobResults", jobs);
            log.info("Runtime config applied to running jobs: {}", jobs);
        }
        return Result.success(resp);
    }

    /** 一键应用档位预设（如 large_8c16g），配合 apply=true 立即生效 */
    @PostMapping("/preset/{name}")
    public Result<Map<String, Object>> applyPreset(@PathVariable String name,
                                                   @RequestParam(defaultValue = "false") boolean apply) {
        Map<String, Map<String, Object>> presets = runtimeConfigService.presets();
        Map<String, Object> preset = presets.get(name);
        if (preset == null) {
            throw new BusinessException(DRPlatformErrorCodeEnum.PARAM_ERROR.getCode(),
                    "未知档位: " + name + "，可选: " + presets.keySet());
        }
        java.util.List<String> changed = runtimeConfigService.applyOverrides(preset);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("preset", name);
        resp.put("changed", changed);
        resp.put("effective", runtimeConfigService.snapshot());
        if (apply) {
            resp.put("jobResults", cdcJobManager.applyRuntimeConfig());
        }
        return Result.success(resp);
    }
}
