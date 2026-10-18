package com.example.remotedatasync.job;

import com.example.remotedatasync.bean.DatabaseMapping;
import com.example.remotedatasync.config.DRPlatformProperties;
import com.example.remotedatasync.manager.DatabaseSyncManager;
import com.example.remotedatasync.mapper.DbhaSwitchStatusMapper;
import com.example.remotedatasync.po.DbhaSwitchStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 主备切换状态扫描任务：监听 dbha_switch_status 表变化，可触发同步作业平滑重建。
 * <p>
 * 当 drplatform.ha.switch-auto-resync=true 时，检测到主备切换记录（mainIp->standbyIp）后，
 * 自动将对应映射的同步源切到备库并重建作业（幂等全量重扫，数据一致不丢）。
 * 默认关闭，避免对自动化测试造成干扰；生产环境可开启，或由人工通过 /sync/mapping/switchSource 触发。
 * </p>
 *
 * @author 50707
 */
@Slf4j
@Component
public class DBHASwitchStatusJob {

    private final DbhaSwitchStatusMapper switchStatusMapper;
    private final DatabaseSyncManager databaseSyncManager;
    private final DRPlatformProperties properties;

    public DBHASwitchStatusJob(DbhaSwitchStatusMapper switchStatusMapper, DatabaseSyncManager databaseSyncManager,
                               DRPlatformProperties properties) {
        this.switchStatusMapper = switchStatusMapper;
        this.databaseSyncManager = databaseSyncManager;
        this.properties = properties;
    }

    @Scheduled(fixedDelay = 15000)
    public void scanSwitchStatus() {
        try {
            List<DbhaSwitchStatus> list = switchStatusMapper.selectList(null);
            if (list == null || list.isEmpty()) {
                return;
            }
            for (DbhaSwitchStatus s : list) {
                log.info("Detected HA switch for db={}, mainIp={} -> standbyIp={}",
                        s.getSourceDbName(), s.getMainIp(), s.getStandbyIp());
                if (!properties.getHa().isSwitchAutoResync()) {
                    continue; // 默认关闭自动切换，仅记录，交由人工 / API 触发
                }
                DatabaseMapping m = databaseSyncManager.findMappingByIp(s.getMainIp());
                if (m == null || s.getStandbyIp() == null) {
                    continue;
                }
                // 将备源地址写入映射并切换（切换方法内部会停止旧作业、重定向源地址、重建作业）
                m.setSourceStandbyHost(s.getStandbyIp());
                m.setSourceStandbyPort(m.getSourcePort());
                try {
                    databaseSyncManager.switchSourceToStandby(m.getInstanceKey());
                } catch (Exception e) {
                    log.warn("Auto switch failed for mainIp={}", s.getMainIp(), e);
                }
            }
        } catch (Exception e) {
            log.warn("DBHASwitchStatusJob failed", e);
        }
    }
}
