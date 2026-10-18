package com.example.remotedatasync.service.impl;

import com.example.remotedatasync.mapper.SyncWatermarkMapper;
import com.example.remotedatasync.po.SyncWatermark;
import com.example.remotedatasync.service.SyncWatermarkService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 同步水位服务实现。
 *
 * @author 50707
 */
@Slf4j
@Service
public class SyncWatermarkServiceImpl implements SyncWatermarkService {

    private final SyncWatermarkMapper watermarkMapper;

    public SyncWatermarkServiceImpl(SyncWatermarkMapper watermarkMapper) {
        this.watermarkMapper = watermarkMapper;
    }

    @Override
    public Map<String, SyncWatermark> loadAll() {
        Map<String, SyncWatermark> result = new HashMap<>();
        try {
            List<SyncWatermark> all = watermarkMapper.selectAll();
            for (SyncWatermark w : all) {
                // 还原 watermark_key 为原始数值类型（若可解析为长整型），保证 (update_time, pk) 单调比较正确
                if (w.getWatermarkKey() != null) {
                    try {
                        w.setWatermarkKeyObj(Long.parseLong(w.getWatermarkKey().trim()));
                    } catch (NumberFormatException e) {
                        w.setWatermarkKeyObj(w.getWatermarkKey());
                    }
                }
                result.put(w.getKey(), w);
            }
            log.info("Loaded {} persisted watermark(s) for crash recovery", result.size());
        } catch (Exception e) {
            // 表可能尚未初始化：降级为无水位（各表走全量重扫一次，幂等不丢）
            log.warn("Failed to load watermarks (table may not exist yet), fall back to full rescan: {}", e.getMessage());
        }
        return result;
    }

    @Override
    public void saveBatch(List<SyncWatermark> list) {
        if (list == null || list.isEmpty()) {
            return;
        }
        try {
            watermarkMapper.upsertBatch(list);
        } catch (Exception e) {
            log.warn("Failed to persist {} watermark(s): {}", list.size(), e.getMessage());
        }
    }

    @Override
    public void delete(String sourceIp, String sourceDb, String tableName) {
        if (sourceIp == null || sourceDb == null || tableName == null) {
            return;
        }
        try {
            int n = watermarkMapper.deleteByKey(sourceIp, sourceDb, tableName);
            log.info("Deleted persisted watermark for {}.{} ({} row(s)) -> forces full rescan after restart",
                    sourceDb, tableName, n);
        } catch (Exception e) {
            log.warn("Failed to delete watermark for {}.{}: {}", sourceDb, tableName, e.getMessage());
        }
    }

    @Override
    public void deleteBySource(String sourceIp) {
        if (sourceIp == null) {
            return;
        }
        try {
            int n = watermarkMapper.deleteBySourceIp(sourceIp);
            log.info("Deleted {} persisted watermark(s) of source {} -> forces full rescan after switch",
                    n, sourceIp);
        } catch (Exception e) {
            log.warn("Failed to delete watermarks of source {}: {}", sourceIp, e.getMessage());
        }
    }
}
