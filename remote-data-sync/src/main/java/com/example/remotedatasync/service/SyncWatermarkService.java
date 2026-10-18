package com.example.remotedatasync.service;

import com.example.remotedatasync.po.SyncWatermark;

import java.util.List;
import java.util.Map;

/**
 * 同步水位服务：断点续传与崩溃恢复的核心。
 * <p>
 * 加载全部已确认水位（崩溃恢复）、批量持久化水位快照。
 * </p>
 *
 * @author 50707
 */
public interface SyncWatermarkService {

    /**
     * 加载全部已确认水位。
     *
     * @return key = sourceIp|sourceDb.tableName -> 水位对象（watermarkKeyObj 已还原为原始类型）
     */
    Map<String, SyncWatermark> loadAll();

    /** 批量持久化水位快照（upsert，幂等） */
    void saveBatch(List<SyncWatermark> list);

    /**
     * 删除指定「源+库+表」的持久化水位行（DDL 变更 / 主备切换前调用）。
     * <p>
     * 目的：让"需要全量重扫"这一事实<b>立即落盘</b>，而不是只清内存。
     * 若仅清内存而未删持久化行，一旦进程在目标表被 DROP/重建过程中崩溃，
     * 重启后会加载到<b>旧水位</b>从而走增量续传，而目标表此时已被清空 → 历史数据永久缺失。
     * 删除持久化行后，任何中断点重启都会退化为全量重扫（幂等、可恢复）。
     * </p>
     */
    void delete(String sourceIp, String sourceDb, String tableName);

    /**
     * 删除某源下全部持久化水位（主备切换后调用，强制对新源做一次全量重扫）。
     * <p>
     * 原因：水位表以 {@code SOURCE_IP} 为键、不含端口，主备同主机不同端口切换时会<b>复用旧水位</b>，
     * 导致新源中 {@code update_time ≤ 旧水位} 的行被跳过。若新源并非旧源的严格超集（例如备库存在
     * 独有行、或异步复制尚未追平），这些行将永远无法同步。切换后清空水位 → 退化为全量重扫，
     * 由幂等 upsert 保证不重不丢。
     * </p>
     */
    void deleteBySource(String sourceIp);
}
