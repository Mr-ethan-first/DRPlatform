package com.example.remotedatasync.sink;

import java.time.LocalDateTime;

/**
 * 落库确认回调：表级 Sink 在目标库批次事务<b>成功提交</b>后调用，告知同步引擎
 * 「该批数据已持久化，可将水位推进到 (maxWatermarkTime, lastWatermarkKey)」。
 * <p>
 * 这是保证「水位仅越过已落库数据」的核心机制：只有收到 ack，引擎才推进内存水位并持久化，
 * 从而确保断电 / 崩溃 / 目标库不可用期间，水位绝不越过未落库的事件 → at-least-once 不丢。
 * </p>
 *
 * @author 50707
 */
public interface WatermarkAck {

    /**
     * 通知某表一批数据已成功落库。
     *
     * @param db                 源库名
     * @param table              表名
     * @param maxWatermarkTime   本批已落行的最大 update_time（null 表示该批无增量行，如纯 DELETE，不推进水位）
     * @param lastWatermarkKey   与 maxWatermarkTime 配对的主键锚点（keyset 续扫点）
     */
    void ack(String db, String table, LocalDateTime maxWatermarkTime, Object lastWatermarkKey);
}
