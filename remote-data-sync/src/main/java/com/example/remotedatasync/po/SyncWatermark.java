package com.example.remotedatasync.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 同步水位表 sync_watermark：每张表「已确认落库」的最大 (update_time, 主键) 水位。
 * <p>
 * 由同步引擎在目标库批次事务提交成功后推进并持久化，是断点续传与崩溃恢复的核心。
 * 水位语义：仅当目标库写入<b>成功提交</b>后才推进 → 绝不会越过未落库的数据 → 保证 at-least-once 不丢。
 * </p>
 *
 * @author 50707
 */
@Data
@NoArgsConstructor
@TableName("sync_watermark")
public class SyncWatermark implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 生产中心（源）IP */
    private String sourceIp;

    /** 源库名 */
    private String sourceDb;

    /** 源表名 */
    private String tableName;

    /** 已确认落库的最大 update_time */
    private LocalDateTime watermarkTime;

    /** 续扫主键锚点（末位行主键值，与 watermarkTime 构成 keyset 续扫点；数字会还原为 Long） */
    private String watermarkKey;

    private LocalDateTime updateTime;

    /** 内存续扫锚点（不持久化）：还原为原始类型后用于 (update_time, pk) 单调比较 */
    private transient Object watermarkKeyObj;

    /** 持久化键：sourceIp|sourceDb.tableName */
    public String getKey() {
        return sourceIp + "|" + sourceDb + "." + tableName;
    }
}
