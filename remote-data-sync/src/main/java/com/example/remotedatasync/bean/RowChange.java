package com.example.remotedatasync.bean;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 单行变更事件（CDC 事件的本地等价物）。
 * <p>
 * 在本地可运行版本中，它由内嵌同步引擎从源库扫描产生，交由 TableSinkFunction 写入目标库。
 * 对应生产环境的 Flink CDC 算子事件。
 * </p>
 *
 * @author 50707
 */
@Data
public class RowChange implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 操作类型：I(insert) / U(update) / D(delete) */
    public enum OpType {
        INSERT, UPDATE, DELETE
    }

    private OpType opType;
    private String tableName;
    /** 变更后的完整行数据（删除时为 null） */
    private Map<String, Object> after = new LinkedHashMap<>();
    /** 主键列名（用于幂等 upsert 与分区路由） */
    private List<String> primaryKeyColumns;

    /**
     * 本行续扫锚点：update_time（用于增量水位推进）。
     * 由同步引擎在扫描源库时按行填充，随事件流经 Sink，落库成功后回灌为水位。
     */
    private transient LocalDateTime watermarkTime;
    /**
     * 本行续扫锚点：主键列的值（与 watermarkTime 构成 keyset 续扫点）。
     */
    private transient Object watermarkKey;

    public RowChange() {
    }

    public RowChange(OpType opType, String tableName, Map<String, Object> after, List<String> primaryKeyColumns) {
        this.opType = opType;
        this.tableName = tableName;
        this.after = after == null ? new LinkedHashMap<>() : after;
        this.primaryKeyColumns = primaryKeyColumns;
    }

    /** 从 after 中按主键列抽取主键值，用于构建 upsert 条件 */
    public Object getPrimaryKeyValue() {
        if (primaryKeyColumns == null || primaryKeyColumns.isEmpty() || after == null) {
            return null;
        }
        return after.get(primaryKeyColumns.get(0));
    }
}
