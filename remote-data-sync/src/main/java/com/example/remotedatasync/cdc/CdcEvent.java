package com.example.remotedatasync.cdc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Flink CDC（Debezium JSON）事件模型。
 *
 * <p>数据变更记录形如：
 * <pre>
 * {"before":{...}|null,"after":{...}|null,"source":{"db":"d","table":"t","file":"mysql-bin.000003","pos":123,"ts_ms":1699999999000},"op":"c|u|d|r","ts_ms":...}
 * </pre>
 * 结构变更（DDL）记录形如：
 * <pre>
 * {"databaseName":"d","tableChanges":[...],"ddl":"CREATE TABLE `d`.`t` (...)","source":{...}}
 * </pre>
 *
 * @author 50707
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CdcEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 操作类型：c=insert, u=update, d=delete, r=快照读取 */
    private String op;

    private Map<String, Object> before;

    private Map<String, Object> after;

    private Source source;

    private Long tsMs;

    /** DDL 文本（仅结构变更事件有值） */
    private String ddl;

    /** DDL 所属库（仅结构变更事件有值） */
    private String databaseName;

    /** DDL 涉及的表（从 tableChanges 提取，库级 DDL 为 null） */
    private String ddlTable;

    /**
     * flink-cdc 3.x binlog 阶段的 schema-change 记录格式：
     * DDL 文本不在外层，而是以 JSON 字符串嵌在 historyRecord 字段
     * （形如 {"source":{...},"historyRecord":"{\"source\":..,\"ddl\":\"ALTER ...\"}"}）。
     * 由 {@link JsonToCdcEventFunction} 解包后回填 ddl/databaseName。
     */
    @JsonProperty("historyRecord")
    private String historyRecord;

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Long getTsMs() {
        return tsMs;
    }

    public void setTsMs(Long tsMs) {
        this.tsMs = tsMs;
    }

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDdlTable() {
        return ddlTable;
    }

    public void setDdlTable(String ddlTable) {
        this.ddlTable = ddlTable;
    }

    public String getHistoryRecord() {
        return historyRecord;
    }

    public void setHistoryRecord(String historyRecord) {
        this.historyRecord = historyRecord;
    }

    // ===================== 便捷方法 =====================

    /** 是否结构变更（DDL）事件 */
    public boolean isSchemaChange() {
        return ddl != null && !ddl.isEmpty();
    }

    /** 是否数据变更事件 */
    public boolean isDataChange() {
        return op != null && source != null;
    }

    /** 目标库名 */
    public String db() {
        return source != null ? source.getDb() : databaseName;
    }

    /** 目标表名 */
    public String table() {
        return source != null ? source.getTable() : ddlTable;
    }

    /**
     * keyBy 键：表级 DDL 与行事件同键（保证同一表内 DDL 与数据的相对顺序）；
     * 库级 DDL（无表）按库键。
     */
    public String tableKey() {
        if (isSchemaChange()) {
            return ddlTable != null ? db() + "." + ddlTable : db();
        }
        return db() + "." + table();
    }

    /** 事件在源端的提交时间（毫秒），用于延迟统计 */
    public long eventTimeMs() {
        if (tsMs != null) {
            return tsMs;
        }
        return source != null && source.getTsMs() != null ? source.getTsMs() : 0L;
    }

    /** binlog 位置（文件:位置），用于日志与排查 */
    public String binlogPosition() {
        if (source == null) {
            return "n/a";
        }
        return source.getFile() + ":" + source.getPos();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Source implements Serializable {

        private static final long serialVersionUID = 1L;

        private String db;

        private String table;

        private String file;

        private Long pos;

        @JsonProperty("ts_ms")
        private Long tsMs;

        private String connector;

        private String name;

        public String getDb() {
            return db;
        }

        public void setDb(String db) {
            this.db = db;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getFile() {
            return file;
        }

        public void setFile(String file) {
            this.file = file;
        }

        public Long getPos() {
            return pos;
        }

        public void setPos(Long pos) {
            this.pos = pos;
        }

        public Long getTsMs() {
            return tsMs;
        }

        public void setTsMs(Long tsMs) {
            this.tsMs = tsMs;
        }

        public String getConnector() {
            return connector;
        }

        public void setConnector(String connector) {
            this.connector = connector;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    /** 供测试/日志使用：行内容摘要 */
    public static String rowSummary(Map<String, Object> row) {
        if (row == null) {
            return "null";
        }
        Map<String, Object> copy = new LinkedHashMap<>(row);
        if (copy.size() > 8) {
            return copy.toString().substring(0, Math.min(200, copy.toString().length())) + "...";
        }
        return copy.toString();
    }
}
