package com.example.remotedatasync.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Map;

/**
 * Debezium JSON 字符串 → {@link CdcEvent}。
 *
 * <p>数据变更与结构变更（DDL）两种记录都由本函数统一解析，
 * 下游按 {@link CdcEvent#tableKey()} keyBy 后进入 sink。
 *
 * @author 50707
 */
public class JsonToCdcEventFunction implements MapFunction<String, CdcEvent> {

    private static final long serialVersionUID = 1L;

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(JsonToCdcEventFunction.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public CdcEvent map(String json) throws Exception {
        if (json == null || json.isEmpty()) {
            return null;
        }
        // 诊断日志：打印原始 JSON（截断），用于核对 schema-change 事件结构
        log.debug("[CDC] raw json: {}", json.length() > 400 ? json.substring(0, 400) + "..." : json);
        try {
            CdcEvent event = MAPPER.readValue(json, CdcEvent.class);
            if (event == null) {
                return null;
            }
            // flink-cdc 3.x binlog 阶段的 schema-change 记录：DDL 嵌在 historyRecord 字符串里，需解包
            if (event.getDdl() == null && event.getHistoryRecord() != null) {
                unwrapHistoryRecord(event);
            }
            if (event.isSchemaChange() && event.getDdlTable() == null) {
                event.setDdlTable(extractTable(event.getDdl()));
            }
            return event;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse CDC event json: "
                    + (json.length() > 200 ? json.substring(0, 200) + "..." : json), e);
        }
    }

    /**
     * 从嵌套的 historyRecord JSON 字符串中提取 ddl / databaseName：
     * 形如 {@code {"source":{...},"position":{...},"databaseName":"db","ddl":"ALTER ...","tableChanges":[...]}}
     */
    @SuppressWarnings("unchecked")
    private static void unwrapHistoryRecord(CdcEvent event) throws Exception {
        Map<String, Object> hist = MAPPER.readValue(event.getHistoryRecord(), Map.class);
        Object ddl = hist.get("ddl");
        if (ddl instanceof String) {
            event.setDdl((String) ddl);
        }
        // 外层 source.db 为空时回退到 historyRecord.databaseName
        if ((event.db() == null || event.db().isEmpty()) && hist.get("databaseName") instanceof String) {
            event.setDatabaseName((String) hist.get("databaseName"));
        }
    }

    /**
     * 从 DDL 文本提取表名（Debezium 的 tableChanges 结构不直接反序列化，改用语句文本提取，
     * 覆盖 CREATE/ALTER/DROP/TRUNCATE/RENAME TABLE 等常见形态）。
     */
    static String extractTable(String ddl) {
        if (ddl == null) {
            return null;
        }
        String s = ddl.trim();
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                        "(?i)(?:CREATE|ALTER|DROP|TRUNCATE|RENAME)\\s+(?:TABLE\\s+)?(?:IF\\s+NOT\\s+EXISTS\\s+)?`?([\\w$]+)`?(?:\\s*\\.\\s*`?([\\w$]+)`?)?")
                .matcher(s);
        if (m.find()) {
            // 带库限定符时取第二组，否则第一组
            return m.group(2) != null ? m.group(2) : m.group(1);
        }
        return null;
    }
}
