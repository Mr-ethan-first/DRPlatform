package com.example.remotedatasync.cdc;

import com.example.remotedatasync.bean.DatabaseMapping;
import com.example.remotedatasync.util.MatchPattern;

import java.io.Serializable;
import java.util.List;

/**
 * CDC 事件过滤器：复用现有 {@link MatchPattern}（精确 / glob / re: 正则）语义，
 * 把映射配置中的 ignore-databases / ignore-tables / ignore-ddl-tables 应用到 binlog 事件流。
 *
 * @author 50707
 */
public class CdcFilter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> ignoreDatabases;

    private final List<String> ignoreTables;

    private final List<String> ignoreDdlTables;

    public CdcFilter(DatabaseMapping mapping) {
        this.ignoreDatabases = mapping.getIgnoreDatabases();
        this.ignoreTables = mapping.getIgnoreTables();
        this.ignoreDdlTables = mapping.getIgnoreDdlTables();
    }

    /** 库是否应同步（不在 ignore-databases 中） */
    public boolean acceptDatabase(String db) {
        return !MatchPattern.matchesAny(ignoreDatabases, db);
    }

    /** 表是否应同步数据（不在 ignore-tables 中） */
    public boolean acceptTable(String db, String table) {
        return !MatchPattern.matchesTable(ignoreTables, db, table);
    }

    /** 表是否应同步 DDL（不在 ignore-ddl-tables 中） */
    public boolean acceptDdl(String db, String table) {
        return !MatchPattern.matchesTable(ignoreDdlTables, db, table);
    }

    /**
     * 事件是否应处理（库级 + 表级过滤；DDL 事件额外受 ignore-ddl-tables 约束）。
     */
    public boolean accept(CdcEvent event) {
        String db = event.db();
        if (!acceptDatabase(db)) {
            return false;
        }
        if (event.isSchemaChange()) {
            if (event.getDdlTable() == null) {
                return true; // 库级 DDL（如 CREATE DATABASE）
            }
            return acceptTable(db, event.getDdlTable()) && acceptDdl(db, event.getDdlTable());
        }
        return acceptTable(db, event.table());
    }
}
