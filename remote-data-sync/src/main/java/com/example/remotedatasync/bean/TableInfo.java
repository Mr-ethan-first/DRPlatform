package com.example.remotedatasync.bean;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 表元信息（表名、主键列、全部列）。
 *
 * @author 50707
 */
@Data
public class TableInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 表名 */
    private String tableName;

    /** 主键列名（用于分区路由与幂等 upsert） */
    private List<String> primaryKeyColumns = new ArrayList<>();

    /** 全部列名 */
    private List<String> columns = new ArrayList<>();

    /**
     * 列定义签名：列名 -> 归一化类型/长度/可空性描述（按列名排序后拼接）。
     * <p>用于 DDL 结构变更检测：仅比较列名无法发现
     * {@code VARCHAR(64)->VARCHAR(128)}、{@code INT->BIGINT}、{@code NULL->NOT NULL}
     * 这类「列集合未变但定义已变」的变更，必须带上类型信息才能识别。
     */
    private String columnDefs = "";

    /** 是否为新增表（CREATE TABLE 触发后标记） */
    private boolean isNew = false;

    public TableInfo() {
    }

    public TableInfo(String tableName, List<String> primaryKeyColumns, List<String> columns) {
        this.tableName = tableName;
        this.primaryKeyColumns = primaryKeyColumns == null ? new ArrayList<>() : primaryKeyColumns;
        this.columns = columns == null ? new ArrayList<>() : columns;
    }
}
