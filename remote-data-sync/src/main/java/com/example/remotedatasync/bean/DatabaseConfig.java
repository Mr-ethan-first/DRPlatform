package com.example.remotedatasync.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * 数据库连接配置（源库 / 目标库通用）。
 *
 * @author 50707
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DatabaseConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String host;
    private int port;
    private String username;
    private String password;
    private String databaseName;

    public String getDBURL() {
        return String.format(
                "jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true&useSSL=false&rewriteBatchedStatements=true&connectTimeout=10000&socketTimeout=60000",
                host, port, databaseName == null ? "" : databaseName);
    }

    /** 仅含主机端口的库无关 URL，用于连接 information_schema 等系统库 */
    public String getServerURL() {
        return String.format(
                "jdbc:mysql://%s:%d/?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=10000&socketTimeout=60000",
                host, port);
    }

    /**
     * 校验数据库连接是否可用。
     *
     * @return 可用返回 true，否则 false
     */
    public Boolean checkDBConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Properties props = new Properties();
            props.setProperty("user", username);
            props.setProperty("password", password);
            props.setProperty("connectTimeout", "5000");
            try (Connection conn = DriverManager.getConnection(getDBURL(), props)) {
                return conn.isValid(3);
            }
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 执行连接测试，并返回可读的成功/失败结果（含失败原因）。
     * 供页面"同步测试"功能使用，便于精确定位连接问题。
     *
     * @return 连接测试结果
     */
    public com.example.remotedatasync.common.ConnectionTestResult testConnection() {
        String url = getDBURL();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Properties props = new Properties();
            props.setProperty("user", username);
            props.setProperty("password", password);
            props.setProperty("connectTimeout", "5000");
            props.setProperty("socketTimeout", "5000");
            try (Connection conn = DriverManager.getConnection(url, props)) {
                boolean valid = conn.isValid(3);
                if (valid) {
                    return com.example.remotedatasync.common.ConnectionTestResult.ok(url);
                }
                return com.example.remotedatasync.common.ConnectionTestResult.fail(url, "连接已建立但 isValid() 返回 false");
            }
        } catch (Exception e) {
            return com.example.remotedatasync.common.ConnectionTestResult.fail(url, e.getMessage());
        }
    }

    /**
     * 仅测试服务端连通性（不指定具体库），用于"连接测试"按钮与"加载源库列表"。
     * 使用库无关 URL（getServerURL），避免因库名缺失而误报失败。
     *
     * @return 连接测试结果
     */
    public com.example.remotedatasync.common.ConnectionTestResult testServerConnection() {
        String url = getServerURL();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Properties props = new Properties();
            props.setProperty("user", username);
            props.setProperty("password", password);
            props.setProperty("connectTimeout", "5000");
            props.setProperty("socketTimeout", "5000");
            try (Connection conn = DriverManager.getConnection(url, props)) {
                boolean valid = conn.isValid(3);
                if (valid) {
                    return com.example.remotedatasync.common.ConnectionTestResult.ok(url);
                }
                return com.example.remotedatasync.common.ConnectionTestResult.fail(url, "连接已建立但 isValid() 返回 false");
            }
        } catch (Exception e) {
            return com.example.remotedatasync.common.ConnectionTestResult.fail(url, e.getMessage());
        }
    }

    /** 浅拷贝，避免同步任务间共享可变状态 */
    public DatabaseConfig copy() {
        DatabaseConfig c = new DatabaseConfig();
        c.host = this.host;
        c.port = this.port;
        c.username = this.username;
        c.password = this.password;
        c.databaseName = this.databaseName;
        return c;
    }
}
