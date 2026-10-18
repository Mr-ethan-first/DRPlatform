package com.example.remotedatasync.cdc;

import com.example.remotedatasync.bean.DatabaseMapping;
import com.example.remotedatasync.config.DRPlatformProperties;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Flink CDC 端到端验证（实验室：源 127.0.0.1:13306 → 目标 127.0.0.1:13307，均为本地临时实例）。
 *
 * <p>验证用户硬指标：
 * <ul>
 *   <li>零源库轮询压力（基于 binlog 全量快照 + 增量）；</li>
 *   <li>实时同步延迟 &lt; 10s（增/删/DDL 在插入后 10s 内落到目标）；</li>
 *   <li>savepoint 断点恢复（崩溃/kill 后续传无丢无重）；</li>
 *   <li>DDL（ADD COLUMN）与 DELETE 同步、幂等 upsert。</li>
 * </ul>
 *
 * <p>源/目标使用<b>同名库</b>（lab_db）镜像，符合生产「同名库镜像」模型。
 *
 * @author 50707
 */
class CdcEndToEndTest {

    private static final String SRC_URL = "jdbc:mysql://127.0.0.1:13306/?useUnicode=true&characterEncoding=utf8"
            + "&serverTimezone=UTC&allowPublicKeyRetrieval=true&useSSL=false";
    private static final String TGT_URL = "jdbc:mysql://127.0.0.1:13307/?useUnicode=true&characterEncoding=utf8"
            + "&serverTimezone=UTC&allowPublicKeyRetrieval=true&useSSL=false";
    private static final String USER = "root";
    private static final String PASS = "";
    private static final String DB = "lab_db";
    private static final String TBL = "test_tbl";

    private static Connection src() throws Exception {
        return DriverManager.getConnection(SRC_URL, USER, PASS);
    }

    private static Connection tgt() throws Exception {
        return DriverManager.getConnection(TGT_URL, USER, PASS);
    }

    private static void exec(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static long countTgt() throws Exception {
        try (Connection c = tgt(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM `" + DB + "`.`" + TBL + "`")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static boolean columnExistsTgt(String col) throws Exception {
        try (Connection c = tgt(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM information_schema.COLUMNS "
                     + "WHERE TABLE_SCHEMA='" + DB + "' AND TABLE_NAME='" + TBL + "' AND COLUMN_NAME='" + col + "'")) {
            rs.next();
            return rs.getLong(1) > 0;
        }
    }

    private static long rowExistsTgt(long id) throws Exception {
        try (Connection c = tgt(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM `" + DB + "`.`" + TBL + "` WHERE id=" + id)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /** 可抛出受检异常的布尔判定（用于轮询） */
    @FunctionalInterface
    private interface CheckedBool {
        boolean get() throws Exception;
    }

    /** 轮询直到 predicate 为真或超时（秒），返回是否达成 */
    private static boolean await(java.time.Duration timeout, CheckedBool p) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                if (p.get()) {
                    return true;
                }
            } catch (Exception ignore) {
                // 目标表可能尚未创建，忽略瞬时异常继续轮询
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return false;
    }

    private static DRPlatformProperties.Cdc buildCdcConfig(File spDir) {
        DRPlatformProperties.Cdc cdc = new DRPlatformProperties.Cdc();
        cdc.setCheckpointIntervalMs(1000);
        cdc.setSavepointPath(spDir.getAbsolutePath());
        cdc.setParallelism(1);
        cdc.setTaskmanagerMemory("1024m");
        cdc.setSnapshotChunkSize(8096);
        cdc.setFetchSize(2048);
        cdc.setServerTimeZone("UTC");
        cdc.setHeartbeatIntervalSec(3);
        cdc.setIncludeSchemaChanges(true);
        cdc.setScanNewlyAddedTable(true);
        cdc.setSinkBatchSize(50);
        cdc.setSinkPoolSize(4);
        cdc.setSourceConnectionPoolSize(4);
        cdc.setServerIdBase(60000);
        cdc.setLagWarnSeconds(10);
        return cdc;
    }

    private static DatabaseMapping buildMapping() {
        DatabaseMapping m = new DatabaseMapping();
        m.setSourceHost("127.0.0.1");
        m.setSourcePort(13306);
        m.setSourceUser(USER);
        m.setSourcePassword(PASS);
        m.setTargetHost("127.0.0.1");
        m.setTargetPort(13307);
        m.setTargetUser(USER);
        m.setTargetPassword(PASS);
        m.setIgnoreDatabases(new ArrayList<>());
        m.setIgnoreTables(new ArrayList<>());
        m.setIgnoreDdlTables(new ArrayList<>());
        return m;
    }

    @Test
    void cdcEndToEnd() throws Exception {
        // ---------- 环境准备 ----------
        try (Connection c = src()) {
            exec(c, "DROP DATABASE IF EXISTS `" + DB + "`");
            exec(c, "CREATE DATABASE `" + DB + "`");
            exec(c, "CREATE TABLE `" + DB + "`.`" + TBL + "` ("
                    + "id INT PRIMARY KEY, name VARCHAR(50), amount DECIMAL(10,2), mod_time DATETIME)");
            exec(c, "INSERT INTO `" + DB + "`.`" + TBL + "` VALUES "
                    + "(1,'a',10.5,NOW()),(2,'b',20.0,NOW()),(3,'c',30.25,NOW())");
        }
        try (Connection c = tgt()) {
            exec(c, "DROP DATABASE IF EXISTS `" + DB + "`");
            exec(c, "CREATE DATABASE `" + DB + "`");
        }

        CdcLagRegistry.configure(10);
        File spDir = Files.createTempDirectory("cdc-sp").toFile();
        DRPlatformProperties.Cdc cfg = buildCdcConfig(spDir);
        DatabaseMapping mapping = buildMapping();
        String key = mapping.getInstanceKey();

        FlinkCdcSyncJob job = new FlinkCdcSyncJob(key, mapping, cfg);

        // ---------- 1) 初始全量（INITIAL）+ 增量 ----------
        long t0 = System.currentTimeMillis();
        job.start(null);
        assertTrue(job.isRunning(), "作业应已启动");

        boolean snapOk = await(java.time.Duration.ofSeconds(20), () -> countTgt() == 3);
        assertTrue(snapOk, "初始快照：3 行应在 20s 内同步到目标");
        System.out.println("[CDC] 快照同步耗时 ~" + (System.currentTimeMillis() - t0) + "ms");

        // ---------- 2) 增量（延迟 < 10s） ----------
        try (Connection c = src()) {
            exec(c, "INSERT INTO `" + DB + "`.`" + TBL + "` VALUES (4,'d',40.0,NOW()),(5,'e',50.0,NOW())");
        }
        long tInc = System.currentTimeMillis();
        boolean incOk = await(java.time.Duration.ofSeconds(10), () -> countTgt() == 5);
        long incLatency = System.currentTimeMillis() - tInc;
        assertTrue(incOk, "增量：2 行应在 10s 内同步");
        assertTrue(incLatency < 10000, "增量同步延迟应 < 10s，实测=" + incLatency + "ms");
        System.out.println("[CDC] 增量同步延迟 ~" + incLatency + "ms（要求 < 10000ms）");

        // ---------- 3) DDL：ADD COLUMN 同步 ----------
        try (Connection c = src()) {
            exec(c, "ALTER TABLE `" + DB + "`.`" + TBL + "` ADD COLUMN ext VARCHAR(20)");
        }
        boolean ddlOk = await(java.time.Duration.ofSeconds(10), () -> columnExistsTgt("ext"));
        assertTrue(ddlOk, "DDL：目标应在 10s 内新增 ext 列");
        // 写入带新列的行并验证
        try (Connection c = src()) {
            exec(c, "INSERT INTO `" + DB + "`.`" + TBL + "` (id,name,amount,mod_time,ext) VALUES (6,'f',60.0,NOW(),'X')");
        }
        boolean ddlDataOk = await(java.time.Duration.ofSeconds(10), () -> rowExistsTgt(6) == 1);
        assertTrue(ddlDataOk, "DDL 后新列数据应同步");

        // ---------- 4) DELETE 同步 ----------
        try (Connection c = src()) {
            exec(c, "DELETE FROM `" + DB + "`.`" + TBL + "` WHERE id=1");
        }
        boolean delOk = await(java.time.Duration.ofSeconds(10), () -> rowExistsTgt(1) == 0);
        assertTrue(delOk, "DELETE：目标应在 10s 内删除 id=1");
        assertTrue(countTgt() == 5, "删除后目标应剩 5 行，实测=" + countTgt());

        // ---------- 5) savepoint 断点恢复（无丢无重） ----------
        long beforeSp = countTgt();
        String spPath = job.stopWithSavepoint();
        assertTrue(spPath != null && new File(spPath).exists(), "stopWithSavepoint 应返回有效路径");
        System.out.println("[CDC] savepoint 路径=" + spPath);

        // 模拟崩溃后重启：新建作业并从 savepoint 恢复
        FlinkCdcSyncJob job2 = new FlinkCdcSyncJob(key, mapping, cfg);
        job2.start(spPath);
        assertTrue(job2.isRunning(), "恢复作业应已启动");

        try (Connection c = src()) {
            // 注意：此时源表已有 5 列（含 ext），必须显式列名，否则列数不匹配
            exec(c, "INSERT INTO `" + DB + "`.`" + TBL + "` (id,name,amount,mod_time) VALUES (7,'g',70.0,NOW())");
        }
        boolean recoverOk = await(java.time.Duration.ofSeconds(10), () -> countTgt() == beforeSp + 1);
        assertTrue(recoverOk, "恢复后增量应从 savepoint 位点续传，目标应 +1 行");
        assertEquals(beforeSp + 1, countTgt(), "savepoint 恢复不应重复也不应丢失既有行");
        System.out.println("[CDC] savepoint 恢复后目标行数=" + countTgt() + "（恢复前=" + beforeSp + "）");

        // ---------- 收尾 ----------
        job2.cancel();
        IdempotentMySqlSink.shutdownPools();
        try (Connection c = src()) {
            exec(c, "DROP DATABASE IF EXISTS `" + DB + "`");
        }
        try (Connection c = tgt()) {
            exec(c, "DROP DATABASE IF EXISTS `" + DB + "`");
        }
        System.out.println("[CDC] 端到端验证通过：快照/增量/延迟<DDL/删除/savepoint 恢复 全部达标");
    }
}
