package com.example.remotedatasync;

import com.example.remotedatasync.common.PageRespVO;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DRPlatform 全链路自动化测试（主机对模型 / 纯忽略模式）。
 * <p>
 * <b>环境</b>：早期版本依赖远程机 192.168.88.88:3306，该主机已离线导致用例无法复现。
 * 现统一改用本机 Docker 容器（root/123456），三实例同在 127.0.0.1、按端口区分：
 * <ul>
 *   <li>{@code 127.0.0.1:13306} 生产中心主库（drp-src-mysql，binlog ROW）</li>
 *   <li>{@code 127.0.0.1:13308} 生产中心备库（drp-src-standby-mysql，主备切换演练用）</li>
 *   <li>{@code 127.0.0.1:13307} 灾备中心（drp-tgt-mysql）</li>
 * </ul>
 * 源主机下所有用户库会以<b>同名库</b>镜像方式实时同步到灾备主机（新增库 / 新增表 /
 * 增量 / 删除对账均为 binlog 驱动）。覆盖 6 个 REST 接口 + 数据一致性 + 映射 CRUD。
 * </p>
 * <p>
 * <b>运行前提</b>：① 三个容器已启动；② 本机 8899 端口上的常驻应用已停止
 * （MySQL 的 max_user_connections 有限，应用连接池与测试上下文叠加会触发
 * {@code User root already has more than 'max_user_connections' active connections}）。
 * </p>
 *
 * @author 50707
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class DRPlatformConsistencyTest {

    /** 生产中心（源主机）与灾备中心（目标主机）——同一 Docker 宿主，按端口区分 */
    private static final String HOST = "127.0.0.1";
    private static final int SRC_PORT = 13306;
    private static final int TGT_PORT = 13307;
    /** 生产中心备库：仅用于「新增映射」用例（见 testMappingConfigEndpoints 注释） */
    private static final int STANDBY_PORT = 13308;

    private static final String USER = "root";
    private static final String PASS = "123456";

    /** 默认主机对 key（instanceKey 含端口，见 DatabaseMapping#getInstanceKey） */
    private static final String DEFAULT_KEY = HOST + ":" + SRC_PORT + "->" + HOST + ":" + TGT_PORT;

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate rest;

    private String base() {
        return "http://127.0.0.1:" + port;
    }

    private HttpHeaders jsonHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return h;
    }

    // ----------------------------- REST 接口测试 -----------------------------

    @Test
    void testStatusEndpoint() {
        ResponseEntity<Map> r = rest.getForEntity(base() + "/sync/status", Map.class);
        assertTrue(r.getStatusCode().is2xxSuccessful(), "status 接口应 200");
        Map body = r.getBody();
        assertNotNull(body);
        assertTrue(body.containsKey("status"), "应返回 status 字段");
        assertTrue(body.containsKey("desc"), "应返回 desc 字段");
    }

    @Test
    void testIpListEndpoint() {
        ResponseEntity<List> r = rest.getForEntity(base() + "/sync/ipList", List.class);
        assertTrue(r.getStatusCode().is2xxSuccessful());
        List<?> list = r.getBody();
        assertNotNull(list);
        assertTrue(list.size() >= 2, "至少应包含生产中心与灾备中心两个节点");
    }

    @Test
    void testDbListEndpoint() {
        String body = "{\"page\":1,\"pageSize\":10,\"condition\":{}}";
        ResponseEntity<PageRespVO> r = rest.exchange(base() + "/sync/db/list", HttpMethod.POST,
                new HttpEntity<>(body, jsonHeaders()), PageRespVO.class);
        assertTrue(r.getStatusCode().is2xxSuccessful(), "db/list 应 200");
        assertNotNull(r.getBody());
    }

    @Test
    void testDatabasesByIp() {
        ResponseEntity<List> r = rest.getForEntity(base() + "/sync/databases/127.0.0.1", List.class);
        assertTrue(r.getStatusCode().is2xxSuccessful());
        List<?> list = r.getBody();
        assertNotNull(list);
        assertTrue(list.contains("geo_source"), "应至少发现源主机下的 geo_source 用户库");
    }

    @Test
    void testResyncAndDetail() {
        // 先触发一次重新同步（全量重置），按源主机 IP 匹配主机对
        String payload = "[{\"ip\":\"127.0.0.1\",\"dbName\":\"geo_source\"}]";
        ResponseEntity<Map> r = rest.exchange(base() + "/sync/resyncDatabases", HttpMethod.POST,
                new HttpEntity<>(payload, jsonHeaders()), Map.class);
        assertTrue(r.getStatusCode().is2xxSuccessful(), "resync 应 200");

        // 等待同步完成后，查询进度列表取首个 ID 做详情校验
        sleep(6000);
        String listBody = "{\"page\":1,\"pageSize\":10,\"condition\":{}}";
        ResponseEntity<PageRespVO> lr = rest.exchange(base() + "/sync/db/list", HttpMethod.POST,
                new HttpEntity<>(listBody, jsonHeaders()), PageRespVO.class);
        PageRespVO<?> page = lr.getBody();
        assertNotNull(page);
        if (page.getResults() != null && !page.getResults().isEmpty()) {
            Object first = page.getResults().get(0);
            Long id = ((Number) ((Map<?, ?>) first).get("id")).longValue();
            ResponseEntity<Map> dr = rest.getForEntity(base() + "/sync/" + id, Map.class);
            assertTrue(dr.getStatusCode().is2xxSuccessful(), "detail 应 200");
        }
    }

    // ----------------------------- 数据一致性测试（同名库跨主机镜像） -----------------------------

    @Test
    void testDataConsistencyInsertAndDelete() {
        String marker = "IT_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String orderNo = marker + "_ORD";
        String username = marker + "_USR";

        // 1) 源库插入一行 order + 一行 user
        execute(SRC_PORT, "geo_source",
                "INSERT INTO t_order (order_no,user_id,amount,status,remark) VALUES ('" + orderNo + "',7777,12.34,1,'it') "
                        + "ON DUPLICATE KEY UPDATE amount=12.34");
        execute(SRC_PORT, "geo_source",
                "INSERT INTO t_user (username,age,email) VALUES ('" + username + "',30,'it@x.com') "
                        + "ON DUPLICATE KEY UPDATE age=30");

        // 2) 等待同步（同名库镜像到灾备主机 13307）
        boolean inserted = waitFor(() -> {
            long src = count(SRC_PORT, "geo_source", "t_order", "order_no='" + orderNo + "'")
                    + count(SRC_PORT, "geo_source", "t_user", "username='" + username + "'");
            long tgt = count(TGT_PORT, "geo_source", "t_order", "order_no='" + orderNo + "'")
                    + count(TGT_PORT, "geo_source", "t_user", "username='" + username + "'");
            return src == 2 && tgt == 2;
        }, Duration.ofSeconds(60));
        assertTrue(inserted, "插入后灾备主机应与源主机一致(各 1 行)");

        // 3) 整体行数一致性：源 == 灾备（同名库）
        boolean consistent = waitFor(() ->
                count(SRC_PORT, "geo_source", "t_order", null) == count(TGT_PORT, "geo_source", "t_order", null)
                        && count(SRC_PORT, "geo_source", "t_user", null) == count(TGT_PORT, "geo_source", "t_user", null),
                Duration.ofSeconds(60));
        assertTrue(consistent, "t_order / t_user 源/灾备整体行数应一致");

        // 4) 源库删除这两行 -> 灾备库应同步删除
        execute(SRC_PORT, "geo_source", "DELETE FROM t_order WHERE order_no='" + orderNo + "'");
        execute(SRC_PORT, "geo_source", "DELETE FROM t_user WHERE username='" + username + "'");

        boolean deleted = waitFor(() ->
                count(TGT_PORT, "geo_source", "t_order", "order_no='" + orderNo + "'") == 0
                        && count(TGT_PORT, "geo_source", "t_user", "username='" + username + "'") == 0,
                Duration.ofSeconds(60));
        assertTrue(deleted, "源库删除后，灾备库应同步移除对应行");

        // 5) 清理（确保测试可重复）
        execute(SRC_PORT, "geo_source", "DELETE FROM t_order WHERE order_no='" + orderNo + "'");
        execute(SRC_PORT, "geo_source", "DELETE FROM t_user WHERE username='" + username + "'");
        execute(TGT_PORT, "geo_source", "DELETE FROM t_order WHERE order_no='" + orderNo + "'");
        execute(TGT_PORT, "geo_source", "DELETE FROM t_user WHERE username='" + username + "'");
    }

    // ----------------------------- 多库同步测试（自动发现新库/新表 + 历史 + 实时） -----------------------------

    @Test
    void testMultiDatabaseHistoricalAndRealtime() {
        // 准备第二个库（生产中心 biz_source），灾备端由引擎按同名自动建库建表
        execute(SRC_PORT, "mysql", "CREATE DATABASE IF NOT EXISTS biz_source");
        execute(SRC_PORT, "biz_source",
                "CREATE TABLE IF NOT EXISTS t_customer ("
                        + "id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64), phone VARCHAR(32), "
                        + "balance DECIMAL(10,2), create_time DATETIME DEFAULT CURRENT_TIMESTAMP, "
                        + "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
        execute(SRC_PORT, "biz_source", "TRUNCATE TABLE t_customer");

        // 1) 历史全量：预置 3 行，等待引擎首次全量扫描同步到灾备主机同名库
        for (int i = 1; i <= 3; i++) {
            execute(SRC_PORT, "biz_source", "INSERT INTO t_customer (name,phone,balance) VALUES ('cust" + i + "','1380000000" + i + "'," + (100 * i) + ")");
        }
        boolean historical = waitFor(() ->
                count(SRC_PORT, "biz_source", "t_customer", null) == 3
                        && count(TGT_PORT, "biz_source", "t_customer", null) == 3, Duration.ofSeconds(60));
        assertTrue(historical, "历史全量同步：biz_source(13306) 与 biz_source(13307) 行数应一致(各 3)");

        // 2) 实时插入
        execute(SRC_PORT, "biz_source", "INSERT INTO t_customer (name,phone,balance) VALUES ('rt_live','13900000000',999.99)");
        boolean inserted = waitFor(() -> count(TGT_PORT, "biz_source", "t_customer", "name='rt_live'") == 1, Duration.ofSeconds(60));
        assertTrue(inserted, "实时插入：新增行应同步到灾备主机");

        // 3) 实时更新
        execute(SRC_PORT, "biz_source", "UPDATE t_customer SET balance=888.88 WHERE name='rt_live'");
        boolean updated = waitFor(() -> {
            double b = queryDecimal(TGT_PORT, "biz_source", "t_customer", "name='rt_live'", "balance");
            return Math.abs(b - 888.88) < 0.01;
        }, Duration.ofSeconds(60));
        assertTrue(updated, "实时更新：balance 变更应同步到灾备主机");

        // 4) 实时删除
        execute(SRC_PORT, "biz_source", "DELETE FROM t_customer WHERE name='rt_live'");
        boolean deleted = waitFor(() -> count(TGT_PORT, "biz_source", "t_customer", "name='rt_live'") == 0, Duration.ofSeconds(60));
        assertTrue(deleted, "实时删除：源库删除后灾备主机应同步移除");

        // 5) 多个库整体一致性：geo_source 各自源==灾备
        boolean geoConsistent = waitFor(() ->
                (count(SRC_PORT, "geo_source", "t_order", null) + count(SRC_PORT, "geo_source", "t_user", null))
                        == (count(TGT_PORT, "geo_source", "t_order", null) + count(TGT_PORT, "geo_source", "t_user", null)),
                Duration.ofSeconds(60));
        assertTrue(geoConsistent, "geo 库对源/灾备应一致");
        assertEquals(3L, count(TGT_PORT, "biz_source", "t_customer", null), "biz 库对灾备应保留 3 行历史数据");

        // 6) 清理
        execute(SRC_PORT, "biz_source", "TRUNCATE TABLE t_customer");
        execute(TGT_PORT, "biz_source", "TRUNCATE TABLE t_customer");
    }

    // ----------------------------- 页面动态配置 / 连接测试端点（主机对模型） -----------------------------

    @Test
    void testMappingConfigEndpoints() {
        // 1) 连接测试（有效账号：生产中心 13306 -> 灾备中心 13307）
        String testPayload = "{\"sourceHost\":\"127.0.0.1\",\"sourcePort\":" + SRC_PORT + ",\"sourceUser\":\"root\",\"sourcePassword\":\"123456\","
                + "\"targetHost\":\"127.0.0.1\",\"targetPort\":" + TGT_PORT + ",\"targetUser\":\"root\",\"targetPassword\":\"123456\"}";
        ResponseEntity<Map> tr = rest.exchange(base() + "/sync/mapping/test", HttpMethod.POST,
                new HttpEntity<>(testPayload, jsonHeaders()), Map.class);
        assertTrue(tr.getStatusCode().is2xxSuccessful(), "mapping/test 应 200");
        Map tbody = tr.getBody();
        assertEquals("0", tbody.get("code"), "连接测试应通过");
        Map tdata = (Map) tbody.get("data");
        assertEquals(Boolean.TRUE, ((Map) tdata.get("source")).get("ok"), "源库连接应成功");
        assertEquals(Boolean.TRUE, ((Map) tdata.get("target")).get("ok"), "目标库连接应成功");

        // 2) 列表应包含配置中的默认主机对（key 含端口）
        ResponseEntity<Map> lr = rest.getForEntity(base() + "/sync/mappings", Map.class);
        assertEquals("0", lr.getBody().get("code"));
        List<Map> list = (List<Map>) lr.getBody().get("data");
        assertTrue(list.stream().anyMatch(m -> DEFAULT_KEY.equals(m.get("instanceKey"))),
                "列表应包含默认主机对 " + DEFAULT_KEY);

        // 3) 新增一个主机对用于 CRUD 验证。
        //    源取「生产中心备库 13308」，并用 ignoreDatabases 忽略全部库 —— 新增映射会立刻拉起同步作业，
        //    若不忽略，备库的存量数据会被灌进灾备库，污染其余用例的断言。
        String key = HOST + ":" + STANDBY_PORT + "->" + HOST + ":" + TGT_PORT;
        String addPayload = "{\"sourceHost\":\"127.0.0.1\",\"sourcePort\":" + STANDBY_PORT + ",\"sourceUser\":\"root\",\"sourcePassword\":\"123456\","
                + "\"targetHost\":\"127.0.0.1\",\"targetPort\":" + TGT_PORT + ",\"targetUser\":\"root\",\"targetPassword\":\"123456\","
                + "\"ignoreDatabases\":[\"re:^.*$\"]}";
        ResponseEntity<Map> ar = rest.exchange(base() + "/sync/mapping/add", HttpMethod.POST,
                new HttpEntity<>(addPayload, jsonHeaders()), Map.class);
        assertTrue(ar.getStatusCode().is2xxSuccessful(), "mapping/add 应 200");
        assertEquals("0", ar.getBody().get("code"), "新增主机对应成功");
        List<String> created = (List<String>) ((Map) ar.getBody().get("data")).get("created");
        assertNotNull(created);
        assertTrue(created.contains(key), "created 应包含新主机对 key: " + key);

        // 4) 重复新增应被拦截
        ResponseEntity<Map> ar2 = rest.exchange(base() + "/sync/mapping/add", HttpMethod.POST,
                new HttpEntity<>(addPayload, jsonHeaders()), Map.class);
        assertEquals("2807002013", ar2.getBody().get("code"), "重复映射应返回已存在错误码");

        // 5) 列表应已包含新映射
        ResponseEntity<Map> lr2 = rest.getForEntity(base() + "/sync/mappings", Map.class);
        List<Map> list2 = (List<Map>) lr2.getBody().get("data");
        assertTrue(list2.stream().anyMatch(m -> key.equals(m.get("instanceKey"))), "列表应包含新增主机对");

        // 6) 移除新映射
        ResponseEntity<Map> rr = rest.exchange(base() + "/sync/mapping/remove", HttpMethod.POST,
                new HttpEntity<>("{\"instanceKey\":\"" + key + "\"}", jsonHeaders()), Map.class);
        assertTrue(rr.getStatusCode().is2xxSuccessful(), "mapping/remove 应 200");
        assertEquals("0", rr.getBody().get("code"), "移除应成功");

        // 7) 移除后列表不再包含该映射
        ResponseEntity<Map> lr3 = rest.getForEntity(base() + "/sync/mappings", Map.class);
        List<Map> list3 = (List<Map>) lr3.getBody().get("data");
        assertTrue(list3.stream().noneMatch(m -> key.equals(m.get("instanceKey"))), "移除后列表不应再包含该映射");
    }

    // ----------------------------- JDBC 工具（按端口区分实例） -----------------------------

    private String url(int port, String db) {
        return "jdbc:mysql://127.0.0.1:" + port + "/" + db
                + "?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true&useSSL=false";
    }

    private void execute(int port, String db, String sql) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            try (Connection c = DriverManager.getConnection(url(port, db), USER, PASS);
                 Statement st = c.createStatement()) {
                st.execute(sql);
            }
        } catch (Exception e) {
            throw new RuntimeException("execute failed: " + sql, e);
        }
    }

    private long count(int port, String db, String table, String where) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String sql = "SELECT COUNT(1) FROM `" + table + "`" + (where == null ? "" : " WHERE " + where);
            try (Connection c = DriverManager.getConnection(url(port, db), USER, PASS);
                 Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        } catch (Exception e) {
            throw new RuntimeException("count failed: " + table, e);
        }
    }

    private double queryDecimal(int port, String db, String table, String where, String column) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String sql = "SELECT `" + column + "` FROM `" + table + "`" + (where == null ? "" : " WHERE " + where);
            try (Connection c = DriverManager.getConnection(url(port, db), USER, PASS);
                 Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                return rs.next() ? rs.getDouble(1) : Double.NaN;
            }
        } catch (Exception e) {
            throw new RuntimeException("queryDecimal failed: " + table, e);
        }
    }

    private boolean waitFor(java.util.function.BooleanSupplier cond, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                if (cond.getAsBoolean()) {
                    return true;
                }
            } catch (Exception ignore) {
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignore) {
        }
    }
}
