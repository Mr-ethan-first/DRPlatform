package com.example.remotedatasync.cdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DDL 应用器：把 binlog 中的 DDL 事件（Debezium 原始语句文本）清洗后应用到目标库。
 *
 * <p>清洗规则（与轮询引擎保持一致，保证跨大版本可移植）：
 * <ul>
 *   <li>去掉库限定符 {@code `db`.} → 目标端按当前库执行；</li>
 *   <li>MySQL 8.0 专有排序规则 {@code utf8mb4_0900_*} → {@code utf8mb4_general_ci}（5.7 目标可执行）；</li>
 *   <li>{@code CREATE TABLE} 补 {@code IF NOT EXISTS}（savepoint 恢复重放时幂等）。</li>
 * </ul>
 *
 * <p>调用方（sink 同一线程）保证同一表的 DDL 与行事件严格按 binlog 顺序执行。
 *
 * @author 50707
 */
public class DdlApplier {

    private static final Logger log = LoggerFactory.getLogger(DdlApplier.class);

    private static final Pattern DB_QUALIFIER = Pattern.compile("`[^`]+`\\s*\\.");

    private static final Pattern COLLATION_0900 = Pattern.compile("(?i)utf8mb4_0900_\\w+");

    private static final Pattern CREATE_TABLE = Pattern.compile("(?i)^\\s*CREATE\\s+TABLE\\s+(?!IF\\s+NOT\\s+EXISTS)");

    private static final Pattern CREATE_DATABASE = Pattern.compile("(?i)^\\s*CREATE\\s+DATABASE\\s+(?!IF\\s+NOT\\s+EXISTS)");

    /**
     * 清洗 DDL 使其可在目标库执行。
     *
     * @param ddl Debezium 提供的原始 DDL 文本
     * @return 清洗后的 DDL；无需执行返回 null（如 USE、SET 等会话级语句）
     */
    public String sanitize(String ddl) {
        if (ddl == null || ddl.trim().isEmpty()) {
            return null;
        }
        String s = ddl.trim();
        // 会话级/元数据级语句不同步（USE/SET/FLUSH 等）
        String upper = s.toUpperCase();
        if (upper.startsWith("USE ") || upper.startsWith("SET ")
                || upper.startsWith("FLUSH ") || upper.startsWith("BEGIN")
                || upper.startsWith("COMMIT") || upper.startsWith("START TRANSACTION")) {
            return null;
        }
        // 去库限定符：`db`.`t` -> `t`
        s = DB_QUALIFIER.matcher(s).replaceAll("");
        // 8.0 专有排序规则降级
        s = COLLATION_0900.matcher(s).replaceAll("utf8mb4_general_ci");
        // CREATE TABLE / CREATE DATABASE 幂等化
        s = CREATE_TABLE.matcher(s).replaceAll("CREATE TABLE IF NOT EXISTS ");
        s = CREATE_DATABASE.matcher(s).replaceAll("CREATE DATABASE IF NOT EXISTS ");
        // SAVEPOINT/RELEASE 等无意义语句跳过
        if (upper.startsWith("SAVEPOINT") || upper.startsWith("RELEASE SAVEPOINT")) {
            return null;
        }
        return s;
    }

    /**
     * 在目标库连接上执行 DDL（自动提交，DDL 在 MySQL 中本身隐式提交）。
     */
    public void apply(Connection conn, String database, String table, String rawDdl) throws SQLException {
        String ddl = sanitize(rawDdl);
        if (ddl == null) {
            return;
        }
        try (Statement st = conn.createStatement()) {
            st.execute(ddl);
            log.info("CDC DDL applied on target [{}]: {}", database + "." + table, abbreviate(ddl));
        } catch (SQLException e) {
            // 幂等重放：checkpoint/savepoint 恢复会把位点前后的 DDL 重放一次，
            // 目标端已生效的结构变更会报「已存在/不存在」类错误，按成功处理。
            // 1050 表已存在 / 1007 库已存在 / 1060 列重复 / 1061 键名重复 / 1091 列不存在（DROP）
            int code = e.getErrorCode();
            if (code == 1050 || code == 1007 || code == 1060 || code == 1061 || code == 1091) {
                log.info("CDC DDL idempotent-skip on target [{}] (code={}): {}",
                        database + "." + table, code, abbreviate(ddl));
                return;
            }
            throw e;
        }
    }

    private static String abbreviate(String s) {
        return s.length() > 300 ? s.substring(0, 300) + "..." : s;
    }
}
