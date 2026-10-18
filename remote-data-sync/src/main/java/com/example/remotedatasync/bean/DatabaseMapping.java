package com.example.remotedatasync.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据库同步映射（灾备主机对）：生产中心主机 -> 灾备中心主机，1:1。
 * <p>
 * 与旧版"按库配置"不同，本版为<b>主机级</b>配置：只需配置源主机与目标主机，
 * 该源主机下<b>所有用户库（除系统库与忽略库）的所有表（除忽略表）</b>将被自动同步，
 * 新增的库 / 表也会在下一轮扫描中被实时纳入。是否同步"哪些表"不再需要逐项配置，
 * 只需配置"忽略项"即可。
 * </p>
 * <p>
 * 字段命名与 application.yml 中 DRPlatform.mappings[].* 的 kebab-case 对应。
 * </p>
 *
 * @author 50707
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatabaseMapping implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 所属用户 ID（null 表示全局/yml 配置的映射，所有用户可见） */
    private Long userId;

    /** 源主机（生产中心） */
    private String sourceHost;
    private int sourcePort = 3306;
    private String sourceUser;
    private String sourcePassword;

    /** 目标主机（灾备中心） */
    private String targetHost;
    private int targetPort = 3306;
    private String targetUser;
    private String targetPassword;

    /** 备源主机（生产中心主备切换用，可选）：主库不可用时切到此地址继续同步 */
    private String sourceStandbyHost;
    private int sourceStandbyPort = 3306;

    /** 整库跳过（不扫描、不同步），支持多个 + 正则（见 MatchPattern） */
    private List<String> ignoreDatabases = new ArrayList<>();

    /** 表跳过：DML 与 DDL 均不处理，支持多个 + 正则（旧式扁平写法，支持「库.表」限定，向后兼容） */
    private List<String> ignoreTables = new ArrayList<>();

    /** 仅跳过 DDL（结构变更不同步），但 DML 数据仍同步，支持多个 + 正则（旧式扁平写法，向后兼容） */
    private List<String> ignoreDdlTables = new ArrayList<>();

    /** 按「库 → 表」层级忽略（DML + DDL 均不处理）：每个库可配置不同的忽略表列表 */
    private List<DbTableIgnore> ignoreTablesByDb = new ArrayList<>();

    /** 按「库 → 表」层级忽略（仅忽略 DDL，数据仍同步）：每个库可配置不同的忽略表列表 */
    private List<DbTableIgnore> ignoreDdlTablesByDb = new ArrayList<>();

    /** 通用忽略（DML + DDL 均不处理），对所有库生效，支持精确 + 正则 */
    private List<String> commonIgnoreTables = new ArrayList<>();

    /** 通用忽略（仅忽略 DDL，数据仍同步），对所有库生效，支持精确 + 正则 */
    private List<String> commonDdlIgnoreTables = new ArrayList<>();

    /** 字段内容转换规则（写入目标库前对匹配字段做值替换） */
    private List<TransformRule> transformRules = new ArrayList<>();

    /** 兼容旧代码：源 IP */
    public String getSourceIP() {
        return sourceHost;
    }

    /** 兼容旧代码：目标 IP */
    public String getTargetIP() {
        return targetHost;
    }

    /** 仅含主机的源库连接（无具体库名，用于发现该主机下所有用户库） */
    public DatabaseConfig toSourceHostConfig() {
        return new DatabaseConfig(sourceHost, sourcePort, sourceUser, sourcePassword, null);
    }

    /** 仅含主机的目标库连接（无具体库名） */
    public DatabaseConfig toTargetHostConfig() {
        return new DatabaseConfig(targetHost, targetPort, targetUser, targetPassword, null);
    }

    /** 指定源库名的连接配置（扫描/同步具体库时使用） */
    public DatabaseConfig toSourceDB(String dbName) {
        return new DatabaseConfig(sourceHost, sourcePort, sourceUser, sourcePassword, dbName);
    }

    /** 指定目标库名的连接配置（库名与源库同名镜像） */
    public DatabaseConfig toTargetDB(String dbName) {
        return new DatabaseConfig(targetHost, targetPort, targetUser, targetPassword, dbName);
    }

    /**
     * 实例 key：{@code sourceHost:sourcePort->targetHost:targetPort}（主机对唯一）。
     * <p>
     * <b>必须包含端口</b>：早期版本只用 IP 拼接，在同一主机多实例场景（如容器化验证环境
     * 全部落在 127.0.0.1，仅靠 13306/13307/13308 区分）下会得到完全相同的 key，导致
     * 「生产中心↔灾备中心互换」「主备切换」时新旧作业共用同一 key，
     * 表现为新作业被 {@code jobs.containsKey(key)} 静默跳过、旧 savepoint 被新方向错误复用。
     * </p>
     */
    public String getInstanceKey() {
        return sourceHost + ":" + sourcePort + "->" + targetHost + ":" + targetPort;
    }

    /** 仅含 IP 的兼容 key（旧接口入参/页面展示用）：sourceHost->targetHost */
    public String getHostOnlyKey() {
        return sourceHost + "->" + targetHost;
    }

    /**
     * 物理链路 key（<b>无序</b>）：同一对主机无论正/反方向都得到同一个 key。
     * <p>
     * 用于识别「同一条灾备链路的角色互换」：生产↔灾备互换后 instanceKey 变了，
     * 但物理链路没变，因此恢复持久化状态时可以用它去重，避免重启后同时启动正反两个作业。
     * </p>
     */
    public String getLinkKey() {
        String a = sourceHost + ":" + sourcePort;
        String b = targetHost + ":" + targetPort;
        return a.compareTo(b) <= 0 ? a + "|" + b : b + "|" + a;
    }
}
