# DRPlatform — MySQL 跨主机数据库灾备同步中间件

DRPlatform 将生产中心 MySQL 的数据实时、可靠地同步到异地灾备中心。采用"主机对模型 + 自动库发现"设计：运维只需配置一组源主机与目标主机，引擎自动发现源端全部用户库表并启动同步，新增库表无需人工介入。同步引擎为**自研内嵌轮询水位引擎（SyncHostJob）**，**不依赖 Flink 集群**，部署轻量。

> 文档导航：完整功能与配置见 [产品规格说明书](DRPlatform-产品规格说明书.md)；上线与运维操作见 [使用说明书](DRPlatform-使用说明书.md)；当前支持能力速览见 [特性清单](DRPlatform-特性清单.md)；需求背景见 [PRD](DRPlatform-PRD.md)；详细设计与数据库见 [详细设计说明书](DRPlatform-详细设计说明书.md)；安装细节见 [安装部署指南](DRPlatform-安装部署指南.md)。

## 核心能力

| 能力 | 说明 |
|------|------|
| 自动库发现 | 周期扫描（约 60s）自动发现源端新增库表并纳入同步 |
| 全量 + 增量同步 | 首次全量流式分页；后续按 `update_time` 水位增量同步，增量单批上限 5000 行（keyset 续扫，同时间戳大批量不丢） |
| 双模式忽略规则 | DML+DDL 忽略 与 仅 DDL 忽略 独立配置，支持精确 / glob / 正则（`re:`）三种匹配 |
| 跨版本 DDL 清洗 | MySQL 8.0 专有语法（如 `utf8mb4_0900_ai_ci`）自动降级为 5.7 兼容语法 |
| 字段转换 | IP 替换、字段脱敏、枚举映射；全局映射与私有映射分层 |
| 配置热生效 | 修改忽略规则 / 字段映射 / 连接参数无需重启，API 即时生效 |
| 断电断网续传 | 水位丢失自动全量重扫，幂等 upsert 保证最终一致 |
| 故障容错与一致性 | 源库 / 目标库 / 同步服务进程断电重启后数据最终一致（故障注入测试覆盖） |
| 多用户隔离 | 配置按用户隔离，私有配置仅创建者可见 |
| 操作审计 | AOP 切面自动记录所有操作，密码等敏感字段脱敏后入库 |
| Web 管理后台 | React 单页应用，可视化监控同步进度与异常，支持双主题一键切换 |
| 安全加固 | 服务端 Session 鉴权、CORS 非通配符、PBKDF2 密码哈希、标识符 SQL 注入白名单、密码落盘 AES 加密（`ENC:`） |

## 技术栈

| 层级 | 选型 |
|------|------|
| 后端 | Spring Boot 3.4.3 + MyBatis-Plus 3.5.9 + Java 17（编译/运行可用 JDK 21） |
| 前端 | React 18 UMD + JSX 构建期预编译（Babel 生成 `app.js`，运行时无 Node 依赖） |
| 数据库 | MySQL 8.0（源）→ MySQL 5.7（目标），跨大版本兼容；同版本任意方向亦可 |
| 连接池 | HikariCP + 全局信号量限流，临时不可用时自动重连 |
| 鉴权 | 服务端 HttpSession + 自定义 `AuthInterceptor`（非 Spring Security，非 JWT） |
| 部署 | systemd + Nginx 反向代理 + Watchdog 守护进程（开机自启 / 宕机自愈） |

> 端口：**默认 `8899`**（jar 内嵌、linux profile、`watchdog.sh`、目标机 `application-linux.yml` 均为 8899）。注意 `deploy/deploy.sh` 内的 Nginx 反代曾写死 `8080`，部署后需核对反代端口与 `8899` 一致。

## 快速部署

### 环境要求
- Linux（CentOS 7 / Ubuntu 18.04+ / Rocky 8·9 等主流发行版）
- JDK 17+（运行）、MySQL 5.7+（源端与目标端可同机或异机）
- 网络可达源端与目标端 MySQL 的 3306 端口

### 构建
> 本仓库使用自带的 `mvn3.sh`（避免系统 `mvn` 在 Git-Bash 下的路径问题）：

```bash
cd remote-data-sync
bash /d/WorkSpace/flink-cdc-sync/mvn3.sh -o clean package -DskipTests
# 产物：remote-data-sync/target/remote-data-sync.jar
```

前端为构建期预编译产物（`frontend/app.js` 由 `frontend/build_app.js` 经 Babel 生成），随 jar 内嵌 `static/` 提供，无需 Node 运行时。

### 部署与启动（目标机 Linux）
推荐用 `deploy/` 下的脚本完成一键部署与守护：

```bash
# 一键部署（装 Java/Nginx、布 jar+前端、初始化元库、配 systemd + Nginx 反代）
sudo bash remote-data-sync/deploy/deploy.sh

# 或交互式安装（输入 MySQL 账号、后端端口默认 8899、源/目标 IP，生成 application-linux.yml）
sudo bash remote-data-sync/deploy/install.sh

# 服务管理（基于 watchdog 守护）
bash remote-data-sync/deploy/start.sh     # 启动 + 开启开机自启
bash remote-data-sync/deploy/stop.sh      # 停止（先停 watchdog，手动停止不自动拉起）
bash remote-data-sync/deploy/boot.sh status  # 查看开机自启状态
systemctl status DRPlatform              # 查看服务
```

部署后访问 `http://<服务器IP>:8899/`。**默认管理员账号 `admin / admin123`，首次部署后请立即修改密码。**

> 若从外部无法访问，请确认目标机防火墙已放行 `8899/tcp`（CentOS：`firewall-cmd --permanent --add-port=8899/tcp && firewall-cmd --reload`）。

## 目录结构（部署后）

```
/data/DRPlatform/
├── bin/
│   └── remote-data-sync.jar        # 应用主包
├── conf/
│   └── application-linux.yml       # 运行配置（由 install.sh 生成）
├── logs/
│   ├── app.log / app.out / app.err # 日志
└── run/                            # 运行时文件（含动态映射 DRPlatform-dynamic-mappings.json）
```

> 元数据库 `DRPlatform` 独立存放配置、水位、审计，与业务库分离。动态新增的主机对持久化到 `DRPlatform-dynamic-mappings.json`（密码以 `ENC:` 密文存储）。

## 配置说明

主配置文件：`remote-data-sync/src/main/resources/application.yml`（内嵌 `linux` profile），部署后实际为 `/data/DRPlatform/conf/application-linux.yml`。

### 核心配置项

```yaml
server:
  port: 8899                         # 默认端口
drplatform:
  standby-mode: true                # true=启动同步引擎，false=仅管理后台
  flink:
    enabled: false                  # 走内嵌轮询引擎，不提交真实 Flink 集群
  engine:
    poll-interval-ms: 2000          # 同步轮询间隔（毫秒）
    batch-size: 1000                # 单批次行数（增量单批 = batch-size * 5 = 5000）
    max-queue-size: 20000           # 死信队列上限
    reconcile-interval-ms: 10000    # 对账间隔（毫秒）
    deviation-timeout-sec: 300      # 偏差超时告警（秒）
  connection:
    max-pool-size: 32               # 单主机连接池上限
    max-global-connections: 200     # 全局连接数信号量
  mappings:
    - source-host: 192.168.88.88    # 源端 MySQL 主机
      source-port: 3306
      source-user: root
      source-password: "********"   # 落盘为 ENC: 密文
      target-host: 192.168.88.88
      target-port: 3306
      target-user: root
      target-password: "********"
      ignore-databases: ["day01"]   # 整库忽略（支持 re:^tmp_.*）
      ignore-tables: []             # 表忽略（DML+DDL 均跳过）
      ignore-ddl-tables: []         # 仅忽略 DDL（数据仍同步）
      transform-rules: []           # 字段转换规则
app:
  auth:
    enabled: true                   # 是否启用登录鉴权（test profile 关闭）
  cors:
    allowed-origins: ""             # 逗号分隔的允许跨域来源；为空仅允许同源（禁止 * + 凭据）
```

### 忽略规则语法
三种匹配可混用：`db.table`（精确）、`db_*.log_*`（glob）、`re:^shard_\d+\..*`（正则）。

### 字段转换规则
```yaml
transform-rules:
  - database: geo_source
    table: t_user
    column: phone
    type: MASK          # MASK=脱敏, REPLACE=替换, MAP=枚举映射
    oldValue: ""
    newValue: "138****0000"
```

### 配置热生效
```bash
# 增量更新单个主机对（无需重启）
curl -X POST http://localhost:8899/sync/mapping/update \
  -H "Content-Type: application/json" -b cookies.txt \
  -d '{"sourceHost":"192.168.88.88","ignoreDatabases":["day01","day02"]}'
# 全量重载
curl -X POST http://localhost:8899/sync/mapping/reload -b cookies.txt
```

## 服务管理

```bash
systemctl start DRPlatform     # 启动（经 watchdog 拉起）
systemctl stop  DRPlatform     # 停止
systemctl status DRPlatform    # 状态
journalctl -u DRPlatform -f    # 实时日志
tail -f /data/DRPlatform/logs/app.log
```

Watchdog（`watchdog.sh`）负责进程保活：应用崩溃自动拉起；`stop.sh` 先终止 watchdog 再停应用，保证"手动停止不被自动拉回"。

## API 接口

除 `/auth/**` 与静态资源外，所有接口需登录后携带 Session Cookie（`JSESSIONID`）访问，未登录返回 **401 JSON**。

### 鉴权
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/auth/register` | 注册新后台用户（密码 PBKDF2 加盐） |
| POST | `/auth/login` | 登录，成功后写 Session（默认约 30 分钟空闲失效） |
| POST | `/auth/logout` | 登出，使 Session 失效 |
| GET  | `/auth/me` | 登录态探测（恒返回 200，`{loggedIn:true/false}`） |

### 同步配置 / 监控 / 元数据（前缀 `/sync`）
| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/sync/mappings` | 当前用户可见的主机对列表（密码脱敏） |
| POST | `/sync/mapping/add` | 批量新增主机对（启动同步作业） |
| POST | `/sync/mapping/remove` | 移除动态新增的映射 |
| POST | `/sync/mapping/reload` | 热重载指定主机对并重建作业 |
| POST | `/sync/mapping/update` | 运行时更新映射配置字段（热生效） |
| POST | `/sync/mapping/test` | 源/目标端连通性测试 |
| POST | `/sync/sourceDatabases` | 枚举源实例可同步用户库（排除系统库） |
| POST | `/sync/sourceTables` | 枚举指定库下用户表 |
| GET  | `/sync/databases/{ips}` | 按 IP 获取其下用户数据库列表 |
| GET  | `/sync/ipList` | 获取生产/灾备中心 IP 列表 |
| POST | `/sync/db/list` | **分页查询同步进度**（注意：非 `/sync/progress`） |
| GET  | `/sync/{id}` | 按 ID 查询同步详情 |
| GET  | `/sync/status` | 同步引擎整体状态（含 ABNORMAL 判定） |
| POST | `/sync/resyncDatabases` | 触发指定库全量重同步 |

### 审计日志（前缀 `/operation-log`）
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/operation-log/list` | 分页查询操作审计（用户名/类型/结果/IP/时间段过滤） |
| GET  | `/operation-log/types` | 操作类型枚举（前端筛选下拉） |

### 健康检查（Actuator，仅暴露 health/info/metrics）
| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/actuator/health` | 服务健康检查（show-details: always） |
| GET | `/actuator/info` | 基本信息 |
| GET | `/actuator/metrics` | 运行指标 |

### 登录示例
```bash
curl -c cookies.txt -X POST http://localhost:8899/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}'
curl -b cookies.txt http://localhost:8899/sync/status
```

## 安全特性

| 维度 | 实现 |
|------|------|
| 认证 | 服务端 HttpSession + `AuthInterceptor`，保护 `/sync/**` 与 `/operation-log/**`；`app.auth.enabled` 可关 |
| 授权隔离 | 配置按 `owner_id` 隔离，普通用户仅见自己资源，管理员可全局排查 |
| CORS | `drplatform.cors.allowed-origins` 显式来源列表；**禁止 `*` 与 `allowCredentials(true)` 同用**（防 CSRF 反射任意 Origin） |
| 密码哈希 | `PasswordUtil` 采用 **PBKDF2WithHmacSHA256，12 万次迭代，256-bit 密钥，加盐 16 字节**；格式 `pbkdf2$iterations$salt$hash`；兼容历史 `salt:hash`（SHA-256） |
| 密码落盘 | 动态映射的源/目标密码以 `CryptoUtil`（AES）加密，`ENC:` 前缀存储，加载时解密 |
| SQL 注入防护 | 库/表标识符走白名单 `^[A-Za-z0-9_$]{1,64}$`（`DatabaseMetadataService.assertSafeIdentifier`），DDL 拼接前强制校验 |
| 审计脱敏 | AOP 切面记录所有操作，`password` 等敏感字段入库前脱敏为 `***` |

## 测试

自动化测试覆盖鉴权、同步引擎、故障注入、安全加固与前端：

| 类别 | 文件 / 类 | 用例数 | 覆盖 |
|------|-----------|--------|------|
| 集成测试（Python + pytest） | `tests/test_01_auth.py` … `test_18_security_robustness.py`（19 个模块） | **253** | 鉴权、同步只读/连接/映射、健壮性、安全、引擎端到端、定时任务、元数据、连接池、忽略转换、HA 重启、配置热生效、用户隔离审计、操作日志、主题切换 |
| 故障注入一致性 | `tests/test_17_fault_injection_consistency.py` | 5 | 源库/目标库/服务断电重启 + 多轮稳定性，数据最终一致 |
| 安全与健壮性 | `tests/test_18_security_robustness.py` | 5 | CORS 非通配符、密码密文落盘、监控盲区 ABNORMAL、同时间戳 5200 行不丢、元数据注入拦截 |
| JUnit 单元 | `SecurityHardeningTest` / `DRPlatformConsistencyTest` | 17 | SQL 注入白名单、CryptoUtil、PBKDF2、跨主机一致性 |

运行（需先启动应用与 MySQL）：
```bash
cd tests && python -m pytest test_17_fault_injection_consistency.py -v   # 需 docker-lab 隔离环境
cd tests && python -m pytest test_18_security_robustness.py -v
```
隔离测试实验室见 `tests/docker-lab/`（`docker-compose.yml` 源/目标 MySQL 容器 + `application-dockerlab.yml`）。

## 本地开发

- JDK 17+（可用 21），Maven 用仓库自带 `mvn3.sh`
- 构建：`bash mvn3.sh -o clean package -DskipTests`
- 启动：`java -jar target/remote-data-sync.jar --server.port=8899`
  - ⚠️ 本机若存在 `SERVER__PORT` 环境变量（如 WorkBuddy 桌面进程占用 9591），会覆盖 `application.yml` 的 8899，需显式 `--server.port=8899` 并 `unset SERVER__PORT SERVER_PORT`
- 前端构建：`cd frontend && node build_app.js`（JSX→`app.js`，并刷新 `index.html` 缓存版本号）

## 故障排查

### 服务无法启动
- `journalctl -u DRPlatform -n 100` 与 `tail -100 /data/DRPlatform/logs/app.err`
- 常见：MySQL 凭据错误、`8899` 端口被占用（`netstat -tlnp | grep 8899`）、Java 版本 < 17

### 外部无法访问 `:8899`
- 目标机防火墙需放行 `8899/tcp`；Nginx 反代（`deploy/deploy.sh`）若写死 `8080` 需改为 `8899` 并 `nginx -s reload`

### 同步延迟高
- `curl -b cookies.txt http://localhost:8899/sync/status` 与 `/sync/db/list` 排查
- 大表全量阻塞 → 配置 `ignore-databases`；目标端压力大 → 降低 `engine.batch-size`

### 忘记管理员密码
```sql
USE DRPlatform;
SELECT ID, USERNAME FROM sys_user;
-- 删除后重启，SysUserInitializer 会重建默认 admin/admin123；或重新注册
DELETE FROM sys_user WHERE USERNAME='admin';
```
> 密码哈希为 `pbkdf2$120000$salt$hash`，需经 `PasswordUtil` 生成，不建议手工写库。

## 卸载
```bash
bash remote-data-sync/deploy/stop.sh
systemctl disable DRPlatform
rm -f /etc/systemd/system/DRPlatform.service /etc/systemd/system/DRPlatform-watchdog.service
rm -rf /data/DRPlatform
systemctl daemon-reload
# 元数据库手动清理：DROP DATABASE DRPlatform;
```

## 相关文档

| 文档 | 说明 |
|------|------|
| [DRPlatform-产品规格说明书.md](DRPlatform-产品规格说明书.md) | 产品规格 — 架构、功能规格、安全模型、配置参考、部署架构 |
| [DRPlatform-使用说明书.md](DRPlatform-使用说明书.md) | 使用说明书 — 部署上线、服务管理、Web 配置、监控运维、故障排查 |
| [DRPlatform-特性清单.md](DRPlatform-特性清单.md) | 特性清单 — 当前支持能力速览 |
| [DRPlatform-PRD.md](DRPlatform-PRD.md) | 产品需求文档 — 功能定义、用户场景、成功指标 |
| [DRPlatform-详细设计说明书.md](DRPlatform-详细设计说明书.md) | 详细设计 — 模块、数据库、API、测试策略 |
| [DRPlatform-安装部署指南.md](DRPlatform-安装部署指南.md) | 安装部署详细步骤（环境 / 一键安装 / 手动 / 升级 / 卸载） |
| [灾备同步容错与异常场景分析报告.md](灾备同步容错与异常场景分析报告.md) | 容错与异常场景分析 |
| [前后端自动化测试与Babel错误根治报告.md](前后端自动化测试与Babel错误根治报告.md) | 自动化测试与前端构建说明 |
