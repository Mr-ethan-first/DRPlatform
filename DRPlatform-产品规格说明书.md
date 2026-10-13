# DRPlatform 产品规格说明书

## 文档元信息

| 项 | 内容 |
|---|------|
| 产品名称 | DRPlatform — MySQL 跨主机数据库灾备同步中间件 |
| 文档类型 | 产品规格说明书（Product Specification） |
| 文档版本 | v1.0（基于 v2.0 PRD 与已上线实现刷新） |
| 刷新日期 | 2026-07-21 |
| 状态 | Released — 基于代码实现反向梳理，已对齐真实 API / 端口 / 安全模型 |
| 配套文档 | [PRD](DRPlatform-PRD.md) · [使用说明书](DRPlatform-使用说明书.md) · [特性清单](DRPlatform-特性清单.md) · [详细设计](DRPlatform-详细设计说明书.md) |

> 变更提示（相对早期文档）：默认端口统一为 **8899**（非 8080/8090）；同步进度接口为 **`POST /sync/db/list`**、详情为 **`GET /sync/{id}`**（非 `/sync/progress`）；密码哈希为 **PBKDF2**（`pbkdf2$iter$salt$hash`，兼容历史 SHA-256）；CORS **禁止 `*` + 凭据**；动态映射密码落盘 **AES 加密 `ENC:`**；鉴权为 **服务端 Session + AuthInterceptor**（非 Spring Security / 非 JWT）；同步引擎为**自研内嵌轮询水位引擎**（`drplatform.flink.enabled=false`，不依赖 Flink 集群）。

---

## 1. 产品概述

### 1.1 产品定位
DRPlatform 是 **MySQL 跨主机数据库灾备同步中间件**，将生产中心 MySQL 数据实时、可靠地同步到异地灾备中心。以"主机对（1:1 源↔目标）"为管理单元，自动发现源端全部用户库表并启动同步，新增库表无需人工介入。

### 1.2 解决的问题
| 痛点 | DRPlatform 解决思路 |
|------|----------------------|
| 新增库表易遗漏 | 周期自动库发现（约 60s），新增库表自动纳入 |
| 跨 MySQL 大版本 DDL 不兼容 | DDL 清洗引擎自动降级为 5.7 兼容语法 |
| 配置需重启生效 | 配置热生效接口，不重启即时更新 |
| 故障恢复慢（断电断网） | 全量重扫 + 幂等 upsert 保证最终一致 |
| 缺乏可视化监控 | Web 管理后台可视化监控进度与异常 |
| 忽略规则粒度不足 | 双模式忽略（DML+DDL / 仅 DDL）独立配置 |
| 操作无审计追溯 | AOP 切面自动记录所有操作，敏感字段脱敏 |
| 明文凭据 / 弱哈希风险 | 密码 PBKDF2 哈希 + 落盘 AES 加密 + SQL 注入白名单 |

### 1.3 非目标
- **不支持跨数据库类型**（MySQL → 其他），当前仅 MySQL → MySQL（5.7 与 8.0 任意方向）
- **不支持双向同步**（避免冲突解决复杂度，非灾备核心诉求）
- **不依赖 Flink 集群**：自研内嵌引擎 `SyncHostJob`，降低部署门槛（`drplatform.flink.enabled=false`）
- **不使用 JWT/Token 鉴权**：管理后台场景下服务端 Session + Cookie 足够

---

## 2. 用户与场景

### 2.1 用户角色
| 角色 | 核心诉求 | 典型场景 |
|------|----------|----------|
| DBA / 运维 | 巡检状态、新增灾备映射、定位异常 | 每日查看主机对状态与延迟，必要时一键重同步 |
| 开发 / 架构师 | 快速搭建灾备、验证一致性 | 初始化配置，验证源/目标行数与字段一致 |
| 项目经理 / 主管 | 掌握灾备覆盖率与异常 | 查看总览，关注异常主机对、延迟趋势 |

### 2.2 典型场景
- **首次部署**：登录 → 配置中心新增主机对（源 8.0 / 目标 5.7）→ 测试连通 → 保存 → 引擎自动全量同步 → 统计监控查看进度。
- **业务新增库**：源端新建库表 → 约 60s 内自动发现 → 自动全量同步 → 无需运维介入。
- **断电断网恢复**：服务/网络恢复后水位丢失 → 受影响表全量重扫 → 幂等 upsert 最终一致 → 状态由 ABNORMAL 恢复 NORMAL。
- **排除系统库表**：为 DML 与 DDL 分别配置忽略规则（如 `mysql.*`、`sys.*`）→ 引擎按规则跳过。

---

## 3. 整体架构

### 3.1 技术栈
| 层级 | 选型 |
|------|------|
| 后端 | Spring Boot 3.4.3 + MyBatis-Plus 3.5.9 + Java 17（运行可用 JDK 21） |
| 前端 | React 18 UMD + JSX 构建期预编译（Babel → `app.js`，运行时无 Node） |
| 源 / 目标 | MySQL 8.0 → 5.7（跨版本）；同版本任意方向 |
| 连接池 | HikariCP + 全局信号量限流 |
| 鉴权 | 服务端 HttpSession + 自定义 `AuthInterceptor`（非 Spring Security） |
| 部署 | systemd + Nginx 反代 + Watchdog 守护（开机自启 / 宕机自愈） |
| 元数据库 | `DRPlatform`（独立，存配置 / 水位 / 审计） |

### 3.2 架构图
```
┌──────────────────────────────────────────────────────────────┐
│                   Web 管理后台（React 单页）                    │
│   统计监控 / 配置中心 / 操作日志 / 登录 / 双主题一键切换          │
└───────────────────────┬──────────────────────────────────────┘
                        │ HTTP :8899（Nginx 反代）
┌───────────────────────▼──────────────────────────────────────┐
│                 Spring Boot 应用（DRPlatform）                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │
│  │AuthInterceptor│ AOP 审计│ │配置热生效 │  │ CORS / 注入白│  │
│  └──────────┘  └──────────┘  └──────────┘  │ 名单校验      │  │
│  ┌────────────────────────────────────────────────────────┐  │
│  │         同步引擎（SyncHostJob，内嵌轮询水位）             │  │
│  │ 扫描发现→全量分页→增量水位(keyset)→忽略过滤→字段转换    │  │
│  │ →DDL清洗→批量 upsert→对账→进度持久化                     │  │
│  └────────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────────┐  │
│  │            HikariCP 连接池（源/目标 + 全局信号量）         │  │
│  └────────────────────────────────────────────────────────┘  │
└───────────────┬──────────────────────────┬───────────────────┘
        ┌───────▼────────┐          ┌───────▼────────┐
        │  源 MySQL 8.0   │          │ 目标 MySQL 5.7  │
        │  （生产中心）    │          │ （灾备中心）     │
        └────────────────┘          └────────────────┘
```

### 3.3 关键设计决策
| 决策 | 选择 | 理由 |
|------|------|------|
| 同步引擎 | 自研内嵌 `SyncHostJob` 轮询水位 | 不依赖 Flink 集群，部署轻量 |
| 增量续扫 | keyset（`(update_time=wm AND pk>key)`） | 同时间戳大批量（>5000）不丢数据 |
| 前端 | React UMD + 构建期预编译 | 避免运行时 Babel 转译漂移 |
| 死信队列 | 上限 20000，超限丢弃 + 全量重扫补偿 | 防 OOM，保证最大恢复 |
| 目标删除 | 基于 PK 的 `DELETE` | 跨版本兼容（5.7 不支持特定 DELETE 语法） |
| 元数据库独立 | `DRPlatform` 与业务库分离 | 配置/水位不受业务库影响 |
| 鉴权 | Session + 自定义拦截器 | 管理后台场景足够，避免 JWT 复杂度 |
| 密码存储 | PBKDF2 哈希 + 落盘 AES(`ENC:`) | 抗暴力破解 + 静态数据加密 |

---

## 4. 功能规格

### 4.0 安全模型（新增）
| 维度 | 规格 |
|------|------|
| 认证 | `AuthController` + `AuthInterceptor`：保护 `/sync/**`、`/operation-log/**`；放行 `/auth/**`、静态资源。Session 默认约 30 分钟空闲失效，可配置。 |
| 授权 | 配置按 `owner_id` 隔离；管理员可全局排查不可改他人私有配置。 |
| CORS | `drplatform.cors.allowed-origins` 显式来源；空值仅同源；**禁止 `*` 与 `allowCredentials(true)` 同用**。 |
| 密码哈希 | `PasswordUtil`：PBKDF2WithHmacSHA256，迭代 120000，256-bit，盐 16B；格式 `pbkdf2$iter$salt$hash`；兼容 `salt:hash`（SHA-256）历史格式。 |
| 凭据落盘 | 动态映射的源/目标密码经 `CryptoUtil`（AES）加密为 `ENC:...`，加载时解密；`DatabaseMapping` 序列化忽略未知字段以保证落盘成功。 |
| SQL 注入防护 | 库/表标识符白名单 `^[A-Za-z0-9_$]{1,64}$`（`assertSafeIdentifier`），DDL 拼接前强制校验，非法直接拒绝。 |
| 审计脱敏 | AOP 切面记录操作；`password` 等字段入库前脱敏为 `***`。 |

### 4.1 主机对与自动库发现
- 管理单元为"主机对"（源 MySQL 连接 + 目标 MySQL 连接，1:1）。
- `ScanNewDatabaseJob` 约每 60s 扫描源端 `information_schema`，排除系统库（`mysql`/`sys`/`information_schema`/`performance_schema`），对新库表自动建同步任务并全量同步。
- 系统库永不纳入；主机对可启用/停用；密码入库加密、API 返回脱敏。

### 4.2 全量 + 增量同步
- **全量**：首次或水位丢失时流式分页（`LIMIT` 分页，默认 1000 行/页），逐页 `INSERT ... ON DUPLICATE KEY UPDATE` 幂等 upsert，内存恒定。
- **增量**：含 `update_time` 的表按水位 `WHERE update_time > ?` 增量；单批上限 `batch-size * 5 = 5000`，超出通过 **keyset 续扫** `((update_time > wm) OR (update_time = wm AND pk > key)) ORDER BY update_time, pk LIMIT 5000` 续扫，**同时间戳大批量不丢**。
- 无 `update_time` 列仅全量模式；增量周期约 2s（poll-interval-ms）。

### 4.3 双模式忽略规则
- **DML+DDL 忽略**：跳过该库表所有数据变更与结构变更。
- **仅 DDL 忽略**：跳过结构变更，保留数据同步。
- 匹配：精确 `db.table`、glob `db_*.log_*`、正则 `re:^shard_\d+\..*`，可混用；规则按主机对隔离，库粒度优先。

### 4.4 跨版本 DDL 清洗
源 8.0 → 目标 5.7 时，`sanitizeDdlForPortability` 将专有语法降级：`utf8mb4_0900_ai_ci` → `utf8mb4_general_ci`、移除 `INVISIBLE`/`CHECK(...)`、表达式默认值替换等。清洗失败入死信队列并告警，不阻塞后续。

### 4.5 字段转换
写入目标前按字段映射变换：IP 替换 / 字段脱敏(MASK) / 枚举映射(MAP) / 替换(REPLACE)。分**全局映射**（yml 配置，所有用户生效）与**私有映射**（单主机对，仅创建者可见），私有优先；转换失败保留原值并 WARN。

### 4.6 Web 管理后台
React 18 UMD 单页，模块：`统计监控`（状态/IP/进度分页/按库列表，5s 轮询）、`配置中心`（主机对增删、连接测试、源库源表枚举、详情抽屉重同步）、`操作日志`（分页+多条件搜索）、登录/注册/登出、`双主题一键切换`（维度分层 Tokyo Night 默认 / Codex 翠绿）。前端经 Nginx 反代访问 REST API，鉴权基于 Session。

### 4.7 多用户隔离
账号体系：普通用户 / 管理员。配置按 `owner_id` 隔离；列表查询自动追加 `owner_id` 过滤；管理员可查看全部但不可改他人私有配置。

### 4.8 操作审计日志
AOP 切面自动记录（无需埋点）：`user_id/username/client_ip/operation_type/request_params(脱敏)/execution_result/error_message/duration_ms/created_at`。后台支持按用户、类型、结果、IP、时间范围筛选。

### 4.9 配置热生效
`/sync/mapping/update`（增量更新单主机对）、`/sync/mapping/reload`（全量重载）。更新在内存生效，下次循环应用，不影响当前批次；变更自动写审计日志。

### 4.10 断电断网续传与最终一致
- 水位丢失 → 受影响表全量重扫 + 幂等 upsert 重建水位。
- 连接临时不可用 → HikariCP 自动重连；超时入死信队列。
- 死信队列超限 → 丢弃最旧 + 触发全量重扫。
- 进度持久化到元数据库，重启自动加载动态映射（`DRPlatform-dynamic-mappings.json`）。
- 源库可达性预检：源库不可达时 `/sync/status` 正确置 **ABNORMAL**（监控盲区已修复）。

### 4.11 DDL 自动同步
ALTER（结构指纹比对）、DROP（源端消失→目标端删）、CREATE（新增表→目标端建，DDL 经清洗）自动同步到目标端，周期约 60s，受忽略规则约束，失败入死信队列。

### 4.12 删除对账
`reconcileDeletes` 周期扫描目标端，删除"目标有、源无"的多余行（基于 PK），保证最终一致；大表首次全量对账默认跳过，单批默认 1000 行。

### 4.13 HA 主备切换检测
`DBHASwitchStatusJob` 检测源端主备切换（server_id / binlog 变化），切换后暂停任务 → 重建连接池 → 恢复并触发全量重扫。默认关闭，需在配置显式启用。

---

## 5. API 接口清单

> Base URL：`http://<host>:8899`，除 `/auth/**` 与静态资源外需登录（Session Cookie）。

### 5.1 鉴权（`/auth`）
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/auth/register` | 注册（密码 PBKDF2 加盐） |
| POST | `/auth/login` | 登录，写 Session |
| POST | `/auth/logout` | 登出 |
| GET  | `/auth/me` | 登录态探测（恒 200） |

### 5.2 同步配置 / 监控 / 元数据（`/sync`）
| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/sync/mappings` | 主机对列表（密码脱敏） |
| POST | `/sync/mapping/add` | 新增主机对 |
| POST | `/sync/mapping/remove` | 移除动态映射 |
| POST | `/sync/mapping/reload` | 热重载配置 |
| POST | `/sync/mapping/update` | 增量更新配置（热生效） |
| POST | `/sync/mapping/test` | 源/目标连通性测试 |
| POST | `/sync/sourceDatabases` | 源端用户库列表 |
| POST | `/sync/sourceTables` | 指定库用户表列表 |
| GET  | `/sync/databases/{ips}` | 按 IP 获取用户库列表 |
| GET  | `/sync/ipList` | 生产/灾备 IP 列表 |
| POST | `/sync/db/list` | **分页查询同步进度** |
| GET  | `/sync/{id}` | 同步详情 |
| GET  | `/sync/status` | 引擎整体状态（含 ABNORMAL） |
| POST | `/sync/resyncDatabases` | 触发全量重同步 |

### 5.3 审计（`/operation-log`）
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/operation-log/list` | 分页 + 多条件查询 |
| GET  | `/operation-log/types` | 操作类型枚举 |

### 5.4 健康检查（Actuator，仅 health/info/metrics）
| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/actuator/health` | 健康检查（show-details: always） |
| GET | `/actuator/info` | 基本信息 |
| GET | `/actuator/metrics` | 运行指标 |

---

## 6. 配置参考

| 配置项 | 默认 | 说明 |
|--------|------|------|
| `server.port` | `8899` | 服务端口（jar / linux profile / watchdog 一致） |
| `drplatform.standby-mode` | `true` | 启动同步引擎 |
| `drplatform.flink.enabled` | `false` | 不提交真实 Flink 集群（用内嵌引擎） |
| `drplatform.engine.poll-interval-ms` | `2000` | 轮询间隔 |
| `drplatform.engine.batch-size` | `1000` | 单批行数（增量单批 = ×5 = 5000） |
| `drplatform.engine.max-queue-size` | `20000` | 死信队列上限 |
| `drplatform.engine.reconcile-interval-ms` | `10000` | 对账间隔 |
| `drplatform.engine.deviation-timeout-sec` | `300` | 偏差超时告警 |
| `drplatform.connection.max-pool-size` | `32` | 单主机连接池上限 |
| `drplatform.connection.max-global-connections` | `200` | 全局连接信号量 |
| `drplatform.mappings[]` | — | 主机对（源/目标连接 + 忽略规则 + 转换规则） |
| `app.auth.enabled` | `true` | 是否启用登录鉴权 |
| `app.cors.allowed-origins` | `""` | 允许跨域来源（空=仅同源，禁止 `*`） |
| 元数据库 | `DRPlatform` @ `127.0.0.1:3306` | 默认连接（root/123456，可改） |

---

## 7. 部署架构

- **单机部署**：`systemd` 托管 `DRPlatform.service`（运行 `watchdog.sh`）+ Nginx 反代 `:8899`。
- **Watchdog 守护**：`watchdog.sh` 轮询监听 jar 进程/端口，应用崩溃或假死自动拉起；收 SIGTERM 以 0 退出，`systemd Restart=on-failure` 不触发重启 → 实现"手动停止不自动拉起"。
- **开机自启**：`boot.sh {enable|disable|status}`（systemd 优先，否则 crontab `@reboot` 兜底）。
- **一键脚本**：`install.sh`（交互式生成 `application-linux.yml`）、`deploy.sh`（CentOS7 一键装 Java/Nginx/MySQL 客户端 + 布署 + systemd + Nginx）、`setDRInfo.sh`（改生产/灾备 IP 并重启）。
- **防火墙**：需放行 `8899/tcp`（Nginx 反代内部指向 `127.0.0.1:8899`）。

---

## 8. 非功能需求与验证

| 指标 | 目标 | 验证方式 |
|------|------|----------|
| 同步延迟 | P99 < 3s | 进度偏差字段统计 |
| 新增库发现 | < 2 分钟 | 60s 扫描 + 2s 轮询 |
| 故障恢复 | < 30s（重连 + 任务恢复） | 进度持久化 + 动态映射加载 |
| 数据最终一致 | 源/目标行数一致 | `test_17` 故障注入（源/目标/服务断电重启）全 5/5 PASS |
| 安全基线 | 无明文凭据 / 弱哈希 / `*` CORS | `test_18` + `SecurityHardeningTest` 全 PASS |

**测试证据**：Python 集成 **253** 用例（19 模块）+ JUnit **17** 方法。关键回归：`test_17`（故障注入一致性，源库/目标库/服务重启 + 多轮稳定性）、`test_18`（CORS 非通配符、密码密文落盘、监控盲区 ABNORMAL、同时间戳 5200 行不丢、元数据注入拦截）、`SecurityHardeningTest`（SQL 注入白名单 / CryptoUtil / PBKDF2）。

---

## 9. 技术风险与缓解

| 风险 | 可能性 | 缓解 |
|------|--------|------|
| 8.0→5.7 DDL 仍有未覆盖语法 | 中 | 持续维护清洗规则集，未覆盖 DDL 入死信队列并告警 |
| 单机性能瓶颈 | 低 | `max-global-connections` 信号量限流 |
| 大表全量占用资源 | 中 | 流式分页 + 增量水位，大表首次全量对账跳过 |
| 死信积压影响恢复 | 低 | 上限 20000，超限触发全量重扫补偿 |
| 凭据/弱哈希泄露 | 低（已加固） | PBKDF2 + 落盘 AES + 注入白名单 + 审计脱敏 |
| 部署端口不一致 | 低 | 统一 8899；`deploy.sh` Nginx 反代需核对 |

---

*产品规格说明书结束。*
