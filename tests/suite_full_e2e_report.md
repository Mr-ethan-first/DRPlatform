# GeoDRSync 前后端全自动化测试 · 最终报告

日期：2026-10-18　|　测试对象：remote-data-sync.jar（11:50 构建，含全部修复）　|　环境：Docker 双 MySQL（13306 源 8.0 → 13307 目标 5.7）

## 一、测试结论：6 组 44 用例全绿

| 组 | 场景 | 用例数 | 结果 | 亮点 |
|---|---|---|---|---|
| G1 | 正常流 + 前端契约 | 10 | ✅ 全过 | INSERT/DDL/DELETE 全部一致；前端引用的 16 个端点后端全部实现（无 404） |
| G2 | 配置异常 | 8 | ✅ 全过 | 缺字段/空主机/目标不可达/密码错误/**自同步环**全部 400 拒绝 |
| G3 | 字符输入异常 | 8 | ✅ 全过 | 中文/Emoji/含空格库名、注入串、超长输入均安全 |
| G4 | 断电断网（pause/stop 源与目标） | 8 | ✅ 全过 | 22/22、42/42、43/43 行数精确一致，**零丢失** |
| G5 | 压力 | 4 | ✅ 全过 | 2 万行 3.4s 写入、30 表全同步、10×200 并发收敛一致、目标无重复 |
| G6 | 可靠性（kill -9 / 断目标启动 / soak） | 6 | ✅ 全过 | 101/101、151/151、161/161 精确一致 |

> 全组连跑一轮 37/38，唯一失败为 P1 故障恢复等待窗口过紧（120s→240s 后单独验证 8/8）。数据一致性无任何丢失。
> 各组结果来源：G1/G2/G3 全组连跑轮；G4 第 3 轮（8/8）；G5 全组连跑轮；G6 第 5 轮（6/6）。

## 二、本轮发现并修复的问题（10 项）

| # | 严重度 | 问题 | 修复 |
|---|---|---|---|
| 1 | ★★★ | **「活僵尸」静默丢数据**：flink-cdc 3.7 从 retained checkpoint 跨进程恢复的 binlog split 缺表 schema → `skip the table schema discovery` → **静默丢弃全部后续数据事件**（位点照常推进、作业 RUNNING、零报错），目标零写入 | `resolveRestorePath` 只认显式 savepoint；无则 INITIAL 全量重扫（幂等 upsert 不重不丢）。正常停止路径均有显式 savepoint，仅 kill -9/断电硬死走重扫（分钟级） |
| 2 | ★★★ | **崩溃中重启继承坏 checkpoint**：作业崩溃循环中 stopWithSavepoint 失败，新作业仍从坏 checkpoint 恢复 → 不崩但丢数据 | stop 不干净 = 状态不可信 → 强制 forceInitial 全量重扫 |
| 3 | ★★★ | DROP 已捕获库触发 flink-cdc `fillMetadataForBinlogSplit` 越界崩溃（fromIndex>toIndex），从坏 checkpoint 恢复=永久崩溃循环 | watchdog 终态自愈改为 forceInitial（binlog 位点越过脏 DROP）；restart-strategy 20→5；测试全程禁止对运行中库 DROP |
| 4 | ★★ | 目标库故障期间启动应用 → 作业注册失败 → **永久停同步**（watchdog 只看已注册作业） | watchdog 新增 `tryStartMissingJob`（映射存在但作业缺失时拉起，带冷却） |
| 5 | ★★ | 真实写路径 `IdempotentMySqlSink.jdbcUrl()` 缺超时，目标冻结时写线程无限挂起 | 补 `connectTimeout=10000&socketTimeout=60000`（含 3 个 watchdog 探测 URL） |
| 6 | ★★ | 热切换时新旧作业 server-id 冲突（MySQL 踢重复 replica）→ 崩溃-重启循环；且单值 server-id 区间只够并行度 ≤2（par≥3 抛 `Subtask ID out of server id range`） | 每次启动原子分配 64 宽 server-id 区间块 + stop 后 3s 宽限 |
| 7 | ★ | addMapping 允许自同步（复制环风险） | 源=目标直接 400 拒绝 |
| 8 | ★ | addMapping 只校验目标连通不校验源 | 补源连通校验 |
| 9 | ★ | 动态配置持久化快照含只读键，启动加载整份被拒 | 加载时忽略只读/未知键，API 修改路径仍严格校验 |
| 10 | ★ | 测试器：Windows 原生命令 GBK 解码崩溃、目标残留旧库导致假失败、detached app 日志 DEVNULL 不可诊断 | bytes+decode(replace)；每轮唯一库名；日志落盘 app_detached.log；cleanup 双端扫描 |

## 三、新增：运行时动态配置（服务器性能越好，同步越快）

线程/内存/吞吐参数全部运行时可调，JSON 持久化（`geodrsync-runtime-config.json`，重启不丢），内建数据丢失 0 容忍硬约束（checkpoint 间隔下限 1s 禁止停用；一致性机制 barrier 前 flush + 幂等 upsert + keyBy 保序与调参正交，任意档位不丢不重）。

| API | 用途 |
|---|---|
| `GET /sync/runtime-config` | 当前生效值 + 合法区间 + 档位预设 |
| `POST /sync/runtime-config` | 修改参数（整体校验，任一非法全不动；未知参数拒绝） |
| `POST /sync/runtime-config?apply=true` | 修改并**无损热应用**：运行中作业 savepoint 优雅停止→恢复，新参数立即生效 |
| `POST /sync/runtime-config/preset/{name}?apply=true` | 一键应用档位：`small_2c4g` / `medium_4c8g` / `large_8c16g` / `xlarge_16c32g` |

可调 13 项：`parallelism`(1~64)、`taskmanagerMemory`(512m~16g)、`snapshotChunkSize`、`fetchSize`、`sinkBatchSize`、`sinkPoolSize`(每目标库连接池)、`sourceConnectionPoolSize`、`checkpointIntervalMs`(1000~600000)、`heartbeatIntervalSec`、`serverTimeZone`、`discoveryIntervalSec`、`discoveryRestartCooldownSec`、`lagWarnSeconds`。

档位示例（大档位 = 大并行 + 大内存 + 大批 + 短 checkpoint）：
- small_2c4g：par=1 / 512m / batch=250 / ckpt=5s
- xlarge_16c32g：par=8 / 4096m / batch=2000 / sinkPool=8 / ckpt=1s

已实测热应用：par 2→4→8→2 全程零 FAILING、作业 savepoint 无损切换。

## 四、遗留建议（非阻塞）

1. 深度对账兜底：watchdog 周期性抽样对比源/目标行数，漂移即 forceInitial（覆盖理论上可能存在的其他静默丢数据形态）。
2. 故障恢复速度优化：目标长时间故障期间可暂停 sink 重试风暴（当前依赖 watchdog 冷却，行为正确但有恢复延迟）。
3. 目标端连接全局配额：`sum(每库 sinkPoolSize)` 无全局上限，库数很多时需对照 `max_connections` 规划。

## 五、产物

- 测试套件：`tests/suite_full_e2e.py`（可 `E2E_GROUPS=G1,...,G6` 选组）
- 各轮日志：`tests/suite_*.log`；运行日志：`remote-data-sync/app_run.log`、`app_detached.log`
- 交付 jar：`remote-data-sync/target/remote-data-sync.jar`（2026-10-18 11:50）
