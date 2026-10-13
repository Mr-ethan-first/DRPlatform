# DRPlatform 使用说明书

> 面向运维 / DBA / 开发，覆盖从上线到日常运维的全流程操作。更完整的安装步骤见 [安装部署指南](DRPlatform-安装部署指南.md)，功能与设计见 [产品规格说明书](DRPlatform-产品规格说明书.md)。
>
> **默认端口 `8899`**；**默认管理员 `admin / admin123`**（首次登录后请立即修改密码）。

---

## 1. 部署上线

### 1.1 环境要求
- 操作系统：CentOS 7.9（生产验证）/ Ubuntu 18.04+ / Rocky 8·9（兼容）
- JDK 17+（运行）、MySQL 5.7+（源端与目标端可同机或异机）
- Nginx 1.18+（反向代理 + 静态资源）
- 网络可达源端 / 目标端 MySQL 的 3306 端口
- 源端 MySQL 建议开启 binlog（`binlog_format=ROW`，用于位点采集与 HA 检测）

### 1.2 构建（开发机）
```bash
cd remote-data-sync
bash /d/WorkSpace/flink-cdc-sync/mvn3.sh -o clean package -DskipTests
# 产物：target/remote-data-sync.jar（前端 app.js 已内嵌 static/）
```

### 1.3 一键部署（目标机 Linux）
```bash
sudo bash remote-data-sync/deploy/deploy.sh     # 装依赖 + 布 jar/前端 + 初始化元库 + systemd + Nginx
# 或交互式安装（输入 MySQL 账号、后端端口默认 8899、源/目标 IP）
sudo bash remote-data-sync/deploy/install.sh
```
`install.sh` 会生成 `/data/DRPlatform/conf/application-linux.yml` 并启动服务。

### 1.4 防火墙与访问
- 放行端口：`firewall-cmd --permanent --add-port=8899/tcp && firewall-cmd --reload`
- 访问地址：`http://<服务器IP>:8899/`
- ⚠️ Nginx 反代（`deploy/deploy.sh` 内）需指向 `127.0.0.1:8899`，若写死 `8080` 需改为 `8899` 后 `nginx -s reload`。

---

## 2. 服务管理

DRPlatform 由 **Watchdog 守护进程**托管，实现宕机自愈与开机自启。

| 操作 | 命令 | 说明 |
|------|------|------|
| 启动（并开启开机自启） | `bash remote-data-sync/deploy/start.sh` | 优先 systemd，否则 nohup 拉起 watchdog |
| 停止 | `bash remote-data-sync/deploy/stop.sh` | 先停 watchdog（使其失去拉起能力）再停应用 → **手动停止不会被自动拉回** |
| 开机自启 | `bash remote-data-sync/deploy/boot.sh enable\|disable\|status` | systemd 优先，失败回退 crontab `@reboot` |
| systemd 原生 | `systemctl start\|stop\|restart\|status DRPlatform` | 查看状态 / 日志 |
| 实时日志 | `journalctl -u DRPlatform -f` 或 `tail -f /data/DRPlatform/logs/app.log` | — |

> **"手动停止不自动拉起"原理**：`stop.sh` 先终止 watchdog；watchdog 收到 SIGTERM 以 0 退出，`systemd Restart=on-failure` 不触发重启。应用自身崩溃时 watchdog 仍在 → 自动拉起。

---

## 3. 配置主机对（Web 后台）

1. **登录**：浏览器打开 `:8899/` → 输入 `admin / admin123` → 进入管理后台。
2. **配置中心 → 新增主机对**：
   - 填写源端 / 目标端 MySQL 连接（host / port / user / password）。
   - 点击 **测试连接** 校验可达性（调用 `/sync/mapping/test`）。
   - 配置 **整库忽略**（`ignore-databases`，支持 `re:^tmp_.*`）、**表忽略**（`ignore-tables`，DML+DDL 均跳过）、**仅 DDL 忽略**（`ignore-ddl-tables`）。
   - 配置 **字段转换规则**（`transform-rules`：库/表/列 + 类型 MASK/REPLACE/MAP + 源值→目标值）。
   - 保存 → 引擎立即触发发现并启动全量同步。
3. **源库 / 源表枚举**：在表单中点击枚举（调用 `/sync/sourceDatabases`、`/sync/sourceTables`）选择需同步的库表。
4. **已配置主机对列表**：`/sync/mappings` 展示，密码脱敏。
5. **详情抽屉**：点击某主机对 → 查看进度、配置 **一键重同步**（`/sync/resyncDatabases` 全量重扫）。

### 忽略规则语法
| 语法 | 示例 | 说明 |
|------|------|------|
| 精确 | `db_name.table_name` | 完全匹配 |
| Glob | `db_*.log_*` | `*` 通配 |
| 正则 | `re:^shard_\d+\..*` | `re:` 前缀触发正则 |

---

## 4. 监控与运维

- **统计监控页**（5s 自动刷新）：同步总状态、生产/灾备 IP、进度分页（`/sync/db/list`）、按库数据库列表（`/sync/databases/{ips}`）。
- **同步状态 `/sync/status`**：
  - `NORMAL`：正常同步。
  - `ABNORMAL`：源库或目标库不可达（含源库断电场景，已通过可达性预检正确判定）。
  - 恢复后自动回到 `NORMAL`。
- **操作日志页**：审计所有操作，支持按用户名 / 类型 / 结果 / IP / 时间段筛选（`/operation-log/list`）。
- **一键重同步**：对某库强制全量重扫，用于数据校正或故障后补偿。

---

## 5. 安全运维

- **修改默认密码**：登录后在用户管理注册新管理员并删除默认 `admin`，或在 `sys_user` 表重置（密码哈希为 `pbkdf2$120000$salt$hash`，需经 `PasswordUtil` 生成，不建议手工写库）。
- **CORS 来源**：如前端通过独立域名访问，在 `application-linux.yml` 配置 `app.cors.allowed-origins: "https://your-domain"`（逗号分隔）；**切勿配置 `*`**，否则与凭据模式冲突且存在 CSRF 风险。
- **凭据落盘**：动态新增的主机对密码以 `ENC:` 密文存于 `DRPlatform-dynamic-mappings.json`，确认文件中无明文密码即为加密生效。
- **审计脱敏**：所有操作日志中 `password` 等字段已脱敏为 `***`。

---

## 6. 故障排查

| 现象 | 排查 |
|------|------|
| 服务起不来 | `journalctl -u DRPlatform -n 100`；检查 MySQL 凭据、`8899` 端口占用（`netstat -tlnp \| grep 8899`）、Java ≥ 17 |
| 外部访问 `:8899` 不通，但本机 `curl 127.0.0.1:8899` 通 | 防火墙未放行 `8899/tcp`；或 Nginx 反代端口写死 `8080`（需改 `8899` 并 `nginx -s reload`） |
| 同步延迟高 | `/sync/status` 与 `/sync/db/list` 看偏差；大表全量阻塞→加 `ignore-databases`；目标端压力大→降 `engine.batch-size` |
| 状态显示 ABNORMAL | 源/目标库不可达；恢复连接后自动回 NORMAL；若长时间不恢复检查网络与 MySQL 状态 |
| 断电 / 重启后数据不一致 | 正常恢复路径为全量重扫 + 幂等 upsert；观察进度回到一致；可用"一键重同步"强制补偿 |
| 忘记管理员密码 | `USE DRPlatform; DELETE FROM sys_user WHERE USERNAME='admin';` 重启后 `SysUserInitializer` 重建默认账号 |

---

## 7. 卸载
```bash
bash remote-data-sync/deploy/stop.sh
systemctl disable DRPlatform
rm -f /etc/systemd/system/DRPlatform.service /etc/systemd/system/DRPlatform-watchdog.service
rm -rf /data/DRPlatform
systemctl daemon-reload
# 元数据库：DROP DATABASE DRPlatform;（如需清理）
```

---

## 8. 常见问题（FAQ）

**Q1：支持双向同步吗？**
A：不支持。DRPlatform 定位为 1:1 异地灾备（源→目标），双向会引入冲突，不在范围内。

**Q2：必须依赖 Flink 吗？**
A：不依赖。同步引擎为自研内嵌轮询水位引擎（`drplatform.flink.enabled=false`），无需额外组件。

**Q3：源端和目标端可以是不同 MySQL 大版本吗？**
A：可以。5.7 与 8.0 任意方向，跨版本时 8.0 专有 DDL 自动降级为 5.7 兼容语法。

**Q4：新增的库表会自动同步吗？**
A：会。约 60s 内自动发现并启动全量同步，无需人工介入。

**Q5：配置修改需要重启吗？**
A：不需要。忽略规则、字段转换、连接参数等可通过 Web 后台或 `/sync/mapping/update`、`/sync/mapping/reload` 热生效。

**Q6：端口被占用（如 8080 被其他应用占用）怎么办？**
A：修改 `server.port`（jar / `application-linux.yml` / `watchdog.sh` 的 `DRPLATFORM_PORT`）与 Nginx 反代为统一端口，并放行对应防火墙端口。

---

*使用说明书结束。*
