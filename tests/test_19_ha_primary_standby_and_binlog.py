# -*- coding: utf-8 -*-
"""DRPlatform 主备切换 + binlog 失效 · 数据绝对一致性自动化测试（隔离实验室）。

本文件在 tests/docker-lab 搭建的隔离环境上运行：
  - 源主库    : 127.0.0.1:13306 (容器 drp-src-mysql)
  - 源备库    : 127.0.0.1:13308 (容器 drp-src-standby-mysql)  —— 主备切换演练用
  - 目标库    : 127.0.0.1:13307 (容器 drp-tgt-mysql)
  - 管控元库  : 127.0.0.1:3306/drplatform (宿主机本地，需提前执行 db/init.sql 建表)
  - 同步服务  : 本地 127.0.0.1:8899 (SPRING_PROFILES_ACTIVE=dockerlab，映射已配置 sourceStandbyHost=13308)

覆盖用户需求的两个新增场景：
  (A) 主备切换：源主库故障 → 调用 POST /sync/mapping/switchSource 把同步源切到备库，
      验证「已同步数据不丢、不重复；切换后新写入从备库继续同步到目标端（最终一致）」。
  (B) binlog 失效：对源主库执行 PURGE/RESET BINARY LOGS 模拟 binlog 被清理删除，
      验证基于 update_time 轮询的同步架构天然免疫 —— 清理后新变更仍正常同步（数据一致）。

核心断言（绝对一致性）：
  - 任何故障/切换下，目标端已同步数据不丢失、不重复（upsert 幂等 + 单调水位）。
  - 故障恢复 / 切换后，源端的新增/更新/删除最终都反映到目标端（最终一致性）。

前置：需先在具备 Docker 的环境执行
  docker compose -f tests/docker-lab/docker-compose.yml up -d
  # 管控面元数据库（宿主机 127.0.0.1:3306）需提前建表：
  #   mysql -uroot -p123456 < remote-data-sync/src/main/resources/db/init.sql
并先用 Maven 构建 jar：
  mvn -o package -DskipTests   (或去掉 -o 联网构建)
"""
import os
import time
import uuid
import subprocess
import pytest
import requests
import pymysql

LAB_BASE_URL = os.environ.get("DRPLATFORM_LAB_URL", "http://127.0.0.1:8899")
SRC = dict(host="127.0.0.1", port=13306, user="root", password="123456")
SRC_STANDBY = dict(host="127.0.0.1", port=13308, user="root", password="123456")
TGT = dict(host="127.0.0.1", port=13307, user="root", password="123456")
CONTROL = dict(host="127.0.0.1", port=3306, user="root", password="123456")
ADMIN_USER = os.environ.get("DRPLATFORM_ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("DRPLATFORM_ADMIN_PASS", "admin123")
APP_JAR = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync\target\remote-data-sync.jar"
APP_CWD = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync"
APP_LOG = r"D:\WorkSpace\flink-cdc-sync\tests\docker-lab\app-lab-19.log"
JAVA = r"D:\software\jdk-21.0.11\bin\java.exe"
SRC_CTR = "drp-src-mysql"
SRC_STANDBY_CTR = "drp-src-standby-mysql"
TGT_CTR = "drp-tgt-mysql"
# 主备切换用的实例 key：dockerlab 映射 source-host=127.0.0.1 -> target-host=127.0.0.1
INSTANCE_KEY = f"{SRC['host']}->{TGT['host']}"
APP_PROC = None

# ----------------------------- 基础设施辅助 -----------------------------

def docker(args, timeout=150):
    r = subprocess.run(["docker"] + args, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} 失败: {r.stderr.strip()}")
    return r.stdout.strip()


def db_conn(c, db=None, timeout=10):
    last = None
    for _ in range(6):
        try:
            return pymysql.connect(host=c["host"], port=c["port"], user=c["user"], password=c["password"],
                                   database=db, charset="utf8mb4",
                                   connect_timeout=timeout, read_timeout=30, write_timeout=30)
        except Exception as e:
            last = e
            time.sleep(2)
    raise last


def db_exec(c, sql, db=None, args=None):
    conn = db_conn(c, db)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args or ())
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()


def db_query(c, sql, db=None, args=None):
    conn = db_conn(c, db)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, args or ())
            return cur.fetchall()
    finally:
        conn.close()


def db_count(c, db, table, where=None):
    conn = db_conn(c, db)
    try:
        with conn.cursor() as cur:
            sql = f"SELECT COUNT(1) FROM `{table}`" + (f" WHERE {where}" if where else "")
            cur.execute(sql)
            return cur.fetchone()[0]
    except Exception:
        return -1
    finally:
        conn.close()


def db_count_in(c, db, table, col, values):
    """统计 col 在 values 列表中的行数（用于按标记集合精确计数，验证不丢不重复）。"""
    if not values:
        return 0
    conn = db_conn(c, db)
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(values))
            cur.execute(f"SELECT COUNT(1) FROM `{table}` WHERE `{col}` IN ({placeholders})", tuple(values))
            return cur.fetchone()[0]
    finally:
        conn.close()


def wait_for(fn, timeout, desc):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = fn()
            if last:
                return last
        except Exception:
            last = None
        time.sleep(2)
    raise AssertionError(f"等待「{desc}」超时（{timeout}s），最后结果: {last}")


def wait_mysql(c, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = db_conn(c, timeout=3)
            conn.close()
            return True
        except Exception:
            time.sleep(2)
    raise AssertionError(f"MySQL {c['host']}:{c['port']} 在 {timeout}s 内不可达")


def _run_native(cmd, timeout=30):
    """调用 Windows 原生命令。

    关键：绝不使用 text=True。中文 Windows 下 PowerShell 等按 GBK 输出，
    而本进程 stdout 编码为 UTF-8，text=True 会在 subprocess 的 reader 线程里抛
    UnicodeDecodeError —— 异常不冒泡，调用方只看到 stdout 为空，
    从而把「命令失败/有告警」误判为「进程不存在」（静默假阴性）。
    """
    r = subprocess.run(cmd, capture_output=True, timeout=timeout)
    return r.returncode, r.stdout.decode("utf-8", "replace"), r.stderr.decode("utf-8", "replace")


def find_app_pid():
    """查找监听 8899 的进程 PID。找不到返回 None。

    主路径用 netstat -ano（纯 ASCII，不受 GBK 编码影响）；netstat 不可用时回退 PowerShell。
    """
    rc, out, _ = _run_native(["netstat", "-ano", "-p", "TCP"])
    if rc == 0 and out:
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 5 and parts[0].upper() == "TCP":
                local, state = parts[1], parts[3].upper()
                if state == "LISTENING" and local.rsplit(":", 1)[-1] == "8899":
                    try:
                        return int(parts[4])
                    except ValueError:
                        continue
        return None
    ps = (r"Get-NetTCPConnection -LocalPort 8899 -State Listen -ErrorAction SilentlyContinue "
          r"| Select-Object -ExpandProperty OwningProcess")
    _, out, _ = _run_native(["powershell", "-NoProfile", "-Command", ps])
    for line in out.splitlines():
        line = line.strip()
        if line.isdigit():
            return int(line)
    return None


def _kill_pid(pid):
    """强杀 PID：先 Stop-Process，失败再退回 taskkill（list 形式，不过 shell）。"""
    if not pid:
        return
    rc, _, _err = _run_native(["powershell", "-NoProfile", "-Command",
                               f"Stop-Process -Id {pid} -Force"])
    if rc != 0:
        _run_native(["taskkill", "/F", "/PID", str(pid)])
    time.sleep(1)


def stop_app(timeout=60):
    """停止同步服务，确保 8899 确实无人监听后才返回 True（清理所有占用者）。"""
    global APP_PROC
    if APP_PROC is not None and APP_PROC.poll() is None:
        _kill_pid(APP_PROC.pid)
    deadline = time.time() + timeout
    while time.time() < deadline:
        pid = find_app_pid()
        if pid is not None:
            _kill_pid(pid)
            continue
        if APP_PROC is None or APP_PROC.poll() is not None:
            APP_PROC = None
            return True
        time.sleep(1)
    APP_PROC = None
    return find_app_pid() is None


def start_app():
    global APP_PROC
    env = dict(os.environ, SPRING_PROFILES_ACTIVE="dockerlab")
    env.pop("SERVER__PORT", None)
    env.pop("SERVER_PORT", None)
    # 启动前彻底清空 8899
    for _ in range(10):
        occ = find_app_pid()
        if occ is None:
            break
        _kill_pid(occ)
    with open(APP_LOG, "a", encoding="utf-8") as log:
        APP_PROC = subprocess.Popen([JAVA, "-jar", APP_JAR, "--server.port=8899"],
                                     cwd=APP_CWD, env=env, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 180
    while time.time() < deadline:
        time.sleep(3)
        if APP_PROC.poll() is not None:
            raise RuntimeError(f"同步服务进程意外退出（exit={APP_PROC.poll()}），见 {APP_LOG}")
        try:
            _login()
        except Exception:
            continue  # 应用尚未就绪，继续等待
        holder = find_app_pid()
        if holder is not None and holder != APP_PROC.pid:
            raise RuntimeError(f"8899 监听进程 {holder} 与本次启动的 {APP_PROC.pid} 不一致（存在残留实例）")
        return True
    raise RuntimeError("同步服务启动超时（180s）")


def _direct_session():
    """绕过本机 HTTP_PROXY 直连 127.0.0.1（本机 HTTP_PROXY 未配 no_proxy，会把 8899 也发给代理）。"""
    s = requests.Session()
    s.trust_env = False
    return s


def _login(base=LAB_BASE_URL):
    s = _direct_session()
    s.headers.update({"Content-Type": "application/json"})
    r = s.post(f"{base}/auth/login", json={"username": ADMIN_USER, "password": ADMIN_PASS}, timeout=20)
    assert r.status_code == 200, f"登录失败 {r.status_code}: {r.text[:200]}"
    assert r.json().get("success") is True
    return s


def app_alive():
    try:
        _direct_session().get(f"{LAB_BASE_URL}/sync/status", timeout=10)
        return True
    except Exception:
        return False


def ensure_watermark_table():
    """确保管控元库存在 sync_watermark 表（崩溃恢复/断点续传核心表；幂等建表）。"""
    ddl = """
    CREATE TABLE IF NOT EXISTS sync_watermark (
        ID BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        SOURCE_IP VARCHAR(45) NOT NULL,
        SOURCE_DB VARCHAR(64) NOT NULL,
        TABLE_NAME VARCHAR(64) NOT NULL,
        WATERMARK_TIME DATETIME NOT NULL,
        WATERMARK_KEY VARCHAR(255) DEFAULT NULL,
        UPDATE_TIME DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (ID),
        UNIQUE KEY IDX_WM (SOURCE_IP, SOURCE_DB, TABLE_NAME)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    conn = db_conn(CONTROL, "drplatform")
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
            conn.commit()
    finally:
        conn.close()


def marker(prefix):
    return prefix + uuid.uuid4().hex[:14]


# ----------------------------- 模块级准备 -----------------------------

@pytest.fixture(scope="module")
def session():
    # 1) 确保 docker 容器运行（含新增的源备库）
    try:
        docker(["inspect", "-f", "{{.State.Running}}", SRC_CTR])
    except Exception:
        docker(["compose", "-f", r"D:\WorkSpace\flink-cdc-sync\tests\docker-lab\docker-compose.yml", "up", "-d"])
    docker(["start", SRC_CTR])
    docker(["start", SRC_STANDBY_CTR])
    docker(["start", TGT_CTR])
    wait_mysql(SRC)
    wait_mysql(SRC_STANDBY)
    wait_mysql(TGT)
    # 2) 确保管控面一致性核心表存在
    try:
        ensure_watermark_table()
    except Exception as e:
        print(f"[warn] ensure_watermark_table failed: {e}")
    # 3) 启停同步服务（本测试全权托管）
    stop_app()
    start_app()
    s = _login()
    yield s
    # teardown：停 app，并重启源主库（若被测试 stop 过）以便实验室可重复运行
    try:
        stop_app()
    except Exception:
        pass
    try:
        docker(["start", SRC_CTR])
    except Exception:
        pass


# ----------------------------- 测试用例 -----------------------------

def test_05_binlog_purge_immune(session):
    """场景(B)：源库 binlog 被 PURGE/失效删除后，基于 update_time 轮询的同步仍正常（天然免疫）。

    验证：开启 binlog 的源主库，在 PURGE 前有数据已同步到目标；PURGE 后再写入的新数据
    仍能同步到目标（证明本架构不依赖 binlog，binlog 失效不影响数据一致性）。
    """
    # 前置：确认 binlog 已开启
    rows = db_query(SRC, "SHOW VARIABLES LIKE 'log_bin'")
    log_bin_on = any(r.get("Value") == "ON" for r in rows)
    assert log_bin_on, "源主库未开启 binlog（docker-compose 应配置 --log-bin）；无法验证 binlog 失效场景"

    # 阶段1：写入基线数据并同步到目标
    base = [marker("BLB_") for _ in range(6)]
    for m in base:
        db_exec(SRC, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,1.00,1,'bl-base')",
                "geo_source", (m,))
    wait_for(lambda: db_count_in(TGT, "geo_source", "t_order", "order_no", base) == len(base),
             60, "binlog 基线同步")

    # 阶段2：记录 PURGE 前 binlog 数量，并执行 PURGE BINARY LOGS 模拟 binlog 失效删除
    before = db_query(SRC, "SHOW BINARY LOGS")
    before_count = len(before)
    now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    db_exec(SRC, f"PURGE BINARY LOGS BEFORE '{now_str}'")
    after = db_query(SRC, "SHOW BINARY LOGS")
    after_count = len(after)
    print(f"[info] binlog PURGE: before={before_count} -> after={after_count}")
    # PURGE 后当前活跃 binlog 仍保留（至少 1 个），但历史 binlog 已被清理
    assert after_count >= 1, "PURGE 后 binlog 数量异常（应为 >=1）"

    # 阶段3：PURGE 后继续写入新数据，验证仍能同步（核心断言）
    new = [marker("BLN_") for _ in range(6)]
    for m in new:
        db_exec(SRC, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,1.00,1,'bl-new')",
                "geo_source", (m,))
    wait_for(lambda: db_count_in(TGT, "geo_source", "t_order", "order_no", new) == len(new),
             60, "binlog PURGE 后新数据同步")

    # 全量一致性：基线 + 新数据全部到达目标，且各自恰好 1 行（不丢、不重复）
    assert db_count_in(TGT, "geo_source", "t_order", "order_no", base) == len(base), \
        "binlog PURGE 前已同步的基线数据不应丢失"
    assert db_count_in(TGT, "geo_source", "t_order", "order_no", new) == len(new), \
        "binlog 失效删除后新数据必须仍能同步到目标（天然免疫）"

    # 清理
    db_exec(SRC, "DELETE FROM t_order WHERE order_no IN (" + ",".join(["%s"] * len(base + new)) + ")",
            "geo_source", tuple(base + new))
    db_exec(TGT, "DELETE FROM t_order WHERE order_no IN (" + ",".join(["%s"] * len(base + new)) + ")",
            "geo_source", tuple(base + new))


def test_06_primary_standby_switch(session):
    """场景(A)：源主库故障 → 主备切换（同步源切到备库），验证数据不丢、不重复、新写入续传。

    步骤：
      1) 主库写入基线并同步到目标；
      2) 将主库基线数据镜像到备库（模拟主备数据一致）；
      3) 停主库（模拟生产中心主库故障）；
      4) 调用 /sync/mapping/switchSource 把同步源切到备库；
      5) 备库写入新数据，验证继续同步到目标；
      6) 断言：目标端基线不丢不重复、新数据到达（绝对一致）。
    """
    # 1) 主库基线
    base = [marker("PBB_") for _ in range(6)]
    for m in base:
        db_exec(SRC, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,1.00,1,'pb-base')",
                "geo_source", (m,))
    wait_for(lambda: db_count_in(TGT, "geo_source", "t_order", "order_no", base) == len(base),
             60, "主库基线同步")
    # 2) 以「真实复制语义」镜像到备库：主键 id 与 update_time 原样保留。
    #    真实 MySQL 主备复制是逐行复制、保留主键的；若测试自造不同主键，
    #    就不符合真实主备切换前提（对账按主键比对），会产生假失败。
    rows = db_query(SRC, "SELECT id,order_no,user_id,amount,status,remark,update_time "
                         "FROM t_order WHERE order_no IN (" +
                     ",".join(["%s"] * len(base)) + ")", "geo_source", tuple(base))
    assert len(rows) == len(base), f"源主库基线行数异常: {len(rows)} != {len(base)}"
    for r in rows:
        db_exec(SRC_STANDBY,
                "INSERT INTO t_order(id,order_no,user_id,amount,status,remark,update_time) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s)", "geo_source",
                (r["id"], r["order_no"], r["user_id"], r["amount"],
                 r["status"], r["remark"], r["update_time"]))

    # 2b) 造一条「只在主库、尚未复制到备库」的行（模拟异步复制延迟），已同步到目标端。
    #     切换到落后的备库后，这条行在备库中不存在 —— 若无防误删保护会被 reconcileDeletes 永久删除。
    lag = marker("PBLAG_")
    db_exec(SRC, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,7.77,1,'pb-lag')",
            "geo_source", (lag,))
    wait_for(lambda: db_count_in(TGT, "geo_source", "t_order", "order_no", [lag]) == 1,
             60, "复制延迟行同步到目标")

    # 2c) 提升后的备库，其 AUTO_INCREMENT 必须超过原主库已分配的最大 id。
    #     真实「主库故障 + 备库提升」后新写入会延续旧主库的自增位点；若沿用备库自身的小自增，
    #     新行会拿到与原主库已用过的 id 相同的值，而灾备目标里已存在这些 id，
    #     upsert 按主键冲突就会把不同业务行互相覆盖 —— 那是测试装置的人为偏差，必须排除。
    max_id_rows = db_query(SRC, "SELECT MAX(id) AS m FROM t_order", "geo_source")
    master_max = int(max_id_rows[0]["m"] or 0)
    db_exec(SRC_STANDBY, f"ALTER TABLE t_order AUTO_INCREMENT = {master_max + 1000}", "geo_source")
    wait_mysql(SRC_STANDBY)

    # 3) 停主库（模拟生产中心主库故障）
    docker(["stop", SRC_CTR])
    try:
        assert app_alive(), "主库故障时同步服务应仍然存活可响应"

        # 4) 主备切换：同步源切到备库
        r = session.post(f"{LAB_BASE_URL}/sync/mapping/switchSource",
                         json={"instanceKey": INSTANCE_KEY}, timeout=30)
        assert r.status_code == 200, f"主备切换接口失败 {r.status_code}: {r.text[:300]}"
        body = r.json()
        assert body.get("success") is True, f"主备切换未成功: {body}"
        assert body.get("data", {}).get("switched") is True, f"switchSource 返回 switched!=true: {body}"

        # 切换后服务仍存活
        assert app_alive(), "主备切换后服务应仍然存活"

        # 5) 备库写入新数据（主库已停，若仍读主库则这些新数据无法同步 → 反证已切到备库）
        new = [marker("PBN_") for _ in range(6)]
        for m in new:
            db_exec(SRC_STANDBY, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,1.00,1,'pb-new')",
                    "geo_source", (m,))
        wait_for(lambda: db_count_in(TGT, "geo_source", "t_order", "order_no", new) == len(new),
                 90, "切换后备库新数据同步（证明已切到备库）")

        # 6) 绝对一致性断言
        # 6a) 基线数据在目标端不丢（切源前已同步，切源后幂等重扫不重复）
        assert db_count_in(TGT, "geo_source", "t_order", "order_no", base) == len(base), \
            "主备切换后目标端基线数据应保留且不重复"
        # 6b) 新数据到达目标端
        assert db_count_in(TGT, "geo_source", "t_order", "order_no", new) == len(new), \
            "切换后从备库写入的新数据必须同步到目标端"
        # 6c) 关键防误删断言：仅存在于原主库（备库尚未复制到）的行，
        #     切换到落后备库后不得被 reconcileDeletes 从目标端删除。
        assert db_count_in(TGT, "geo_source", "t_order", "order_no", [lag]) == 1, \
            "切换到落后的备库后，「备库尚未复制到但已同步」的行不得被误删（防误删保护失效）"
    finally:
        # 恢复：重启主库，便于实验室重复运行（注意：应用内存中的源地址仍为备库，下次 docker compose 重启服务即恢复）
        docker(["start", SRC_CTR])
    wait_mysql(SRC)

    # 清理：主库、备库、目标端都删除测试数据
    allm = base + new + [lag]
    ph = ",".join(["%s"] * len(allm))
    db_exec(SRC, f"DELETE FROM t_order WHERE order_no IN ({ph})", "geo_source", tuple(allm))
    db_exec(SRC_STANDBY, f"DELETE FROM t_order WHERE order_no IN ({ph})", "geo_source", tuple(allm))
    db_exec(TGT, f"DELETE FROM t_order WHERE order_no IN ({ph})", "geo_source", tuple(allm))


if __name__ == "__main__":
    pytest.main(["-v", __file__])
