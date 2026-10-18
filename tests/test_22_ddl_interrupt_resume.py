# -*- coding: utf-8 -*-
"""DRPlatform DDL 同步「中断—续传」测试（隔离实验室）。

场景：DDL 处理链路是「删除持久化水位 → 驱逐旧 Sink → DROP 目标表 → 按新结构重建 → 全量重扫回填」。
若同步进程**恰好在回填过程中被杀掉**（断电/崩溃），重启后必须把目标表完整回填，
不能留下「表还在、数据却是残缺的」这种隐性丢数。

<b>如何保证真的打断</b>：
  - 使用专用表 {@code t_r22_probe} 并灌入 8000 行；
  - 启动应用时把 batch-size 调小（--drplatform.engine.batch-size=200 → 每页 1000 行，共 8 页），
    显著拉长「DROP 之后、全量回填完成之前」的窗口；
  - 持续高频轮询目标端行数，**一旦出现 tgt < src（即回填只进行了一半）立刻杀进程**；
  - 断言杀进程瞬间确实处于「未回填完」状态，确保本测试不是假通过。

覆盖断言：
  1. 打断瞬间确实处于残缺状态（tgt < src）
  2. 重启后目标表被完整回填（tgt == src）
  3. DDL 前已同步的数据一条不少
  4. DDL 新增列的结构与数据都正确
  5. 续传后 增/改/删 仍正常

环境：源 13306 / 目标 13307 / 应用 127.0.0.1:8899（dockerlab profile）
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
TGT = dict(host="127.0.0.1", port=13307, user="root", password="123456")
ADMIN_USER = os.environ.get("DRPLATFORM_ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("DRPLATFORM_ADMIN_PASS", "admin123")
APP_JAR = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync\target\remote-data-sync.jar"
APP_CWD = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync"
APP_LOG = r"D:\WorkSpace\flink-cdc-sync\tests\docker-lab\app-lab-22.log"
JAVA = r"D:\software\jdk-21.0.11\bin\java.exe"
SRC_CTR = "drp-src-mysql"
TGT_CTR = "drp-tgt-mysql"
COMPOSE = r"D:\WorkSpace\flink-cdc-sync\tests\docker-lab\docker-compose.yml"
DB = "geo_source"
PREFIX = "R22_"
PROBE = "t_r22_probe"
NEW_COL = "r22_note"
ROWS = 8000
# 调小 batch 以拉长「DROP 后 / 回填完成前」的窗口
APP_EXTRA_ARGS = ["--drplatform.engine.batch-size=200"]
ALTER_LOG_MARK = f"DDL ALTER detected geo_source.{PROBE}"

APP_PROC = None


# ----------------------------- 基础设施 -----------------------------

def _run_native(cmd, timeout=30):
    r = subprocess.run(cmd, capture_output=True, timeout=timeout)
    return r.returncode, r.stdout.decode("utf-8", "replace"), r.stderr.decode("utf-8", "replace")


def find_app_pid():
    rc, out, _ = _run_native(["netstat", "-ano", "-p", "TCP"])
    if rc == 0 and out:
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 5 and parts[0].upper() == "TCP":
                if parts[3].upper() == "LISTENING" and parts[1].rsplit(":", 1)[-1] == "8899":
                    try:
                        return int(parts[4])
                    except ValueError:
                        continue
        return None
    ps = (r"Get-NetTCPConnection -LocalPort 8899 -State Listen -ErrorAction SilentlyContinue "
          r"| Select-Object -ExpandProperty OwningProcess")
    _, out, _ = _run_native(["powershell", "-NoProfile", "-Command", ps])
    for line in out.splitlines():
        if line.strip().isdigit():
            return int(line.strip())
    return None


def _kill_pid(pid):
    if not pid:
        return
    rc, _, _e = _run_native(["powershell", "-NoProfile", "-Command", f"Stop-Process -Id {pid} -Force"])
    if rc != 0:
        _run_native(["taskkill", "/F", "/PID", str(pid)])
    time.sleep(1)


def stop_app(timeout=60):
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


def _direct_session():
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


def start_app():
    global APP_PROC
    env = dict(os.environ, SPRING_PROFILES_ACTIVE="dockerlab")
    env.pop("SERVER__PORT", None)
    env.pop("SERVER_PORT", None)
    for _ in range(10):
        occ = find_app_pid()
        if occ is None:
            break
        _kill_pid(occ)
    with open(APP_LOG, "a", encoding="utf-8") as log:
        APP_PROC = subprocess.Popen([JAVA, "-jar", APP_JAR, "--server.port=8899"] + APP_EXTRA_ARGS,
                                     cwd=APP_CWD, env=env, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 180
    while time.time() < deadline:
        time.sleep(3)
        if APP_PROC.poll() is not None:
            raise RuntimeError(f"同步服务进程意外退出（exit={APP_PROC.poll()}），见 {APP_LOG}")
        try:
            _login()
        except Exception:
            continue
        holder = find_app_pid()
        if holder is not None and holder != APP_PROC.pid:
            raise RuntimeError(f"8899 监听进程 {holder} 与本次启动的 {APP_PROC.pid} 不一致")
        return True
    raise RuntimeError("同步服务启动超时（180s）")


def app_alive():
    try:
        _direct_session().get(f"{LAB_BASE_URL}/sync/status", timeout=10)
        return True
    except Exception:
        return False


def log_size():
    try:
        return os.path.getsize(APP_LOG)
    except OSError:
        return 0


def log_contains(mark, offset):
    try:
        with open(APP_LOG, "r", encoding="utf-8", errors="replace") as f:
            f.seek(offset)
            return mark in f.read()
    except Exception:
        return False


# ----------------------------- DB 辅助 -----------------------------

def docker(args, timeout=180):
    r = subprocess.run(["docker"] + args, capture_output=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} 失败: {r.stderr.decode('utf-8','replace').strip()}")
    return r.stdout.decode("utf-8", "replace").strip()


def db_conn(c, db=None, timeout=10):
    last = None
    for _ in range(6):
        try:
            return pymysql.connect(host=c["host"], port=c["port"], user=c["user"], password=c["password"],
                                   database=db, charset="utf8mb4",
                                   connect_timeout=timeout, read_timeout=60, write_timeout=60)
        except Exception as e:
            last = e
            time.sleep(2)
    raise last


def db_exec(c, sql, db=None, args=None):
    conn = db_conn(c, db)
    try:
        with conn.cursor() as cur:
            if args:      # 无参数时传空元组会把 LIKE 'xx%' 的 % 当占位符
                cur.execute(sql, args)
            else:
                cur.execute(sql)
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()


def db_query(c, sql, db=None, args=None):
    conn = db_conn(c, db)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            if args:
                cur.execute(sql, args)
            else:
                cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


def wait_mysql(c, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            db_conn(c, timeout=3).close()
            return True
        except Exception:
            time.sleep(2)
    raise AssertionError(f"MySQL {c['host']}:{c['port']} 不可达")


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


def cnt(conn, table, where=None, args=None):
    sql = f"SELECT COUNT(1) AS c FROM `{table}`" + (f" WHERE {where}" if where else "")
    return int(db_query(conn, sql, DB, args)[0]["c"])


def safe_cnt(conn, table):
    """目标表可能被 DROP 重建，计数异常时返回 -1 而不抛出。"""
    try:
        return cnt(conn, table)
    except Exception:
        return -1


def column_exists(conn, table, col):
    return int(db_query(conn, "SELECT COUNT(1) AS c FROM information_schema.COLUMNS "
                              "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s",
                        None, (DB, table, col))[0]["c"]) > 0


def setup_probe_table():
    """在源库创建探针表并灌入 ROWS 行数据。"""
    db_exec(SRC, f"DROP TABLE IF EXISTS `{PROBE}`", DB)
    db_exec(SRC, f"CREATE TABLE `{PROBE}` ("
                 "id BIGINT NOT NULL AUTO_INCREMENT, "
                 "tag VARCHAR(64) NOT NULL, "
                 "val INT DEFAULT NULL, "
                 "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                 "PRIMARY KEY(id), UNIQUE KEY uk_tag(tag)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", DB)
    # 分批灌数据，避免单条 SQL 过大
    batch = 1000
    for i in range(0, ROWS, batch):
        vals = ",".join(["(%s,%s)"] * min(batch, ROWS - i))
        args = []
        for j in range(i, min(i + batch, ROWS)):
            args.extend([f"{PREFIX}TAG_{j}", j])
        db_exec(SRC, f"INSERT INTO `{PROBE}`(tag,val) VALUES {vals}", DB, tuple(args))


# ----------------------------- 测试 -----------------------------

def test_ddl_interrupt_resume():
    """DDL 回填过程中杀进程 → 重启 → 必须完整回填（验证崩溃安全的水位删除顺序）。"""
    # 1) 环境就绪 + 造大数据量
    docker(["start", SRC_CTR])
    docker(["start", TGT_CTR])
    wait_mysql(SRC)
    wait_mysql(TGT)
    for c in (SRC, TGT):
        try:
            db_exec(c, f"DROP TABLE IF EXISTS `{PROBE}`", DB)
        except Exception:
            pass
    stop_app()
    start_app()
    assert app_alive(), "同步服务应存活"

    setup_probe_table()
    print(f"[interrupt] 源端灌入 {ROWS} 行探针数据")

    # 2) 首次全量同步到目标端
    wait_for(lambda: safe_cnt(TGT, PROBE) == ROWS, 240, f"探针表首次全量同步（{ROWS} 行）")
    src_total = cnt(SRC, PROBE)
    print(f"[interrupt] DDL 前: 源={src_total} 行, 目标={cnt(TGT, PROBE)} 行")

    # 3) 执行 ALTER，并卡点等待引擎开始处理
    offset = log_size()
    db_exec(SRC, f"ALTER TABLE `{PROBE}` ADD COLUMN {NEW_COL} VARCHAR(64) NULL", DB)
    print("[interrupt] 已执行 ALTER TABLE ... ADD COLUMN")

    wait_for(lambda: log_contains(ALTER_LOG_MARK, offset), 90, "引擎开始处理 DDL")
    print("[interrupt] 引擎已开始处理 DDL，开始高频轮询目标行数…")

    # 4) 卡点杀进程：一旦目标端出现「未回填完」状态（tgt < src），立即杀进程
    interrupted = False
    kill_state = None
    deadline = time.time() + 120
    while time.time() < deadline:
        t = safe_cnt(TGT, PROBE)
        if 0 <= t < src_total:
            interrupted = True
            kill_state = t
            stop_app()
            break
        if t == src_total:
            # 本轮回填已完成；若日志显示刚发生过 ALTER，回填可能太快，稍等下一轮
            time.sleep(0.2)
        else:
            time.sleep(0.1)

    assert interrupted, f"未能在回填窗口内打断（目标端始终={kill_state}，源={src_total}）；" \
                       f"请下调 --drplatform.engine.batch-size 或增加探针行数以拉长窗口"
    assert not app_alive(), "进程杀死后服务应不可达"
    print(f"[interrupt] ✅ 已打断：杀进程瞬间目标端仅 {kill_state} 行 / 源端 {src_total} 行（未回填完）")

    # 5) 重启续传
    start_app()
    assert app_alive(), "重启后服务应恢复"
    print("[interrupt] 已重启，等待全量回填…")

    # 6) 核心断言：完整回填
    wait_for(lambda: safe_cnt(TGT, PROBE) == src_total, 300,
             f"重启后完整回填（目标 {src_total} 行）")
    print(f"[interrupt] ✅ 回填完成: 源={src_total} 行, 目标={cnt(TGT, PROBE)} 行")

    # 7) 结构与数据正确性
    assert column_exists(TGT, PROBE, NEW_COL), "中断续传后，新增列未同步到目标端"
    distinct = int(db_query(TGT, f"SELECT COUNT(DISTINCT tag) AS c FROM `{PROBE}`", DB)[0]["c"])
    assert distinct == src_total, f"回填后 tag 去重数不符: {distinct} != {src_total}"
    print(f"[interrupt] ✅ 结构与数据完整性校验通过（tag 去重 {distinct}/{src_total}）")

    # 8) 续传后数据增/改/删仍正常
    m = f"{PREFIX}NC_" + uuid.uuid4().hex[:8]
    val = "n-" + uuid.uuid4().hex[:6]
    db_exec(SRC, f"INSERT INTO `{PROBE}`(tag,val,{NEW_COL}) VALUES(%s,1,%s)", DB, (m, val))
    wait_for(lambda: str(db_query(TGT, f"SELECT {NEW_COL} AS v FROM `{PROBE}` WHERE tag=%s", DB, (m,))[0]["v"]) == val,
             180, "续传后新增列数据同步")
    db_exec(SRC, f"UPDATE `{PROBE}` SET val=777 WHERE tag=%s", DB, (m,))
    wait_for(lambda: int(db_query(TGT, f"SELECT val AS v FROM `{PROBE}` WHERE tag=%s", DB, (m,))[0]["v"]) == 777,
             180, "续传后更新同步")
    db_exec(SRC, f"DELETE FROM `{PROBE}` WHERE tag=%s", DB, (m,))
    wait_for(lambda: cnt(TGT, PROBE, "tag=%s", (m,)) == 0, 240, "续传后删除同步")
    print("[interrupt] ✅ 续传后 增/改/删 均正常")

    # 9) 收尾
    for c in (SRC, TGT):
        try:
            db_exec(c, f"DROP TABLE IF EXISTS `{PROBE}`", DB)
        except Exception:
            pass


if __name__ == "__main__":
    pytest.main(["-v", __file__])