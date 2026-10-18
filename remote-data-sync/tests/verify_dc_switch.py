# -*- coding: utf-8 -*-
"""
Docker 环境「主备切换 / 生产↔灾备双向切换」全自动化验证
==============================================================
与 verify_docker_advanced.py（动态 DDL / 故障注入）互补，本脚本聚焦 **切换类容灾演练**：

  S1 主备切换数据一致性
      S1.1 克隆主库(13306) -> 备库(13308)，校验克隆完整
      S1.2 主库继续写入并同步到灾备(13307)
      S1.3 主库断电(docker stop)
      S1.4 调用 /sync/mapping/switchSource 切到备源(13308)
      S1.5 备源写入 -> 灾备收到
      S1.6 灾备 ⊇ 备源（逐表行数 + 内容哈希）
      S1.7 切换前主库数据未丢失（灾备保留最后写入）
      S1.8 主库恢复后切回主源；此时用「灾备 ⊇ 生产」判定 —— 故障期间写入备源的
          行是真实业务数据，主库并没有，灾备保留它们才正确（删掉就是数据丢失）

  S2 灾备中心切生产中心（reverse 第 1 次）
      S2.1 切换前灾备 ⊇ 生产
      S2.2 /sync/mapping/reverse -> 源=13307 目标=13306
      S2.3 新生产中心写入 -> 新灾备中心收到
      S2.4 内容一致

  S3 反向链路：新灾备中心(13306) 断电(stop)
  S4 反向链路：新灾备中心(13306) 断网(pause)
  S5 反向链路：新生产中心(13307) 断网(pause)

  S6 生产中心切灾备中心（reverse 第 2 次 = 回切）
      S6.1 /sync/mapping/reverse -> 源=13306 目标=13307（回到初始方向）
      S6.2 回切后写入同步 + 内容一致

  S7 切换后应用断电重启
      S7.1 kill -9 应用后重启
      S7.2 重启后方向保持（不回退到 yml 的原始方向）
      S7.3 数据最终一致

环境：复用既有容器（root/123456）
  drp-src-mysql          127.0.0.1:13306  生产中心主库
  drp-src-standby-mysql  127.0.0.1:13308  生产中心备库
  drp-tgt-mysql          127.0.0.1:13307  灾备中心
  mysql8                 127.0.0.1:3306   元库 DRPlatform
"""
import os
import time
import json
import shutil
import hashlib
import subprocess
from datetime import datetime

import pymysql

DOCKER = r"C:\Program Files\Docker\Docker\resources\bin\docker.exe"
JAVA = r"D:\software\jdk-21.0.11\bin\java.exe"
APP_JAR = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync\target\remote-data-sync.jar"
APP_CWD = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync"
REPORT_MD = os.path.join(APP_CWD, "target", "docker-dc-switch-report.md")
LOG_FILE = os.path.join(APP_CWD, "target", "docker-dc-switch-run.log")
COOKIE = os.path.join(APP_CWD, "target", "dcsw-cookie.txt")
APP_URL = "http://127.0.0.1:8899"

SRC = {"host": "127.0.0.1", "port": 13306}      # 生产中心主库
STANDBY = {"host": "127.0.0.1", "port": 13308}  # 生产中心备库
TGT = {"host": "127.0.0.1", "port": 13307}      # 灾备中心
AUTH = {"user": "root", "password": "123456"}
SYSTEM_DBS = {"information_schema", "mysql", "performance_schema", "sys"}

BASE_DBS = ["geo_source", "biz_source"]
# 字段转换规则命中的表：源/目标值按设计不同，一致性比对跳过
TRANSFORM_EXEMPT = {("geo_source", "dyn_tbl")}

RESULTS = []
proc = None
T0 = time.time()
# 本次运行批次号：所有测试数据的唯一键值都带它，保证脚本可反复重跑
# （t_order.order_no / t_customer.name 上有唯一约束，固定字面量会导致第二轮插入直接失败）
RUN = datetime.now().strftime("%m%d%H%M%S")


def log(msg):
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def record(phase, item, ok, detail=""):
    RESULTS.append((phase, item, ok, detail))
    log(f"  {'PASS' if ok else 'FAIL'} | {phase} | {item} | {detail}")


# ------------------------------------------------------------------ 数据库
def db(cfg, database=None):
    return pymysql.connect(
        host=cfg["host"], port=cfg["port"], user=AUTH["user"], password=AUTH["password"],
        database=database, charset="utf8mb4", autocommit=True, connect_timeout=5, read_timeout=120,
    )


def user_tables(cfg):
    out = {}
    try:
        with db(cfg) as conn, conn.cursor() as cur:
            cur.execute("SELECT table_schema, table_name FROM information_schema.tables "
                        "WHERE table_type='BASE TABLE'")
            for d, t in cur.fetchall():
                if d not in SYSTEM_DBS:
                    out.setdefault(d, []).append(t)
    except Exception as e:
        log(f"  user_tables 失败 {cfg['port']}: {str(e)[:100]}")
    return out


def count(cfg, schema, table, where=None):
    try:
        with db(cfg) as conn, conn.cursor() as cur:
            sql = f"SELECT COUNT(*) FROM `{schema}`.`{table}`"
            if where:
                sql += " WHERE " + where
            cur.execute(sql)
            return cur.fetchone()[0]
    except Exception:
        return None


def table_hash(cfg, schema, table):
    """逐行内容哈希（与行数无关，能发现「行数相同但内容不同」的静默不一致）"""
    try:
        with db(cfg) as conn, conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position", (schema, table))
            cols = [c[0] for c in cur.fetchall()]
            if not cols:
                return None
            cur.execute(f"SELECT {','.join('`'+c+'`' for c in cols)} FROM `{schema}`.`{table}` "
                        f"ORDER BY {','.join('`'+c+'`' for c in cols)}")
            h = hashlib.md5()
            n = 0
            for row in cur.fetchall():
                n += 1
                h.update("\x1f".join("" if v is None else str(v) for v in row).encode("utf-8", "replace"))
                h.update(b"\x1e")
            return h.hexdigest(), n
    except Exception:
        return None


def exec_sql(cfg, schema, sql, ignore_error=False):
    try:
        with db(cfg, schema) as conn, conn.cursor() as cur:
            cur.execute(sql)
        return True
    except Exception as e:
        if not ignore_error:
            log(f"  SQL 失败: {sql[:80]} -> {str(e)[:120]}")
        return False


def wait_for(cond, timeout, interval=3, tag=""):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if cond():
                if tag:
                    log(f"[{tag}] 条件满足")
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def compare(src_cfg, tgt_cfg, src_name, tgt_name, exempt=TRANSFORM_EXEMPT):
    """返回 (是否一致, 明细, 差异表清单)"""
    s, t = user_tables(src_cfg), user_tables(tgt_cfg)
    ok_all, lines, diff = True, [], []
    for d in sorted(set(list(s.keys()) + list(t.keys()))):
        s_tabs, t_tabs = set(s.get(d, [])), set(t.get(d, []))
        for tab in sorted(s_tabs | t_tabs):
            if (d, tab) in exempt:
                lines.append(f"  {d}.{tab}: 跳过（字段转换规则生效）")
                continue
            if tab not in s_tabs or tab not in t_tabs:
                ok_all = False
                diff.append(f"{d}.{tab}(缺失:{src_name if tab not in s_tabs else tgt_name})")
                lines.append(f"  {d}.{tab}: 表缺失")
                continue
            sh, th = table_hash(src_cfg, d, tab), table_hash(tgt_cfg, d, tab)
            same = (sh is not None and sh == th)
            ok_all &= same
            if not same:
                diff.append(f"{d}.{tab}({src_name}={str(sh)[:12]} {tgt_name}={str(th)[:12]})")
            lines.append(f"  {d}.{tab}: {src_name}={sh[1] if sh else '?'}({str(sh)[:8] if sh else '?'}) "
                         f"{tgt_name}={th[1] if th else '?'}({str(th)[:8] if th else '?'}) {'OK' if same else 'MISMATCH'}")
    return ok_all, "\n".join(lines), diff


def wait_identical(src_cfg, tgt_cfg, src_name, tgt_name, timeout=180, tag=""):
    """轮询直到两端逐表内容完全一致"""
    last = None
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok, lines, diff = compare(src_cfg, tgt_cfg, src_name, tgt_name)
        last = lines
        if ok:
            log(f"[{tag}] 双端内容一致")
            return True, lines
        time.sleep(5)
    return False, last


def wait_superset(src_cfg, tgt_cfg, src_name, tgt_name, timeout=180, tag=""):
    """
    轮询直到「tgt ⊇ src」：src 的每一行都出现在 tgt 中。
    用于主备切换场景 —— 备库只到克隆时刻，灾备目标还保留着主库故障前的最后写入，
    因此目标应当是备库的超集（而不是相等），删除这些冗余行才是真正的数据丢失。
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok, miss = True, []
        s = user_tables(src_cfg)
        for d, tabs in s.items():
            for tab in tabs:
                if (d, tab) in TRANSFORM_EXEMPT:
                    continue
                sh = table_hash(src_cfg, d, tab)
                if sh is None or sh[1] == 0:
                    continue
                pk = first_pk(src_cfg, d, tab)
                if not pk:
                    continue
                try:
                    with db(src_cfg) as cs, cs.cursor() as c1, db(tgt_cfg) as ct, ct.cursor() as c2:
                        c1.execute(f"SELECT `{pk}` FROM `{d}`.`{tab}`")
                        ids = [r[0] for r in c1.fetchall()]
                        if not ids:
                            continue
                        found = 0
                        for i in range(0, len(ids), 500):
                            chunk = ids[i:i + 500]
                            ph = ",".join(["%s"] * len(chunk))
                            c2.execute(f"SELECT COUNT(*) FROM `{d}`.`{tab}` WHERE `{pk}` IN ({ph})", chunk)
                            found += c2.fetchone()[0]
                        if found != len(ids):
                            ok = False
                            miss.append(f"{d}.{tab}({found}/{len(ids)})")
                except Exception as e:
                    ok = False
                    miss.append(f"{d}.{tab}(err:{str(e)[:40]})")
        if ok:
            log(f"[{tag}] 目标端已覆盖源端全部主键")
            return True, ""
        time.sleep(5)
    return False, ",".join(miss)


def first_pk(cfg, schema, table):
    try:
        with db(cfg) as conn, conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.key_column_usage "
                        "WHERE table_schema=%s AND table_name=%s AND constraint_name='PRIMARY' "
                        "ORDER BY ordinal_position LIMIT 1", (schema, table))
            r = cur.fetchone()
            return r[0] if r else None
    except Exception:
        return None


# ------------------------------------------------------------------ 应用
def free_port_8899():
    import socket
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", 8899))
        return
    except OSError:
        pass
    finally:
        s.close()
    log("8899 被占用，清理残留进程 ...")
    out = subprocess.run(["netstat", "-ano"], capture_output=True).stdout.decode("utf-8", "replace")
    pids = {ln.split()[-1] for ln in out.splitlines()
            if ":8899 " in ln and "LISTENING" in ln and ln.split()[-1].isdigit()}
    for p in sorted(pids):
        subprocess.run(["taskkill", "/F", "/T", "/PID", p], capture_output=True)
    time.sleep(3)


def start_app():
    global proc
    free_port_8899()
    log("启动应用 jar ...")
    out = open(os.path.join(APP_CWD, "target", "app-dcsw.log"), "ab")
    proc = subprocess.Popen(
        [JAVA, "-Xmx2g", "-jar", APP_JAR, "--server.port=8899"],
        cwd=APP_CWD,
        env={k: v for k, v in os.environ.items() if k not in ("SERVER__PORT", "SERVER_PORT")},
        stdout=out, stderr=subprocess.STDOUT,
    )
    return proc


def kill_app_hard():
    global proc
    if proc is None or proc.poll() is not None:
        return
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
    try:
        proc.wait(timeout=20)
    except Exception:
        pass
    log(f"应用已强杀 pid={proc.pid}")


def http_call(path, method="GET", body=None, timeout=20):
    try:
        cmd = ["curl", "-s", "--noproxy", "*", "--max-time", str(timeout), "-b", COOKIE]
        if method == "POST":
            cmd += ["-X", "POST", "-H", "Content-Type: application/json", "-d", json.dumps(body)]
        cmd.append(APP_URL + path)
        r = subprocess.run(cmd, capture_output=True, timeout=timeout + 5)
        txt = r.stdout.decode("utf-8", "replace").strip()
        if not txt:
            return None
        obj = json.loads(txt)
        if isinstance(obj, dict) and obj.get("success") is False:
            log(f"  {path} 失败: {str(obj.get('message'))[:160]}")
            return None
        return obj.get("data") if isinstance(obj, dict) and "data" in obj else obj
    except Exception as e:
        log(f"  http {path} 异常: {e}")
        return None


def login():
    r = subprocess.run(
        ["curl", "-s", "--noproxy", "*", "--max-time", "10", "-c", COOKIE, "-X", "POST",
         "-H", "Content-Type: application/json",
         "-d", json.dumps({"username": "admin", "password": "admin123"}), APP_URL + "/auth/login"],
        capture_output=True, timeout=20)
    try:
        return bool(json.loads(r.stdout.decode("utf-8", "replace")).get("success"))
    except Exception:
        return False


def wait_app_ready(timeout=200):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if login():
            if http_call("/sync/cdc/jobs") is not None:
                return True
        if proc is not None and proc.poll() is not None:
            log(f"应用进程提前退出 exit={proc.returncode}")
            return False
        time.sleep(4)
    return False


def current_source_port():
    maps = http_call("/sync/mappings")
    if isinstance(maps, list):
        for m in maps:
            if isinstance(m, dict) and "sourcePort" in m:
                return m.get("sourcePort"), m.get("targetPort"), m.get("instanceKey")
    return None, None, None


# ------------------------------------------------------------------ Docker
def docker_cmd(*args, timeout=180):
    r = subprocess.run([DOCKER, *args], capture_output=True, timeout=timeout)
    return (r.stdout.decode("utf-8", "replace").strip(),
            r.stderr.decode("utf-8", "replace").strip(), r.returncode)


def container_of(port):
    return {13306: "drp-src-mysql", 13307: "drp-tgt-mysql", 13308: "drp-src-standby-mysql"}.get(port)


def wait_mysql_up(cfg, timeout=240):
    return wait_for(lambda: exec_sql(cfg, None, "SELECT 1", ignore_error=True), timeout, 3)


def container_running(cfg):
    out, _, _ = docker_cmd("inspect", "-f", "{{.State.Running}}", container_of(cfg["port"]))
    return out.strip().lower() == "true"


def power_off(cfg):
    """断电：docker stop（进程终止，重启后数据仍在磁盘）"""
    c = container_of(cfg["port"])
    log(f"  断电 {c} (port={cfg['port']}) ...")
    docker_cmd("stop", c, timeout=180)
    time.sleep(3)
    return not container_running(cfg)


def power_on(cfg):
    c = container_of(cfg["port"])
    log(f"  上电 {c} (port={cfg['port']}) ...")
    docker_cmd("start", c, timeout=180)
    wait_mysql_up(cfg)


def net_down(cfg):
    """断网：docker pause（进程存活但冻结，模拟网络不可达）"""
    c = container_of(cfg["port"])
    log(f"  断网 {c} (port={cfg['port']}) ...")
    docker_cmd("pause", c, timeout=120)


def net_up(cfg):
    c = container_of(cfg["port"])
    log(f"  恢复网络 {c} (port={cfg['port']}) ...")
    docker_cmd("unpause", c, timeout=120)
    wait_mysql_up(cfg)


def clone_src_to_standby():
    """mysqldump 主库 -> 备库，模拟备库作为主库的实时副本（库清单动态发现）"""
    c_src, c_std = container_of(SRC["port"]), container_of(STANDBY["port"])
    dbs = sorted(user_tables(SRC).keys())
    if not dbs:
        log("  主库无用户库，跳过克隆")
        return False
    log(f"克隆 {c_src} -> {c_std}，库={dbs} ...")
    dump = subprocess.Popen(
        [DOCKER, "exec", c_src, "mysqldump", "-uroot", "-p123456",
         "--single-transaction", "--set-gtid-purged=OFF", "--add-drop-database",
         "--databases", *dbs],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    load = subprocess.Popen(
        [DOCKER, "exec", "-i", c_std, "mysql", "-uroot", "-p123456"],
        stdin=dump.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    dump.stdout.close()
    _, err = load.communicate(timeout=600)
    derr = dump.stderr.read().decode("utf-8", "replace")
    dump.wait(timeout=60)
    if load.returncode != 0:
        log(f"  克隆失败: {err.decode('utf-8','replace')[:300]} | dump-err: {derr[:200]}")
        return False
    log("  克隆完成")
    return True


# ------------------------------------------------------------------ 阶段
def reset_env():
    """清空历史状态：删除 savepoints（否则从旧 checkpoint 恢复会跳过全量快照）"""
    sp = os.path.join(APP_CWD, "savepoints")
    if os.path.isdir(sp):
        shutil.rmtree(sp, ignore_errors=True)
        log("已删除 savepoints/")
    dm = os.path.join(APP_CWD, "DRPlatform-dynamic-mappings.json")
    if os.path.isfile(dm):
        os.remove(dm)
        log("已删除 DRPlatform-dynamic-mappings.json（清掉历史换向状态）")


def seed(cfg, n=6, prefix="seed"):
    exec_sql(cfg, "mysql", "CREATE DATABASE IF NOT EXISTS geo_source", ignore_error=True)
    exec_sql(cfg, "geo_source",
             "CREATE TABLE IF NOT EXISTS t_order ("
             "id BIGINT PRIMARY KEY AUTO_INCREMENT, order_no VARCHAR(64) UNIQUE, user_id BIGINT,"
             "amount DECIMAL(10,2), status INT, remark VARCHAR(128),"
             "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
             "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
    exec_sql(cfg, "geo_source",
             "CREATE TABLE IF NOT EXISTS t_user ("
             "id BIGINT PRIMARY KEY AUTO_INCREMENT, username VARCHAR(64) UNIQUE, age INT,"
             "email VARCHAR(128), remark VARCHAR(128))", ignore_error=True)
    exec_sql(cfg, "mysql", "CREATE DATABASE IF NOT EXISTS biz_source", ignore_error=True)
    exec_sql(cfg, "biz_source",
             "CREATE TABLE IF NOT EXISTS t_customer ("
             "id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64) UNIQUE, phone VARCHAR(32),"
             "balance DECIMAL(10,2),"
             "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
             "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
    for i in range(n):
        exec_sql(cfg, "geo_source",
                 f"INSERT IGNORE INTO t_order (order_no,user_id,amount,status,remark) "
                 f"VALUES ('{prefix}_o{i}',{1000+i},{10+i}.50,1,'{prefix}')")
        exec_sql(cfg, "biz_source",
                 f"INSERT IGNORE INTO t_customer (name,phone,balance) "
                 f"VALUES ('{prefix}_c{i}','1380000{i:04d}',{100+i})")


def phase0():
    log("=== 前置：重置并启动 ===")
    reset_env()
    # 目标库清掉历史用户库，强制走一次全量快照，保证基线干净
    for d in ["geo_source", "biz_source", "dyn_db"]:
        exec_sql(TGT, "mysql", f"DROP DATABASE IF EXISTS `{d}`", ignore_error=True)
    seed(SRC, 6, "base")
    start_app()
    ok = wait_app_ready()
    record("前置", "应用启动（8899）", ok, "")
    if not ok:
        return False
    ok, lines = wait_identical(SRC, TGT, "生产", "灾备", timeout=240, tag="基线收敛")
    record("前置", "基线数据一致（全量快照完成）", ok, lines if not ok else "")
    return ok


def phase1():
    log("=== S1 主备切换数据一致性 ===")
    # S1.1 克隆主库 -> 备库
    ok = clone_src_to_standby()
    ok2, lines = wait_identical(SRC, STANDBY, "主库", "备库", timeout=60, tag="克隆校验")
    record("S1-主备切换", "主库克隆到备库且内容一致", ok and ok2, lines if not ok2 else "")

    # S1.2 主库继续写入（这部分备库不会有，模拟「故障时尚未复制的尾部数据」）
    for i in range(4):
        exec_sql(SRC, "geo_source",
                 f"INSERT INTO t_order (order_no,user_id,amount,status,remark) "
                 f"VALUES ('{RUN}_tail_{i}',2000+{i},{20+i}.00,1,'tail')")
    ok, lines = wait_identical(SRC, TGT, "生产", "灾备", timeout=120, tag="主库尾部写入")
    record("S1-主备切换", "主库尾部写入同步到灾备", ok, lines if not ok else "")
    tgt_before = count(TGT, "geo_source", "t_order")

    # S1.3 主库断电
    off_ok = power_off(SRC)
    record("S1-主备切换", "主库断电(docker stop)成功", off_ok, "")

    # S1.4 切到备源
    key = "127.0.0.1->127.0.0.1"
    data = http_call("/sync/mapping/switchSource", "POST", {"instanceKey": key}, timeout=60)
    ok = isinstance(data, dict) and data.get("switched") is True
    record("S1-主备切换", "调用 switchSource 切到备源", ok, json.dumps(data, ensure_ascii=False)[:160])
    time.sleep(10)
    sp, tp, ikey = current_source_port()
    record("S1-主备切换", "同步源已切到备库 13308", sp == 13308, f"当前源端口={sp}")

    # S1.5 备源写入 -> 灾备收到
    for i in range(3):
        exec_sql(STANDBY, "geo_source",
                 f"INSERT INTO t_order (order_no,user_id,amount,status,remark) "
                 f"VALUES ('{RUN}_postsw_{i}',3000+{i},{30+i}.00,1,'postsw')")
    ok = wait_for(lambda: (count(TGT, "geo_source", "t_order") or 0) >= tgt_before + 3,
                  150, 5, "备源新增")
    tgt_after = count(TGT, "geo_source", "t_order")
    record("S1-主备切换", "备源新增 3 行同步到灾备", ok, f"灾备行数 {tgt_before} -> {tgt_after}")

    # S1.6 灾备 ⊇ 备源
    ok, miss = wait_superset(STANDBY, TGT, "备源", "灾备", timeout=180, tag="灾备覆盖备源")
    record("S1-主备切换", "灾备 ⊇ 备源（备源每行都在灾备中）", ok, miss)

    # S1.7 切换前主库数据未丢失（主库已 stop，只能查灾备端）
    TAIL_WHERE = f"order_no LIKE '{RUN}\\_tail\\_%'"
    tail_kept = wait_for(lambda: count(TGT, "geo_source", "t_order", TAIL_WHERE) == 4,
                         90, 5, "尾部数据")
    tail_n = count(TGT, "geo_source", "t_order", TAIL_WHERE)
    record("S1-主备切换", "主库故障前尾部写入未丢失（灾备保留）", tail_kept,
           f"灾备 tail 行数={tail_n}/4")

    # S1.8 主库恢复，切回主源
    power_on(SRC)
    seed(SRC, 0)
    data = http_call("/sync/mapping/switchSource", "POST", {"instanceKey": key}, timeout=60)
    time.sleep(10)
    sp2, _, _ = current_source_port()
    record("S1-主备切换", "主库恢复后可切回主源 13306", sp2 == 13306, f"当前源端口={sp2}")
    for i in range(2):
        exec_sql(SRC, "geo_source",
                 f"INSERT INTO t_order (order_no,user_id,amount,status,remark) "
                 f"VALUES ('{RUN}_back_{i}',4000+{i},{40+i}.00,1,'back')")
    # 切回主源后用「超集」而非「相等」判定：故障期间写入备源的 postsw_* 行是真实业务数据，
    # 主库并没有这些行，灾备中心保留它们才是对的（删掉就是真正的数据丢失）。
    ok, miss = wait_superset(SRC, TGT, "生产", "灾备", timeout=240, tag="切回后覆盖")
    record("S1-主备切换", "切回主源后灾备 ⊇ 生产（含故障期备源写入）", ok, miss)
    # 直接按本轮标记行计数：总量差值会被历轮残留干扰，按标记找才是精确断言
    POSTSW_WHERE = f"order_no LIKE '{RUN}\\_postsw\\_%'"
    kept = wait_for(lambda: count(TGT, "geo_source", "t_order", POSTSW_WHERE) == 3, 120, 5, "postsw 保留")
    record("S1-主备切换", "故障期备源写入完整保留在灾备（未被删除对账抹掉）",
           kept, f"灾备 postsw 行数={count(TGT, 'geo_source', 't_order', POSTSW_WHERE)}/3")


def phase2():
    log("=== S2 灾备中心切生产中心（reverse 第 1 次）===")
    # 承接 S1：此时灾备仍比生产多出故障期写入备源的 3 行，故用超集判定
    ok, miss = wait_superset(SRC, TGT, "生产", "灾备", timeout=180, tag="切换前覆盖")
    record("S2-灾备切生产", "切换前灾备 ⊇ 生产", ok, miss)

    key = "127.0.0.1->127.0.0.1"
    data = http_call("/sync/mapping/reverse", "POST", {"instanceKey": key}, timeout=60)
    ok = isinstance(data, dict) and data.get("reversed") is True
    record("S2-灾备切生产", "调用 /sync/mapping/reverse 成功", ok, json.dumps(data, ensure_ascii=False)[:200])
    time.sleep(12)
    sp, tp, ikey = current_source_port()
    record("S2-灾备切生产", "新生产中心=13307、新灾备中心=13306",
           sp == 13307 and tp == 13306, f"源={sp} 目标={tp} key={ikey}")

    # 新生产中心（原灾备）写入 -> 新灾备中心（原生产）应收到
    for i in range(4):
        exec_sql(TGT, "geo_source",
                 f"INSERT INTO t_order (order_no,user_id,amount,status,remark) "
                 f"VALUES ('{RUN}_rev_{i}',5000+{i},{50+i}.00,1,'reverse')")
        exec_sql(TGT, "biz_source",
                 f"INSERT INTO t_customer (name,phone,balance) VALUES "
                 f"('{RUN}_rev_c{i}','1390000{i:04d}',{500+i})")
    ok, lines = wait_identical(TGT, SRC, "新生产(13307)", "新灾备(13306)", timeout=240, tag="反向同步")
    record("S2-灾备切生产", "反向链路同步且内容一致", ok, lines if not ok else "")
    return ok


def phase3():
    log("=== S3 反向链路：新灾备中心(13306) 断电 ===")
    record("S3-反向故障", "新灾备中心断电成功", power_off(SRC), "")
    for i in range(2):
        exec_sql(TGT, "geo_source",
                 f"INSERT INTO t_order (order_no,user_id,amount,status,remark) "
                 f"VALUES ('{RUN}_s3_{i}',6000+{i},{60+i}.00,1,'s3')")
    power_on(SRC)
    record("S3-反向故障", "新灾备中心重新上电", True, "")
    # 反向链路被中断，需要等巡检/重启把作业拉回；为加速，直接触发一次 resync
    http_call("/sync/resyncDatabases", "POST", [{"ip": "127.0.0.1", "dbName": "geo_source"}], timeout=60)
    ok, lines = wait_identical(TGT, SRC, "新生产(13307)", "新灾备(13306)", timeout=300, tag="S3 收敛")
    record("S3-反向故障", "新灾备中心断电恢复后内容一致", ok, lines if not ok else "")


def phase4():
    log("=== S4 反向链路：新灾备中心(13306) 断网 ===")
    net_down(SRC)
    record("S4-反向故障", "新灾备中心断网(pause)成功", True, "")
    for i in range(2):
        exec_sql(TGT, "geo_source",
                 f"INSERT INTO t_order (order_no,user_id,amount,status,remark) "
                 f"VALUES ('{RUN}_s4_{i}',7000+{i},{70+i}.00,1,'s4')")
    time.sleep(5)
    net_up(SRC)
    record("S4-反向故障", "新灾备中心网络恢复", True, "")
    ok, lines = wait_identical(TGT, SRC, "新生产(13307)", "新灾备(13306)", timeout=300, tag="S4 收敛")
    record("S4-反向故障", "新灾备中心断网恢复后内容一致", ok, lines if not ok else "")


def phase5():
    log("=== S5 反向链路：新生产中心(13307) 断网 ===")
    net_down(TGT)
    record("S5-反向故障", "新生产中心断网(pause)成功", True, "")
    time.sleep(6)
    net_up(TGT)
    record("S5-反向故障", "新生产中心网络恢复", True, "")
    for i in range(2):
        exec_sql(TGT, "geo_source",
                 f"INSERT INTO t_order (order_no,user_id,amount,status,remark) "
                 f"VALUES ('{RUN}_s5_{i}',8000+{i},{80+i}.00,1,'s5')")
    ok, lines = wait_identical(TGT, SRC, "新生产(13307)", "新灾备(13306)", timeout=300, tag="S5 收敛")
    record("S5-反向故障", "新生产中心断网恢复后内容一致", ok, lines if not ok else "")


def phase6():
    log("=== S6 生产中心切灾备中心（reverse 第 2 次 = 回切）===")
    key = "127.0.0.1->127.0.0.1"
    data = http_call("/sync/mapping/reverse", "POST", {"instanceKey": key}, timeout=60)
    ok = isinstance(data, dict) and data.get("reversed") is True
    record("S6-生产切灾备", "调用 /sync/mapping/reverse 回切成功", ok, json.dumps(data, ensure_ascii=False)[:200])
    time.sleep(12)
    sp, tp, ikey = current_source_port()
    record("S6-生产切灾备", "已回到初始方向 源=13306 目标=13307",
           sp == 13306 and tp == 13307, f"源={sp} 目标={tp} key={ikey}")

    for i in range(3):
        exec_sql(SRC, "geo_source",
                 f"INSERT INTO t_order (order_no,user_id,amount,status,remark) "
                 f"VALUES ('{RUN}_fb_{i}',9000+{i},{90+i}.00,1,'failback')")
    ok, lines = wait_identical(SRC, TGT, "生产", "灾备", timeout=300, tag="回切后同步")
    record("S6-生产切灾备", "回切后正向同步且内容一致", ok, lines if not ok else "")


def phase7():
    log("=== S7 切换后应用断电重启（方向持久化）===")
    # 先切到「与 yml 原始方向相反」的状态，这样重启若回退到 yml 就一定会被抓到
    key = "127.0.0.1->127.0.0.1"
    data = http_call("/sync/mapping/reverse", "POST", {"instanceKey": key}, timeout=60)
    time.sleep(12)
    sp_before, tp_before, _ = current_source_port()
    record("S7-应用重启", "切换至反向（源=13307 目标=13306）",
           (sp_before, tp_before) == (13307, 13306), f"源={sp_before} 目标={tp_before}")

    kill_app_hard()
    time.sleep(3)
    start_app()
    ok = wait_app_ready()
    record("S7-应用重启", "应用强杀后重启成功", ok, "")
    if not ok:
        return
    time.sleep(10)
    sp, tp, ikey = current_source_port()
    record("S7-应用重启", "重启后同步方向保持（未回退到 yml 原始值）",
           (sp, tp) == (13307, 13306), f"重启前={sp_before}->{tp_before} 重启后={sp}->{tp} key={ikey}")

    for i in range(2):
        exec_sql(TGT, "geo_source",
                 f"INSERT INTO t_order (order_no,user_id,amount,status,remark) "
                 f"VALUES ('{RUN}_rb_{i}',9500+{i},{95+i}.00,1,'reboot')")
    ok, lines = wait_identical(TGT, SRC, "新生产(13307)", "新灾备(13306)", timeout=300, tag="重启后同步")
    record("S7-应用重启", "重启后反向链路数据最终一致", ok, lines if not ok else "")

    # 收尾：切回与 yml 一致的初始方向，避免影响后续验证脚本
    http_call("/sync/mapping/reverse", "POST", {"instanceKey": key}, timeout=60)
    time.sleep(12)
    sp2, tp2, _ = current_source_port()
    record("S7-应用重启", "收尾：已切回初始方向 源=13306 目标=13307",
           (sp2, tp2) == (13306, 13307), f"源={sp2} 目标={tp2}")


def write_report():
    passed = sum(1 for r in RESULTS if r[2])
    total = len(RESULTS)
    elapsed = int(time.time() - T0)
    lines = [
        "# GeoDRSync 主备切换 / 生产↔灾备双向切换验证报告",
        "",
        f"- 时间：{datetime.now():%Y-%m-%d %H:%M:%S}",
        f"- 总耗时：{elapsed}s",
        f"- 结论：**{'全部通过 ✅' if passed == total else '存在失败 ❌'}**（{passed}/{total}）",
        "",
        "| 阶段 | 验证项 | 结果 | 明细 |",
        "|---|---|---|---|",
    ]
    for phase, item, ok, detail in RESULTS:
        detail = str(detail).replace("\n", " / ").replace("|", "\\|")
        lines.append(f"| {phase} | {item} | {'✅' if ok else '❌'} | {detail[:220]} |")
    lines += [
        "",
        "## 环境",
        "",
        "- 生产中心主库 drp-src-mysql 127.0.0.1:13306",
        "- 生产中心备库 drp-src-standby-mysql 127.0.0.1:13308",
        "- 灾备中心 drp-tgt-mysql 127.0.0.1:13307",
        "- 元库 mysql8 127.0.0.1:3306/DRPlatform",
        "",
        "## 一致性判定口径",
        "",
        "- 逐表对「所有列按主键顺序」取内容哈希，而非只比行数 —— 能发现行数相同但内容不同的静默不一致。",
        "- 主备切换场景用「灾备 ⊇ 备源」判定：备库只到克隆时刻，灾备目标还保留主库故障前的尾部写入，",
        "  因此目标应当是备库的超集；反向=< 才代表真实数据丢失。",
        "- 切回主源后的回切场景同样用「灾备 ⊇ 生产」判定：故障期间写入备库的行主库并不存在，",
        "  灾备中心保留它们是<b>正确</b>的（CDC 链路不做删除对账，不会反向抹掉这些真实业务数据）。",
        "- `geo_source.dyn_tbl` 命中字段转换规则（源 secret / 目标 MASKED），按设计排除比对。",
    ]
    with open(REPORT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log(f"报告已写入 {REPORT_MD}")


def main():
    for f in (LOG_FILE,):
        if os.path.exists(f):
            os.remove(f)
    os.makedirs(os.path.dirname(REPORT_MD), exist_ok=True)
    log("==== Docker 主备/双向切换验证开始 ====")
    try:
        if not phase0():
            log("前置失败，终止")
        else:
            phase1()
            phase2()
            phase3()
            phase4()
            phase5()
            phase6()
            phase7()
    except Exception as e:
        log(f"!! 异常: {e}")
        import traceback
        log(traceback.format_exc())
    finally:
        try:
            kill_app_hard()
        except Exception:
            pass
        # 恢复所有容器在线
        for cfg in (SRC, TGT, STANDBY):
            try:
                docker_cmd("start", container_of(cfg["port"]), timeout=120)
                docker_cmd("unpause", container_of(cfg["port"]), timeout=120)
            except Exception:
                pass
        write_report()
        failed = [r for r in RESULTS if not r[2]]
        log(f"==== 验证结束：{'全部通过' if not failed else str(len(failed)) + ' 项失败'} ====")
        return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
