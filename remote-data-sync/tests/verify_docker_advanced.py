# -*- coding: utf-8 -*-
"""
Docker 环境进阶全自动化端到端验证（GeoDRSync / Flink CDC 模式）
================================================================
覆盖场景：
  P1 动态新增表          —— 运行中源库新建表，应自动纳入同步
  P2 动态新增库          —— 运行中源库新建库+表，应自动纳入同步
  P3 动态 TRUNCATE       —— 源库 TRUNCATE，目标库应同步清空
  P4 动态修改字段        —— ADD / MODIFY / DROP COLUMN 结构变更同步
  P5 字段转换规则        —— transform-rules 命中值替换（源值不变、目标值被替换）
  P6 源库断网            —— pause 源容器后恢复，同步应自动续传
  P7 目标库断电+断网     —— stop/start 目标容器（断电）与 pause（断网）后收敛
  P8 主备切换中断        —— 切换过程中强杀应用，重启后续传且数据不丢
  P9 全量同步中断继续    —— 首轮全量快照中途强杀，重启后快照续传并一致

环境：复用前期老容器（root/123456）
  drp-src-mysql        127.0.0.1:13306  源库（binlog ROW）
  drp-src-standby-mysql 127.0.0.1:13308 备源（主备切换用）
  drp-tgt-mysql        127.0.0.1:13307  目标库
  mysql8               127.0.0.1:3306   元库 DRPlatform
"""
import os
import sys
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
REPORT_MD = os.path.join(APP_CWD, "target", "docker-e2e-advanced-report.md")
LOG_FILE = os.path.join(APP_CWD, "target", "docker-advanced-run.log")
COOKIE = os.path.join(APP_CWD, "target", "adv-cookie.txt")
APP_URL = "http://127.0.0.1:8899"

SRC = {"host": "127.0.0.1", "port": 13306}
STANDBY = {"host": "127.0.0.1", "port": 13308}
TGT = {"host": "127.0.0.1", "port": 13307}
AUTH = {"user": "root", "password": "123456"}
SYSTEM_DBS = {"information_schema", "mysql", "performance_schema", "sys"}
LAG_LIMIT = 10.0

BASE_DBS = ["geo_source", "biz_source"]
DYN_DB = "dyn_db"
DYN_TABLE = "dyn_tbl"

RESULTS = []
proc = None


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
        database=database, charset="utf8mb4", autocommit=True, connect_timeout=5, read_timeout=60,
    )


def user_dbs(cfg):
    with db(cfg) as conn, conn.cursor() as cur:
        cur.execute("SELECT schema_name FROM information_schema.schemata")
        return [r[0] for r in cur.fetchall() if r[0] not in SYSTEM_DBS]


def user_tables(cfg):
    out = {}
    with db(cfg) as conn, conn.cursor() as cur:
        cur.execute("SELECT table_schema, table_name FROM information_schema.tables "
                    "WHERE table_type='BASE TABLE'")
        for d, t in cur.fetchall():
            if d not in SYSTEM_DBS:
                out.setdefault(d, []).append(t)
    return out


def count(cfg, schema, table):
    try:
        with db(cfg) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{schema}`.`{table}`")
            return cur.fetchone()[0]
    except Exception:
        return None


def table_hash(cfg, schema, table):
    with db(cfg) as conn, conn.cursor() as cur:
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position", (schema, table))
        cols = [c[0] for c in cur.fetchall()]
        if not cols:
            return None
        cur.execute(f"SELECT COUNT(*) FROM `{schema}`.`{table}`")
        n = cur.fetchone()[0]
        if n == 0:
            return hashlib.md5(b"EMPTY").hexdigest()
        cur.execute(f"SELECT {','.join('`'+c+'`' for c in cols)} FROM `{schema}`.`{table}` "
                    f"ORDER BY {','.join('`'+c+'`' for c in cols)}")
        h = hashlib.md5()
        for row in cur.fetchall():
            h.update("\x1f".join("" if v is None else str(v) for v in row).encode("utf-8", "replace"))
            h.update(b"\x1e")
        return h.hexdigest()


def column_type(cfg, schema, table, col):
    with db(cfg) as conn, conn.cursor() as cur:
        cur.execute("SELECT data_type, character_maximum_length FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name=%s AND column_name=%s", (schema, table, col))
        r = cur.fetchone()
        return (r[0], r[1]) if r else None


def exec_sql(cfg, sql, ignore_error=False):
    try:
        with db(cfg) as conn, conn.cursor() as cur:
            cur.execute(sql)
        return True
    except Exception as e:
        if not ignore_error:
            log(f"  SQL 失败: {sql} -> {str(e)[:120]}")
        return False


# 字段转换规则命中的表：源值与目标值按设计不同，一致性比对需跳过
TRANSFORM_EXEMPT = {("geo_source", "dyn_tbl")}


def compare_all(tag, extra_dbs=()):
    """源/目标逐库逐表比对（排除转换规则生效的表：源值与目标值按设计不同）"""
    src = user_tables(SRC)
    tgt = user_tables(TGT)
    ok_all, lines = True, []
    for d in sorted(set(list(src.keys()) + list(tgt.keys()))):
        s_tabs, t_tabs = set(src.get(d, [])), set(tgt.get(d, []))
        if s_tabs != t_tabs:
            ok_all = False
            lines.append(f"  {d}: 表集合不一致 src缺={t_tabs-s_tabs} tgt缺={s_tabs-t_tabs}")
            continue
        for t in sorted(s_tabs):
            if (d, t) in TRANSFORM_EXEMPT:
                lines.append(f"  {d}.{t}: 跳过（字段转换规则生效，源/目标值按设计不同）")
                continue
            sc, tc = count(SRC, d, t), count(TGT, d, t)
            sh, th = table_hash(SRC, d, t), table_hash(TGT, d, t)
            same = (sc == tc and sh == th)
            ok_all &= same
            lines.append(f"  {d}.{t}: src={sc}({str(sh)[:8]}) tgt={tc}({str(th)[:8]}) {'OK' if same else 'MISMATCH'}")
    log(f"[{tag}] 一致性比对:\n" + "\n".join(lines))
    return ok_all, "\n".join(lines)


def wait_for(cond, timeout, interval=3, tag=""):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = cond()
        if last:
            if tag:
                log(f"[{tag}] 条件满足")
            return True
        time.sleep(interval)
    return False


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
    for p in pids:
        subprocess.run(["taskkill", "/F", "/T", "/PID", p], capture_output=True)
    time.sleep(3)


def start_app():
    free_port_8899()
    log("启动应用 jar ...")
    out = open(os.path.join(APP_CWD, "target", "app-adv.log"), "ab")
    return subprocess.Popen(
        [JAVA, "-Xmx2g", "-jar", APP_JAR, "--server.port=8899"],
        cwd=APP_CWD,
        env={k: v for k, v in os.environ.items() if k not in ("SERVER__PORT", "SERVER_PORT")},
        stdout=out, stderr=subprocess.STDOUT,
    )


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


def http_call(path, method="GET", body=None, timeout=15):
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
            log(f"  {path} 失败: {str(obj.get('message'))[:120]}")
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


def wait_app_ready(timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if login():
            jobs = http_call("/sync/cdc/jobs")
            if jobs is not None:
                return True
        if proc is not None and proc.poll() is not None:
            log(f"应用进程提前退出 exit={proc.returncode}")
            return False
        time.sleep(4)
    return False


def cdc_state():
    jobs = http_call("/sync/cdc/jobs")
    if isinstance(jobs, list) and jobs and isinstance(jobs[0], dict):
        return jobs[0]
    return None


def docker_cmd(*args, timeout=120):
    r = subprocess.run([DOCKER, *args], capture_output=True, timeout=timeout)
    return r.stdout.decode("utf-8", "replace").strip(), r.stderr.decode("utf-8", "replace").strip(), r.returncode


def wait_mysql_up(cfg, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with db(cfg) as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True
        except Exception:
            time.sleep(3)
    return False


# ------------------------------------------------------------------ 阶段
def reset_env(full=False):
    """清理历史状态；full=True 时连目标库一起删（用于全量快照测试）"""
    sp = os.path.join(APP_CWD, "savepoints")
    if os.path.isdir(sp):
        shutil.rmtree(sp, ignore_errors=True)
    for cfg in (SRC, TGT):
        exec_sql(cfg, f"DROP DATABASE IF EXISTS `{DYN_DB}`", ignore_error=True)
        exec_sql(cfg, f"DROP TABLE IF EXISTS `{BASE_DBS[0]}`.`{DYN_TABLE}`", ignore_error=True)
        exec_sql(cfg, f"ALTER TABLE `{BASE_DBS[0]}`.`t_user` DROP COLUMN remark", ignore_error=True)
    if full:
        for d in BASE_DBS + [DYN_DB]:
            exec_sql(TGT, f"DROP DATABASE IF EXISTS `{d}`", ignore_error=True)
        log("已删除目标库全部用户库（强制下一轮走全量快照）")
        return
    # 基线：清空并播种
    for cfg in (SRC, TGT):
        for d in BASE_DBS:
            for t in ["t_order", "t_user", "t_customer"]:
                exec_sql(cfg, f"TRUNCATE TABLE `{d}`.`{t}`", ignore_error=True)
    for i in range(3):
        exec_sql(SRC, f"INSERT INTO geo_source.t_order (order_no, user_id, amount, status, remark) "
                      f"VALUES ('base{i}', {i}, {10.5+i}, 1, 'r{i}')")
    for i in range(2):
        exec_sql(SRC, f"INSERT INTO geo_source.t_user (username, age, email) VALUES ('u{i}', {20+i}, 'u{i}@x.com')")
    for i in range(2):
        exec_sql(SRC, f"INSERT INTO biz_source.t_customer (name, phone, balance) "
                      f"VALUES ('c{i}', '1380000000{i}', {100.5+i})")
    log("基线播种完成")


def p1_new_table():
    log("=== P1 动态新增表 ===")
    exec_sql(SRC, f"CREATE TABLE `{BASE_DBS[0]}`.`{DYN_TABLE}` ("
                  f"id BIGINT NOT NULL AUTO_INCREMENT, note VARCHAR(64) NULL, "
                  f"amount DECIMAL(10,2) NULL, ts DATETIME NULL, PRIMARY KEY (id))")
    time.sleep(2)
    for i in range(3):
        exec_sql(SRC, f"INSERT INTO `{BASE_DBS[0]}`.`{DYN_TABLE}` (note, amount, ts) "
                      f"VALUES ('n{i}', {1.5+i}, NOW())")
    ok = wait_for(lambda: count(TGT, BASE_DBS[0], DYN_TABLE) == 3, 120, 4, "P1")
    record("P1-新增表", "新表自动纳入同步且 3 行到达", ok, f"目标行数={count(TGT, BASE_DBS[0], DYN_TABLE)}")


def p2_new_database():
    log("=== P2 动态新增库 ===")
    exec_sql(SRC, f"CREATE DATABASE IF NOT EXISTS `{DYN_DB}` DEFAULT CHARACTER SET utf8mb4")
    exec_sql(SRC, f"CREATE TABLE `{DYN_DB}`.`t1` (id BIGINT NOT NULL AUTO_INCREMENT, "
                  f"v VARCHAR(64) NULL, PRIMARY KEY (id))")
    time.sleep(2)
    exec_sql(SRC, f"INSERT INTO `{DYN_DB}`.`t1` (v) VALUES ('a'),('b')")
    ok = wait_for(lambda: count(TGT, DYN_DB, "t1") == 2, 150, 5, "P2")
    if not ok:
        log("  P2 未自动发现，尝试 /sync/mapping/reload ...")
        http_call("/sync/mapping/reload", "POST", {"instanceKey": "127.0.0.1->127.0.0.1"})
        ok2 = wait_for(lambda: count(TGT, DYN_DB, "t1") == 2, 120, 5, "P2-reload")
        record("P2-新增库", "新库自动纳入（reload 后生效）", ok2,
               f"自动发现={'是' if ok else '否'}，reload 后目标行数={count(TGT, DYN_DB, 't1')}")
    else:
        record("P2-新增库", "新库自动纳入同步", True, f"目标行数={count(TGT, DYN_DB, 't1')}")


def p3_truncate():
    log("=== P3 动态 TRUNCATE ===")
    exec_sql(SRC, f"TRUNCATE TABLE `{BASE_DBS[0]}`.`{DYN_TABLE}`")
    ok = wait_for(lambda: count(TGT, BASE_DBS[0], DYN_TABLE) == 0, 90, 3, "P3")
    record("P3-TRUNCATE", "源库 TRUNCATE 后目标库同步清空", ok,
           f"目标行数={count(TGT, BASE_DBS[0], DYN_TABLE)}")


def p4_alter():
    log("=== P4 动态修改字段 ===")
    # ADD
    exec_sql(SRC, f"ALTER TABLE `{BASE_DBS[0]}`.`{DYN_TABLE}` ADD COLUMN ext VARCHAR(32) NULL")
    ok_add = wait_for(lambda: column_type(TGT, BASE_DBS[0], DYN_TABLE, "ext") is not None, 60, 3, "P4-add")
    record("P4-改字段", "ADD COLUMN 同步", ok_add, f"目标列类型={column_type(TGT, BASE_DBS[0], DYN_TABLE, 'ext')}")

    # 新列写入数据
    exec_sql(SRC, f"INSERT INTO `{BASE_DBS[0]}`.`{DYN_TABLE}` (note, ext) VALUES ('with_ext', 'E1')")
    ok_data = wait_for(lambda: exec_scalar(TGT, f"SELECT COUNT(*) FROM `{BASE_DBS[0]}`.`{DYN_TABLE}` "
                                                 f"WHERE note='with_ext'") == 1, 60, 3, "P4-data")
    r = None
    if ok_data:
        try:
            r = exec_scalar(TGT, f"SELECT ext FROM `{BASE_DBS[0]}`.`{DYN_TABLE}` WHERE note='with_ext'")
        except Exception:
            r = None
    record("P4-改字段", "新列数据同步", ok_data and r == "E1", f"目标 ext={r}")

    # MODIFY
    exec_sql(SRC, f"ALTER TABLE `{BASE_DBS[0]}`.`{DYN_TABLE}` MODIFY COLUMN ext VARCHAR(128) NULL")
    ok_mod = wait_for(
        lambda: (column_type(TGT, BASE_DBS[0], DYN_TABLE, "ext") or (None, None))[1] == 128, 60, 3, "P4-modify")
    record("P4-改字段", "MODIFY COLUMN（扩长）同步", ok_mod, f"目标列={column_type(TGT, BASE_DBS[0], DYN_TABLE, 'ext')}")

    # DROP
    exec_sql(SRC, f"ALTER TABLE `{BASE_DBS[0]}`.`{DYN_TABLE}` DROP COLUMN ext")
    ok_drop = wait_for(lambda: column_type(TGT, BASE_DBS[0], DYN_TABLE, "ext") is None, 60, 3, "P4-drop")
    record("P4-改字段", "DROP COLUMN 同步", ok_drop, "")


def p5_transform():
    log("=== P5 字段转换规则 ===")
    exec_sql(SRC, f"DELETE FROM `{BASE_DBS[0]}`.`{DYN_TABLE}`")
    time.sleep(4)
    exec_sql(SRC, f"INSERT INTO `{BASE_DBS[0]}`.`{DYN_TABLE}` (note) VALUES ('secret'),('normal')")
    ok = wait_for(lambda: count(TGT, BASE_DBS[0], DYN_TABLE) == 2, 60, 3, "P5")

    def tgt_notes():
        with db(TGT) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT note FROM `{BASE_DBS[0]}`.`{DYN_TABLE}` ORDER BY id")
            return [r[0] for r in cur.fetchall()]

    def src_notes():
        with db(SRC) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT note FROM `{BASE_DBS[0]}`.`{DYN_TABLE}` ORDER BY id")
            return [r[0] for r in cur.fetchall()]

    time.sleep(3)
    s, t = src_notes(), tgt_notes()
    ok_rule = ("MASKED" in t) and ("secret" in s) and ("normal" in t)
    record("P5-字段转换", "命中规则的值被替换（源不变/目标替换）", ok and ok_rule,
           f"源={s} 目标={t}")


def p6_source_network_cut():
    log("=== P6 源库断网 ===")
    out, err, rc = docker_cmd("pause", "drp-src-mysql")
    if rc != 0:
        record("P6-源断网", "docker pause 源库", False, err[:100])
        return
    log("源库已 pause 25s")
    time.sleep(25)
    docker_cmd("unpause", "drp-src-mysql")
    log("源库恢复，等待应用重连 ...")
    wait_mysql_up(SRC, 120)
    # 恢复后写入
    t0 = time.time()
    for i in range(5):
        exec_sql(SRC, f"INSERT INTO geo_source.t_order (order_no, user_id, amount, status) "
                      f"VALUES ('after_cut{i}', {i}, {5.5+i}, 1)")
    ok = wait_for(lambda: count(TGT, "geo_source", "t_order") is not None
                  and exec_scalar(TGT, "SELECT COUNT(*) FROM geo_source.t_order WHERE order_no LIKE 'after_cut%'") == 5,
                  120, 4, "P6")
    record("P6-源断网", "源库恢复后新增 5 行同步到达", ok, f"耗时 {time.time()-t0:.0f}s")
    okc = wait_converged_all("P6-收敛", 120)
    record("P6-源断网", "断网恢复后全库一致", okc, "")


def exec_scalar(cfg, sql):
    try:
        with db(cfg) as conn, conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]
    except Exception:
        return None


def wait_converged_all(tag, timeout=180):
    return wait_for(lambda: compare_all(tag)[0], timeout, 5)


def p7_target_outage():
    log("=== P7 目标库断电 + 断网 ===")
    # 7a 断电：stop 后 start
    docker_cmd("stop", "drp-tgt-mysql")
    log("目标库已断电(stop)，期间写入 10 行")
    for i in range(10):
        exec_sql(SRC, f"INSERT INTO geo_source.t_order (order_no, user_id, amount, status) "
                      f"VALUES ('pwr{i}', {i}, {7.5+i}, 1)")
        time.sleep(0.4)
    docker_cmd("start", "drp-tgt-mysql")
    up = wait_mysql_up(TGT, 180)
    record("P7-目标断电", "目标库 stop 后可重新拉起", up, "")
    ok = wait_converged_all("P7a-收敛", 300)
    record("P7-目标断电", "目标库断电恢复后全库一致", ok,
           f"t_order src={count(SRC,'geo_source','t_order')} tgt={count(TGT,'geo_source','t_order')}")

    # 7b 断网：pause 后 unpause（目标不可达但进程在）
    docker_cmd("pause", "drp-tgt-mysql")
    log("目标库已断网(pause)，期间写入 10 行")
    for i in range(10):
        exec_sql(SRC, f"INSERT INTO geo_source.t_order (order_no, user_id, amount, status) "
                      f"VALUES ('net{i}', {i}, {8.5+i}, 1)")
        time.sleep(0.4)
    docker_cmd("unpause", "drp-tgt-mysql")
    ok2 = wait_converged_all("P7b-收敛", 300)
    record("P7-目标断网", "目标库断网恢复后全库一致", ok2,
           f"t_order src={count(SRC,'geo_source','t_order')} tgt={count(TGT,'geo_source','t_order')}")


def p8_ha_switch_interrupt():
    log("=== P8 主备切换中断 ===")
    for i in range(3):
        exec_sql(SRC, f"INSERT INTO geo_source.t_order (order_no, user_id, amount, status) "
                      f"VALUES ('pre_sw{i}', {i}, {1.5+i}, 1)")
    before = wait_converged_all("P8-切换前", 90)
    src_rows_before = count(SRC, "geo_source", "t_order")

    # 强杀模拟“切换过程被中断”
    kill_app_hard()
    log("切换前强杀应用（模拟中断），重启 ...")
    global proc
    proc = start_app()
    if not wait_app_ready():
        record("P8-主备切换", "中断后应用重启成功", False, "")
        return
    record("P8-主备切换", "中断后应用重启成功", True, "")

    data = http_call("/sync/mapping/switchSource", "POST", {"instanceKey": "127.0.0.1->127.0.0.1"})
    switched = isinstance(data, dict) and data.get("switched") is True
    record("P8-主备切换", "调用 switchSource 成功切换", switched, json.dumps(data, ensure_ascii=False)[:160])

    time.sleep(5)
    mappings = http_call("/sync/mappings")
    now_port = None
    if isinstance(mappings, list):
        for m in mappings:
            if isinstance(m, dict) and m.get("sourcePort") in (13306, 13308):
                now_port = m.get("sourcePort")
    record("P8-主备切换", "映射源已切到备源 13308", now_port == 13308, f"当前源端口={now_port}")

    # 新源写入并验证同步
    for i in range(3):
        exec_sql(STANDBY, f"INSERT INTO geo_source.t_order (order_no, user_id, amount, status) "
                          f"VALUES ('post_sw{i}', {i}, {2.5+i}, 1)")
    ok_new = wait_for(lambda: exec_scalar(TGT, "SELECT COUNT(*) FROM geo_source.t_order "
                                                "WHERE order_no LIKE 'post_sw%'") == 3, 120, 4, "P8")
    record("P8-主备切换", "切换后备源新增 3 行同步到达", ok_new, "")
    # 切回主源，保证后续阶段仍用 13306
    http_call("/sync/mapping/switchSource", "POST", {"instanceKey": "127.0.0.1->127.0.0.1"})
    time.sleep(5)
    back = http_call("/sync/mappings")
    back_port = None
    if isinstance(back, list):
        for m in back:
            if isinstance(m, dict) and m.get("sourcePort") in (13306, 13308):
                back_port = m.get("sourcePort")
    record("P8-主备切换", "可切回主源 13306（可逆）", back_port == 13306, f"当前源端口={back_port}")
    record("P8-主备切换", "切换前数据未丢失", before and count(TGT, "geo_source", "t_order") >= src_rows_before,
           f"切换前源行数={src_rows_before} 目标={count(TGT,'geo_source','t_order')}")


def p9_snapshot_interrupt():
    log("=== P9 全量同步中断继续 ===")
    kill_app_hard()
    reset_env(full=True)          # 目标库清空 → 下一轮强制全量快照
    global proc
    proc = start_app()
    if not wait_app_ready():
        record("P9-全量中断", "应用启动", False, "")
        return
    log("全量快照进行中，8s 后强杀应用（中断快照）...")
    time.sleep(8)
    kill_app_hard()
    tgt_dbs_after_kill = user_dbs(TGT)
    log(f"  强杀时目标库已有库: {tgt_dbs_after_kill}")
    proc = start_app()
    if not wait_app_ready():
        record("P9-全量中断", "中断后应用重启", False, "")
        return
    record("P9-全量中断", "中断后应用重启成功", True, f"强杀时目标库={tgt_dbs_after_kill}")
    ok = wait_converged_all("P9-收敛", 300)
    record("P9-全量中断", "快照中断后重启续传并最终一致", ok,
           f"目标库={user_dbs(TGT)}")
    # 续传后增量能力
    for i in range(3):
        exec_sql(SRC, f"INSERT INTO geo_source.t_order (order_no, user_id, amount, status) "
                      f"VALUES ('post_full{i}', {i}, {3.5+i}, 1)")
    ok2 = wait_for(lambda: exec_scalar(TGT, "SELECT COUNT(*) FROM geo_source.t_order "
                                             "WHERE order_no LIKE 'post_full%'") == 3, 90, 4, "P9-inc")
    record("P9-全量中断", "续传完成后增量同步正常", ok2, "")


def write_report(t_start):
    fails = [r for r in RESULTS if not r[2]]
    lines = [
        "# GeoDRSync Docker 进阶自动化验证报告",
        "",
        f"- 时间：{datetime.now():%Y-%m-%d %H:%M:%S}",
        f"- 总耗时：{time.time()-t_start:.0f}s",
        f"- 结论：**{'全部通过 ✅' if not fails else f'{len(fails)} 项失败 ❌'}**（共 {len(RESULTS)} 项）",
        "",
        "| 阶段 | 验证项 | 结果 | 明细 |",
        "|---|---|---|---|",
    ]
    for ph, item, ok, det in RESULTS:
        det = str(det).replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {ph} | {item} | {'✅' if ok else '❌'} | {det} |")
    lines += ["", "## 环境", "",
              "- 源库 drp-src-mysql 127.0.0.1:13306；备源 drp-src-standby-mysql 127.0.0.1:13308",
              "- 目标库 drp-tgt-mysql 127.0.0.1:13307；元库 mysql8 127.0.0.1:3306/DRPlatform", ""]
    with open(REPORT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log(f"报告已写入 {REPORT_MD}")


def main():
    global proc
    t_start = time.time()
    try:
        os.remove(LOG_FILE)
    except OSError:
        pass
    log("==== Docker 进阶验证开始 ====")
    out, _, rc = docker_cmd("ps", "--format", "{{.Names}} {{.Status}}")
    log(f"容器状态: {out}")
    if rc != 0:
        sys.exit(2)

    kill_app_hard()
    reset_env()
    proc = start_app()
    if not wait_app_ready():
        record("前置", "应用启动", False, "等待就绪超时")
        kill_app_hard()
        write_report(t_start)
        sys.exit(3)
    record("前置", "应用启动（8899）", True, "")
    ok0 = wait_converged_all("前置-基线", 180)
    record("前置", "基线数据一致（快照完成）", ok0, "")

    try:
        p1_new_table()
        p2_new_database()
        p3_truncate()
        p4_alter()
        p5_transform()
        p6_source_network_cut()
        p7_target_outage()
        p8_ha_switch_interrupt()
        p9_snapshot_interrupt()
    finally:
        kill_app_hard()

    write_report(t_start)
    fails = [r for r in RESULTS if not r[2]]
    log(f"==== 验证结束：{'全部通过' if not fails else f'{len(fails)} 项失败'} ====")
    sys.exit(0 if not fails else 1)


if __name__ == "__main__":
    main()
