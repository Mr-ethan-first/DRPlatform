# -*- coding: utf-8 -*-
"""DRPlatform 大数据量压测：45 个数据库 × 67 张表 ≈ 3015 张表并发同步。

测评目标（对齐用户需求）：
  1. 全量同步速度：3015 张表 / 约 155 万行从启动到全部落库的耗时与吞吐
  2. 内存溢出验证：-Xmx2g 固定堆下监控 JVM 堆使用，确认无 OOM
  3. 线程过量验证：监控 JVM 活动线程数（预期约 3015 个 TableSink + 少量框架线程）
  4. 反压逐步同步：增量风暴期间服务不崩溃、数据最终一致

前置条件：
  - Docker 实验室容器就绪（drp-src-mysql:13306 / drp-tgt-mysql:13307）
  - 压测数据已由 gen_perf_data.py 灌入源端（本脚本会检查，缺失则自动灌入）
  - 应用 jar 已构建（remote-data-sync/target/remote-data-sync.jar）

用法：
    python run_perf_45db.py                # 完整流程（准备→启动→全量→增量→校验→报告）
    python run_perf_45db.py --skip-gen     # 跳过数据生成（数据已存在）
    python run_perf_45db.py --keep-app     # 结束后不停应用（便于人工检查）
"""
import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime

import pymysql
import requests

# ----------------------------- 常量 -----------------------------

SRC = dict(host="127.0.0.1", port=13306, user="root", password="123456")
TGT = dict(host="127.0.0.1", port=13307, user="root", password="123456")
META = dict(host="127.0.0.1", port=3306, user="root", password="123456")

DB_PREFIX = "perf_db_"
DBS = 45
TABLES_PER_DB = 67
TOTAL_TABLES = DBS * TABLES_PER_DB          # 3015
BASE_URL = "http://127.0.0.1:8899"
ADMIN_USER, ADMIN_PASS = "admin", "admin123"

APP_JAR = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync\target\remote-data-sync.jar"
APP_CWD = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync"
APP_LOG = r"D:\WorkSpace\flink-cdc-sync\tests\docker-lab\app-perf.log"
JAVA = r"D:\software\jdk-21.0.11\bin\java.exe"
GEN_SCRIPT = r"D:\WorkSpace\flink-cdc-sync\tests\gen_perf_data.py"

HEAP_LIMIT_MB = 2048          # 压测固定堆上限
FULL_SYNC_TIMEOUT = 3600      # 全量同步超时（秒）
INC_STORM_SECONDS = 180       # 增量风暴持续时间
SAMPLE_INTERVAL = 5           # 监控采样间隔（秒）

REPORT = r"D:\WorkSpace\flink-cdc-sync\tests\perf-report.txt"

# 监控采样记录
samples = []          # [(ts, heap_used_mb, heap_max_mb, threads, cpu)]
sample_lock = threading.Lock()
stop_monitor = threading.Event()
APP_PROC = None


# ----------------------------- 基础工具 -----------------------------

def log(msg):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(REPORT, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def db_conn(c, db=None, timeout=10):
    return pymysql.connect(host=c["host"], port=c["port"], user=c["user"],
                           password=c["password"], database=db, charset="utf8mb4",
                           connect_timeout=timeout, read_timeout=600, write_timeout=600)


def db_query(c, sql, db=None, args=None):
    conn = db_conn(c, db)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, args) if args else cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


def db_exec(c, sql, db=None, args=None):
    conn = db_conn(c, db)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args) if args else cur.execute(sql)
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()


def session():
    s = requests.Session()
    s.trust_env = False          # 绕过本机 HTTP_PROXY（会把 127.0.0.1 也发给代理）
    return s


def login():
    s = session()
    s.headers.update({"Content-Type": "application/json"})
    r = s.post(f"{BASE_URL}/auth/login", json={"username": ADMIN_USER, "password": ADMIN_PASS}, timeout=20)
    r.raise_for_status()
    assert r.json().get("success") is True, f"登录失败: {r.text[:200]}"
    return s


def find_app_pid():
    """netstat 找 8899 监听 PID（纯 ASCII 输出，避免 GBK 解码陷阱）。

    注意 IPv6 行格式：TCP [::]:8899 [::]:0 LISTENING <pid>，
    必须按空白分割取最后一段（rsplit(":") 会被 [::]:0 里的冒号干扰）。
    """
    r = subprocess.run(["netstat", "-ano"], capture_output=True)
    out = r.stdout.decode("ascii", "replace")
    for line in out.splitlines():
        if ":8899" in line and "LISTENING" in line:
            return int(line.split()[-1])
    return None


def kill_pid(pid):
    subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True)


# ----------------------------- Phase 0：环境准备 -----------------------------

def prepare_env(skip_gen):
    log("=== Phase 0：环境准备 ===")
    # 1) 调高 MySQL 连接上限（默认 151 撑不住 45 库 × 池 + 200 并发信号量）
    for name, c in (("源库13306", SRC), ("目标库13307", TGT)):
        db_exec(c, "SET GLOBAL max_connections=1000")
        v = db_query(c, "SHOW VARIABLES LIKE 'max_connections'")[0]["Value"]
        log(f"{name} max_connections -> {v}")

    # 2) 清理目标端历史 perf 库（保证应用从零建库建表）
    rows = db_query(TGT, "SELECT SCHEMA_NAME AS n FROM information_schema.SCHEMATA "
                         "WHERE SCHEMA_NAME LIKE 'perf_db_%'")
    for r in rows:
        db_exec(TGT, f"DROP DATABASE `{r['n']}`")
    log(f"目标端已清理 {len(rows)} 个历史 perf_db_* 库")

    # 3) 清理元库 perf 水位（保证从零开始）
    try:
        n = db_exec(META, "DELETE FROM sync_watermark WHERE SOURCE_DB LIKE 'perf_db_%'", "drplatform")
        log(f"元库已清理 {n} 条 perf 水位")
    except Exception as e:
        log(f"元库水位清理跳过: {e}")

    # 4) 确保压测数据存在
    n_src = db_query(SRC, "SELECT COUNT(*) AS c FROM information_schema.SCHEMATA "
                          "WHERE SCHEMA_NAME LIKE 'perf_db_%'")[0]["c"]
    if n_src >= DBS and skip_gen:
        log(f"源端已有 {n_src} 个 perf_db_* 库，跳过生成")
    elif n_src >= DBS:
        log(f"源端已有 {n_src} 个 perf_db_* 库，跳过生成（如需重建请手动运行 gen_perf_data.py --force）")
    else:
        log(f"源端仅 {n_src} 个 perf_db_* 库，开始灌入压测数据（约 155 万行，耗时较长）…")
        t0 = time.time()
        subprocess.run([sys.executable, GEN_SCRIPT], check=True)
        log(f"数据灌入完成，耗时 {time.time() - t0:.0f}s")

    # 5) 统计源端规模
    st = db_query(SRC, "SELECT COUNT(*) AS tables_, "
                       "COALESCE(SUM(TABLE_ROWS), 0) AS rows_ FROM information_schema.TABLES "
                       "WHERE TABLE_SCHEMA LIKE 'perf_db_%'")[0]
    log(f"源端规模：{int(st['tables_'])} 张表，约 {int(st['rows_'])} 行（information_schema 近似值）")
    assert int(st["tables_"]) >= TOTAL_TABLES, f"源端表数不足: {st['tables_']} < {TOTAL_TABLES}"


# ----------------------------- Phase 1：启动应用 -----------------------------

def start_app():
    global APP_PROC
    log("=== Phase 1：启动同步服务（perflab profile，-Xmx2g）===")
    pid = find_app_pid()
    if pid:
        kill_pid(pid)
        time.sleep(3)
    env = dict(os.environ, SPRING_PROFILES_ACTIVE="perflab")
    env.pop("SERVER__PORT", None)
    env.pop("SERVER_PORT", None)
    args = [JAVA, f"-Xmx{HEAP_LIMIT_MB}m", "-Xms512m",
            "-XX:+HeapDumpOnOutOfMemoryError",
            f"-XX:HeapDumpPath={APP_CWD}\\heapdump-perf.hprof",
            "-jar", APP_JAR, "--server.port=8899"]
    with open(APP_LOG, "a", encoding="utf-8") as f:
        APP_PROC = subprocess.Popen(args, cwd=APP_CWD, env=env,
                                    stdout=f, stderr=subprocess.STDOUT)
    t0 = time.time()
    while time.time() - t0 < 300:
        time.sleep(3)
        if APP_PROC.poll() is not None:
            raise RuntimeError(f"应用进程退出 exit={APP_PROC.poll()}，见 {APP_LOG}")
        try:
            login()
            log(f"应用就绪，耗时 {time.time() - t0:.0f}s（pid={APP_PROC.pid}）")
            return time.time()
        except Exception:
            continue
    raise RuntimeError("应用启动超时（300s）")


# ----------------------------- 监控 -----------------------------

def actuator_metrics(s, names):
    out = {}
    for n in names:
        try:
            url = f"{BASE_URL}/actuator/metrics/{n}"
            if n == "jvm.memory.used":
                url += "?tag=area:heap"      # 只取堆（默认聚合含 nonheap）
            r = s.get(url, timeout=10)
            if r.status_code == 200:
                ms = r.json().get("measurements") or []
                if ms:
                    out[n] = ms[0].get("value")
        except Exception:
            pass
    return out


def monitor_loop(s):
    """每 5s 采样 JVM 堆/线程/CPU + 元库水位进度 + 目标端行数。"""
    while not stop_monitor.is_set():
        try:
            m = actuator_metrics(s, ["jvm.memory.used", "jvm.threads.live",
                                     "jvm.gc.pause", "system.cpu.usage"])
            heap = float(m.get("jvm.memory.used") or 0) / 1024 / 1024
            threads = int(m.get("jvm.threads.live") or 0)
            cpu = float(m.get("system.cpu.usage") or 0)
            try:
                wm = int(db_query(META, "SELECT COUNT(*) AS c FROM sync_watermark "
                                        "WHERE SOURCE_DB LIKE 'perf_db_%'",
                                  "drplatform")[0]["c"])
            except Exception:
                wm = -1
            try:
                trows = int(db_query(TGT, "SELECT COALESCE(SUM(TABLE_ROWS),0) AS r "
                                          "FROM information_schema.TABLES "
                                          "WHERE TABLE_SCHEMA LIKE 'perf_db_%'")[0]["r"])
            except Exception:
                trows = -1
            with sample_lock:
                samples.append(dict(ts=time.time(), heap=heap, threads=threads,
                                    cpu=cpu, wm=wm, trows=trows))
        except Exception:
            pass
        stop_monitor.wait(SAMPLE_INTERVAL)


# ----------------------------- Phase 2：全量同步测评 -----------------------------

def wait_full_sync(t_ready):
    log("=== Phase 2：全量同步速度测评 ===")
    t0 = time.time()
    last_log = 0
    stable_count = 0
    last_wm = -1
    last_tables = 0
    while time.time() - t0 < FULL_SYNC_TIMEOUT:
        with sample_lock:
            snap = list(samples)
        wm = snap[-1]["wm"] if snap else 0
        trows = snap[-1]["trows"] if snap else 0
        el = time.time() - t0
        if el - last_log >= 30:
            rate = trows / el if el > 0 else 0
            log(f"  进度: 水位表 {wm}/{TOTAL_TABLES}，目标端约 {trows} 行，"
                f"已用 {el:.0f}s，均速 {rate:.0f} 行/s")
            last_log = el
        # 完成判据：目标端 3015 张表全部创建 + 水位数连续 60s 稳定
        # （无 update_time 的表走全量兜底路径，不落盘水位，属产品设计行为；
        #  60s 稳定可排除单张大表同步造成的短暂平台期）
        try:
            last_tables = int(db_query(TGT, "SELECT COUNT(*) AS c FROM information_schema.TABLES "
                                            "WHERE TABLE_SCHEMA LIKE 'perf_db_%'")[0]["c"])
        except Exception:
            pass
        if wm == last_wm:
            stable_count += 1
        else:
            stable_count = 0
            last_wm = wm
        if last_tables >= TOTAL_TABLES and stable_count >= 30:
            t_full = time.time()
            no_wm = TOTAL_TABLES - wm
            log(f"✅ 全量同步完成：目标端 {last_tables} 张表全部创建，水位覆盖 {wm} 张"
                f"（{no_wm} 张无 update_time 表走全量兜底，不落盘水位），"
                f"耗时 {t_full - t_ready:.0f}s（应用就绪起算）")
            return t_full
        time.sleep(2)
    raise TimeoutError(f"全量同步超时（{FULL_SYNC_TIMEOUT}s），最后水位 "
                       f"{samples[-1]['wm'] if samples else 0}/{TOTAL_TABLES}，"
                       f"目标表数 {last_tables}")


# ----------------------------- Phase 3：增量风暴（反压验证） -----------------------------

def incremental_storm(s, t_full):
    log(f"=== Phase 3：增量风暴（{INC_STORM_SECONDS}s，模拟业务持续写入）===")
    t_end = time.time() + INC_STORM_SECONDS
    total_ins = total_upd = total_del = 0
    stamp = int(time.time())
    col_cache = {}
    while time.time() < t_end:
        try:
            # 随机选库/表（避开刚同步过的热点，均匀分布）
            d = (stamp + total_ins * 7) % DBS
            t = (stamp + total_ins * 13) % TABLES_PER_DB
            db = f"{DB_PREFIX}{d:02d}"
            tbl = f"perf_t_{t:03d}"
            # 该表结构由 gen_perf_data 决定（5 变体循环），带列类型探测（缓存）
            key = (db, tbl)
            if key not in col_cache:
                cols = [r for r in db_query(
                    SRC, "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
                    None, (db, tbl))]
                col_cache[key] = cols
            cols = col_cache[key]
            biz = [r for r in cols if r["COLUMN_NAME"] not in ("id", "update_time")]
            if not biz:
                continue

            def fit(col_type, val):
                """按列类型裁剪值：varchar(n) 截断、日期列给当前时间、数值列转整数、其他原样。"""
                ct = col_type.lower()
                if ct.startswith("varchar") or ct.startswith("char"):
                    n = 16
                    import re as _re
                    m = _re.search(r"\((\d+)\)", ct)
                    if m:
                        n = int(m.group(1))
                    return str(val)[:n]
                if ct.startswith(("datetime", "timestamp")):
                    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if ct.startswith(("date", "time", "year")):
                    return datetime.now().strftime("%Y-%m-%d")
                if ct.startswith(("int", "bigint", "smallint", "tinyint", "decimal", "double", "float")):
                    try:
                        return int(str(val).split("_")[-1]) if "_" in str(val) else int(val)
                    except Exception:
                        return 0
                return val

            tag_col = biz[0]["COLUMN_NAME"]
            tag_ct = biz[0]["COLUMN_TYPE"]
            all_names = [r["COLUMN_NAME"] for r in cols]
            has_ut = "update_time" in all_names
            ins_cols = [r["COLUMN_NAME"] for r in biz] + (["update_time"] if has_ut else [])
            ph = ",".join(["%s"] * len(ins_cols))
            collist = ",".join(f"`{c}`" for c in ins_cols)
            # 【关键】显式写 update_time，取 Python 本地时间（与 gen_perf_data 的 uts 同时基）。
            # 若省略该列，MySQL 以 CURRENT_TIMESTAMP 填充——容器时区为 UTC，比本地时间字面值
            # 早约 8 小时 → 新行 update_time 低于已有水位 → keyset 增量扫描永远看不到 → 漏同步
            # （2026-10-17 压测复现：368 张表各漏 19 行，差异 6,992 行）。
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # INSERT（每行按列类型生成合法值）
            vals, args = [], []
            for k in range(20):
                vals.append(f"({ph})")
                for r in biz:
                    raw = f"S{stamp}_{total_ins}_{k}" if r["COLUMN_NAME"] == tag_col else k
                    args.append(fit(r["COLUMN_TYPE"], raw))
                if has_ut:
                    args.append(now_str)
            db_exec(SRC, f"INSERT INTO `{db}`.`{tbl}` ({collist}) VALUES {','.join(vals)}",
                    None, tuple(args))
            total_ins += 20
            # 取本批次首行主键（tag 精确匹配 + id 倒序，规避旧批次同截断 tag 的干扰）
            row = db_query(SRC, f"SELECT `id` FROM `{db}`.`{tbl}` "
                                f"WHERE `{tag_col}`=%s ORDER BY `id` DESC LIMIT 1",
                           None, (fit(tag_ct, f"S{stamp}_{total_ins - 20}_0"),))
            if not row:
                continue
            rid = row[0]["id"]
            # UPDATE（按主键精确改最后一个业务列 + update_time，保证变更对增量扫描可见）
            upd_col, upd_ct = biz[-1]["COLUMN_NAME"], biz[-1]["COLUMN_TYPE"]
            upd_val = fit(upd_ct, f"U{stamp}_{total_ins}")
            if has_ut:
                db_exec(SRC, f"UPDATE `{db}`.`{tbl}` SET `{upd_col}`=%s, `update_time`=%s "
                             f"WHERE `id`=%s", None, (upd_val, now_str, rid))
            else:
                db_exec(SRC, f"UPDATE `{db}`.`{tbl}` SET `{upd_col}`=%s WHERE `id`=%s",
                        None, (upd_val, rid))
            total_upd += 1
            # DELETE（按主键精确删本行；旧版按 tag 删，VARCHAR(16) 列截断会误删同批 20 行）
            db_exec(SRC, f"DELETE FROM `{db}`.`{tbl}` WHERE `id`=%s", None, (rid,))
            total_del += 1
            stamp += 1
        except Exception as e:
            log(f"  ⚠️ 增量风暴写入异常（继续）: {str(e)[:200]}")
            time.sleep(2)
    log(f"增量风暴结束：INSERT {total_ins} 行 / UPDATE {total_upd} 次 / DELETE {total_del} 次")

    # 等待风暴数据收敛（两阶段）：
    # 阶段 1（快路径）：水位数 + 目标端 TABLE_ROWS 连续 3 次稳定 → 插入/更新已基本落库；
    # 阶段 2（权威判据）：源/目标精确 COUNT 相等（连续 2 次）→ 删除对账完成。
    # 删除对账需连续 N 轮确认（默认 3 轮，3015 表串行循环约 80s/轮 → 约 4-5 分钟），
    # TABLE_ROWS 为近似值不能作为删除对账判据，必须用精确 COUNT。
    def exact_total(c):
        total = 0
        for d in range(DBS):
            db = f"{DB_PREFIX}{d:02d}"
            parts = [f"SELECT COUNT(*) AS c FROM `{db}`.`perf_t_{t:03d}`"
                     for t in range(TABLES_PER_DB)]
            sql = "SELECT COALESCE(SUM(c),0) AS total FROM (" + " UNION ALL ".join(parts) + ") x"
            total += int(db_query(c, sql)[0]["total"])
        return total

    t0 = time.time()
    last_sig, stable = None, 0
    while time.time() - t0 < 600:
        try:
            wm = int(db_query(META, "SELECT COUNT(*) AS c FROM sync_watermark "
                                    "WHERE SOURCE_DB LIKE 'perf_db_%'", "drplatform")[0]["c"])
            trows = int(db_query(TGT, "SELECT COALESCE(SUM(TABLE_ROWS),0) AS r "
                                      "FROM information_schema.TABLES "
                                      "WHERE TABLE_SCHEMA LIKE 'perf_db_%'")[0]["r"])
            sig = (wm, trows)
            stable = stable + 1 if sig == last_sig else 0
            last_sig = sig
            if stable >= 3:
                log(f"✅ 插入/更新已收敛（水位 {wm}，目标约 {trows} 行，"
                    f"{time.time() - t0:.0f}s），进入删除对账等待…")
                break
        except Exception:
            pass
        time.sleep(5)

    t1 = time.time()
    eq_stable = 0
    while time.time() - t1 < 900:
        try:
            st, tt = exact_total(SRC), exact_total(TGT)
            log(f"  对账观察: 源 {st} / 目标 {tt}（差 {st - tt}，相等稳定 {eq_stable}/2）")
            if st == tt:
                eq_stable += 1
                if eq_stable >= 2:
                    log(f"✅ 删除对账完成，源/目标精确行数一致（{st} 行，"
                        f"对账等待 {time.time() - t1:.0f}s）")
                    return
            else:
                eq_stable = 0
        except Exception as e:
            log(f"  对账观察异常（继续）: {str(e)[:120]}")
        time.sleep(60)
    log("⚠️ 删除对账等待超时（继续校验，差异将在 Phase 4 暴露）")


# ----------------------------- Phase 4：一致性校验 -----------------------------

def verify_consistency():
    log("=== Phase 4：一致性校验 ===")
    # 1) 全库精确行数比对（逐库 UNION ALL 统计，避免单条 SQL 过长）
    def total_rows(c):
        total = 0
        for d in range(DBS):
            db = f"{DB_PREFIX}{d:02d}"
            parts = [f"SELECT COUNT(*) AS c FROM `{db}`.`perf_t_{t:03d}`"
                     for t in range(TABLES_PER_DB)]
            sql = "SELECT COALESCE(SUM(c),0) AS total FROM (" + " UNION ALL ".join(parts) + ") x"
            total += int(db_query(c, sql)[0]["total"])
        return total

    t0 = time.time()
    src_total = total_rows(SRC)
    tgt_total = total_rows(TGT)
    log(f"精确行数：源端 {src_total} / 目标端 {tgt_total}（COUNT 耗时 {time.time() - t0:.0f}s）")

    # 2) 表数比对
    for name, c in (("源端", SRC), ("目标端", TGT)):
        n = int(db_query(c, "SELECT COUNT(*) AS c FROM information_schema.TABLES "
                            "WHERE TABLE_SCHEMA LIKE 'perf_db_%'")[0]["c"])
        log(f"{name} perf 表数：{n}")

    # 3) 抽样重复主键检查（随机 10 库 × 5 表）
    dup_total = 0
    checked = 0
    for d in range(0, DBS, 5):
        db = f"{DB_PREFIX}{d:02d}"
        for t in range(0, TABLES_PER_DB, 13):
            tbl = f"perf_t_{t:03d}"
            try:
                r = db_query(TGT, "SELECT COUNT(*) AS c FROM ("
                                  "SELECT id FROM `{}`.`{}` GROUP BY id HAVING COUNT(*)>1) x"
                              .format(db, tbl))[0]["c"]
                dup_total += int(r)
                checked += 1
            except Exception:
                pass
    log(f"抽样重复主键检查：{checked} 张表，重复组 {dup_total}")

    ok_rows = src_total == tgt_total
    ok_dup = dup_total == 0
    log(f"一致性结论：行数{'一致' if ok_rows else '不一致 ❌'}，"
        f"重复主键{'无' if ok_dup else '存在 ❌'}")
    return ok_rows and ok_dup


# ----------------------------- Phase 5：报告 -----------------------------

def write_report(t_ready, t_full, storm_stats):
    with sample_lock:
        snap = list(samples)
    if not snap:
        log("无监控采样，跳过报告")
        return
    peak_heap = max(s["heap"] for s in snap)
    peak_threads = max(s["threads"] for s in snap)
    final_heap = snap[-1]["heap"]
    final_threads = snap[-1]["threads"]
    dur_full = t_full - t_ready
    # 同步速率：取全量阶段最后两个采样的行数差/时间差（瞬时），以及全程平均
    full_snaps = [s for s in snap if s["ts"] <= t_full]
    avg_rate = (full_snaps[-1]["trows"] / dur_full) if full_snaps and dur_full > 0 else 0
    inst_rate = 0
    if len(full_snaps) >= 2:
        a, b = full_snaps[-2], full_snaps[-1]
        dt = b["ts"] - a["ts"]
        if dt > 0:
            inst_rate = (b["trows"] - a["trows"]) / dt

    lines = [
        "",
        "=" * 64,
        "DRPlatform 大数据量压测报告（45 库 × 67 表 = 3015 表）",
        "=" * 64,
        f"压测时间           : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"JVM 堆上限         : -Xmx{HEAP_LIMIT_MB}m",
        f"全量同步耗时       : {dur_full:.0f}s（应用就绪 → {TOTAL_TABLES} 表全部落库）",
        f"平均同步速率       : {avg_rate:.0f} 行/s（目标端行数/耗时，information_schema 近似）",
        f"末段瞬时速率       : {inst_rate:.0f} 行/s",
        f"峰值 JVM 堆使用    : {peak_heap:.0f} MB / {HEAP_LIMIT_MB} MB"
        f"（{peak_heap / HEAP_LIMIT_MB * 100:.1f}%）",
        f"峰值活动线程数     : {peak_threads}",
        f"结束时堆/线程      : {final_heap:.0f} MB / {final_threads}",
        f"OOM                : {'未发生' if APP_PROC and APP_PROC.poll() is None else '进程已退出 ❌'}",
        f"增量风暴           : {storm_stats}",
        f"监控采样数         : {len(snap)}（每 {SAMPLE_INTERVAL}s）",
        "=" * 64,
    ]
    text = "\n".join(lines)
    log(text)


# ----------------------------- 主流程 -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-gen", action="store_true")
    ap.add_argument("--keep-app", action="store_true")
    args = ap.parse_args()

    # 重置报告
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write(f"DRPlatform 压测 {datetime.now()}\n")

    prepare_env(args.skip_gen)
    t_ready = start_app()
    s = login()

    mon = threading.Thread(target=monitor_loop, args=(s,), daemon=True)
    mon.start()

    t_full = wait_full_sync(t_ready)
    incremental_storm(s, t_full)

    # 停监控前最后采一次
    time.sleep(SAMPLE_INTERVAL + 2)
    stop_monitor.set()
    mon.join(timeout=10)

    consistent = verify_consistency()

    storm = (f"INSERT/UPDATE/DELETE 持续 {INC_STORM_SECONDS}s，"
             f"服务{'存活' if APP_PROC.poll() is None else '崩溃 ❌'}")
    write_report(t_ready, t_full, storm)

    log(f"\n最终结论：{'✅ 全部通过' if consistent else '❌ 存在不一致'}")
    if not args.keep_app:
        pid = find_app_pid()
        if pid:
            kill_pid(pid)
            log("应用已停止")
    log(f"详细报告: {REPORT}")


if __name__ == "__main__":
    main()
