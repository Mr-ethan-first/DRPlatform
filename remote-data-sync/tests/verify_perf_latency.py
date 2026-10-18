# -*- coding: utf-8 -*-
"""
GeoDRSync 实时同步时效性（端到端延迟）基准测试
=============================================
环境（与 Docker 验证一致，root/123456）：
  - 源库 127.0.0.1:13306   geo_source.t_order
  - 目标库 127.0.0.1:13307  geo_source.t_order（由 CDC 同步）
  - 元库 127.0.0.1:3306     DRPlatform

测量内容（端到端：源库事务提交 → 目标库可见）：
  INSERT 延迟、UPDATE 延迟、DELETE 延迟、DDL(ADD COLUMN) 延迟、批量(100 行)延迟
  输出 p50/p95/p99/min/max/avg 与批量吞吐（行/秒）。

用法（Git Bash）：
  /c/Users/Administrator/.workbuddy/binaries/python/envs/default/Scripts/python.exe \
      tests/verify_perf_latency.py
"""
import os
import sys
import time
import json
import shutil
import subprocess
from datetime import datetime

import pymysql

# ---------------------------------------------------------------- 常量
DOCKER = r"C:\Program Files\Docker\Docker\resources\bin\docker.exe"
JAVA = r"D:\software\jdk-21.0.11\bin\java.exe"
APP_JAR = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync\target\remote-data-sync.jar"
APP_CWD = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync"
REPORT_JSON = os.path.join(APP_CWD, "target", "perf-latency.json")
LOG_FILE = os.path.join(APP_CWD, "target", "perf-latency-run.log")
COOKIE = os.path.join(APP_CWD, "target", "latency-cookie.txt")
APP_URL = "http://127.0.0.1:8899"

SRC = {"host": "127.0.0.1", "port": 13306}
TGT = {"host": "127.0.0.1", "port": 13307}
AUTH = {"user": "root", "password": "123456"}
SCHEMA = "geo_source"
TABLE = "t_order"
NONCE = datetime.now().strftime("%Y%m%d%H%M%S")

proc = None


def log(msg):
    line = f"[{datetime.now():%H:%M:%S}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ---------------------------------------------------------------- 数据库工具
def db(cfg, database=None, timeout=10):
    return pymysql.connect(
        host=cfg["host"], port=cfg["port"], user=AUTH["user"], password=AUTH["password"],
        database=database, charset="utf8mb4", autocommit=True,
        connect_timeout=5, read_timeout=timeout,
    )


def tgt_poll(predicate_fn, timeout=30, interval=0.2):
    """反复在目标库执行 predicate_fn()（返回 True 表示已可见），返回 (found, elapsed_s)"""
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        try:
            if predicate_fn():
                return True, time.perf_counter() - _t0
        except Exception:
            pass
        time.sleep(interval)
    return False, None


_t0 = 0.0


def measure_insert(n):
    log(f"--- INSERT 延迟测量（{n} 次，0.5s 抖动铺开 checkpoint）---")
    lat = []
    for i in range(n):
        marker = f"lat_{NONCE}_{i}"
        with db(SRC, SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO `t_order` (order_no,user_id,amount,status,remark) "
                "VALUES (%s,%s,%s,%s,%s)",
                (marker, 100000 + i, round(10.5 + i, 2), 1, "lat"),
            )
        global _t0
        _t0 = time.perf_counter()
        found, el = tgt_poll(
            lambda m=marker: _exists(m), timeout=30
        )
        if found:
            lat.append(el)
        else:
            log(f"  !! INSERT 超时未同步: {marker}")
        if (i + 1) % 20 == 0:
            log(f"  已测 {i+1}/{n}, 当前均值 {sum(lat)/len(lat):.2f}s" if lat else "  无样本")
        time.sleep(0.5)
    return stats(lat, "INSERT")


def _exists(marker):
    with db(TGT, SCHEMA) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM `t_order` WHERE order_no=%s LIMIT 1", (marker,))
        return cur.fetchone() is not None


def _value_is(marker, col, val):
    with db(TGT, SCHEMA) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT `{col}` FROM `t_order` WHERE order_no=%s LIMIT 1", (marker,))
        r = cur.fetchone()
        return r is not None and str(r[0]) == str(val)


def _gone(marker):
    with db(TGT, SCHEMA) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM `t_order` WHERE order_no=%s LIMIT 1", (marker,))
        return cur.fetchone() is None


def measure_update(n):
    log(f"--- UPDATE 延迟测量（{n} 次）---")
    # 预插 n 行并等待同步
    markers = []
    for i in range(n):
        m = f"upd_{NONCE}_{i}"
        with db(SRC, SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO `t_order` (order_no,user_id,amount,status,remark) VALUES (%s,%s,%s,%s,%s)",
                (m, 200000 + i, 20.0 + i, 0, "upd"),
            )
        markers.append(m)
    for m in markers:
        ok, _ = tgt_poll(lambda mm=m: _exists(mm), timeout=40)
        if not ok:
            log(f"  !! UPDATE 预插未同步: {m}")
    lat = []
    for i, m in enumerate(markers):
        new_status = 9
        with db(SRC, SCHEMA) as conn, conn.cursor() as cur:
            cur.execute("UPDATE `t_order` SET status=%s WHERE order_no=%s", (new_status, m))
        global _t0
        _t0 = time.perf_counter()
        found, el = tgt_poll(lambda mm=m, v=new_status: _value_is(mm, "status", v), timeout=30)
        if found:
            lat.append(el)
        else:
            log(f"  !! UPDATE 超时: {m}")
    return stats(lat, "UPDATE")


def measure_delete(n):
    log(f"--- DELETE 延迟测量（{n} 次）---")
    markers = []
    for i in range(n):
        m = f"del_{NONCE}_{i}"
        with db(SRC, SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO `t_order` (order_no,user_id,amount,status,remark) VALUES (%s,%s,%s,%s,%s)",
                (m, 300000 + i, 30.0 + i, 0, "del"),
            )
        markers.append(m)
    for m in markers:
        ok, _ = tgt_poll(lambda mm=m: _exists(mm), timeout=40)
        if not ok:
            log(f"  !! DELETE 预插未同步: {m}")
    lat = []
    for i, m in enumerate(markers):
        with db(SRC, SCHEMA) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM `t_order` WHERE order_no=%s", (m,))
        global _t0
        _t0 = time.perf_counter()
        found, el = tgt_poll(lambda mm=m: _gone(mm), timeout=30)
        if found:
            lat.append(el)
        else:
            log(f"  !! DELETE 超时: {m}")
    return stats(lat, "DELETE")


def measure_ddl(n):
    log(f"--- DDL(ADD COLUMN) 延迟测量（{n} 次）---")
    cols = []
    lat = []
    for i in range(n):
        col = f"lat_col_{NONCE}_{i}"
        with db(SRC, SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(f"ALTER TABLE `t_order` ADD COLUMN `{col}` varchar(32) DEFAULT NULL")
        cols.append(col)
        global _t0
        _t0 = time.perf_counter()
        found, el = tgt_poll(lambda c=col: _col_exists(c), timeout=40)
        if found:
            lat.append(el)
        else:
            log(f"  !! DDL 超时: {col}")
    # 清理新增列（在源端 DROP，CDC 同步到目标）
    for c in cols:
        try:
            with db(SRC, SCHEMA) as conn, conn.cursor() as cur:
                cur.execute(f"ALTER TABLE `t_order` DROP COLUMN `{c}`")
        except Exception as e:
            log(f"  DDL 清理失败 {c}: {e}")
    return stats(lat, "DDL_ADD_COLUMN")


def _col_exists(col):
    with db(TGT, SCHEMA) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_schema='geo_source' "
            "AND table_name='t_order' AND column_name=%s LIMIT 1",
            (col,),
        )
        return cur.fetchone() is not None


def measure_batch(n):
    log(f"--- 批量写入延迟（{n} 行，单事务逐条提交）---")
    markers = [f"batch_{NONCE}_{i}" for i in range(n)]
    t_start = time.perf_counter()
    for i, m in enumerate(markers):
        with db(SRC, SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO `t_order` (order_no,user_id,amount,status,remark) VALUES (%s,%s,%s,%s,%s)",
                (m, 400000 + i, 40.0 + i, 0, "batch"),
            )
    commit_done = time.perf_counter()
    # 等待全部到达
    deadline = time.time() + 120
    done = 0
    while time.time() < deadline:
        with db(TGT, SCHEMA) as conn, conn.cursor() as cur:
            ph = ",".join(["%s"] * len(markers))
            cur.execute(f"SELECT COUNT(*) FROM `t_order` WHERE order_no IN ({ph})", markers)
            done = cur.fetchone()[0]
        if done >= n:
            break
        time.sleep(0.3)
    all_done = time.perf_counter()
    batch_latency = all_done - commit_done
    sustained = n / batch_latency if batch_latency > 0 else 0
    log(f"  批量提交耗时 {commit_done - t_start:.2f}s，末行可见耗时 {batch_latency:.2f}s，"
        f"稳态吞吐 {sustained:.1f} 行/s，到达 {done}/{n}")
    return {
        "op": "BATCH_INSERT", "n": n,
        "batch_latency_s": round(batch_latency, 3),
        "sustained_rows_per_s": round(sustained, 1),
        "arrived": done,
    }


def stats(lat, op):
    if not lat:
        return {"op": op, "samples": 0}
    s = sorted(lat)
    p = lambda q: s[min(len(s) - 1, int(q * len(s)))]
    r = {
        "op": op, "samples": len(s),
        "min_s": round(s[0], 3),
        "p50_s": round(p(0.5), 3),
        "p95_s": round(p(0.95), 3),
        "p99_s": round(p(0.99), 3),
        "max_s": round(s[-1], 3),
        "avg_s": round(sum(s) / len(s), 3),
    }
    log(f"  {op}: 样本={len(s)} min={r['min_s']} p50={r['p50_s']} "
        f"p95={r['p95_s']} p99={r['p99_s']} max={r['max_s']} avg={r['avg_s']}")
    return r


# ---------------------------------------------------------------- App 管理
def app_env():
    return {k: v for k, v in os.environ.items() if k not in ("SERVER__PORT", "SERVER_PORT")}


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
    global proc
    free_port_8899()
    app_out = open(os.path.join(APP_CWD, "target", "app-latency.log"), "ab")
    proc = subprocess.Popen(
        [JAVA, "-Xmx2g", "-jar", APP_JAR, "--server.port=8899"],
        cwd=APP_CWD, env=app_env(), stdout=app_out, stderr=subprocess.STDOUT,
    )
    return proc


def login():
    r = subprocess.run(
        ["curl", "-s", "--noproxy", "*", "--max-time", "10", "-c", COOKIE,
         "-X", "POST", "-H", "Content-Type: application/json",
         "-d", json.dumps({"username": "admin", "password": "admin123"}),
         APP_URL + "/auth/login"],
        capture_output=True, timeout=20,
    )
    try:
        return bool(json.loads(r.stdout.decode("utf-8", "replace").strip()).get("success"))
    except Exception:
        return False


def wait_app_ready(timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if login():
            rr = subprocess.run(
                ["curl", "-s", "--noproxy", "*", "--max-time", "10", "-b", COOKIE,
                 APP_URL + "/sync/cdc/jobs"],
                capture_output=True, timeout=20,
            )
            if rr.stdout.strip():
                return True
        if proc is not None and proc.poll() is not None:
            log(f"应用进程提前退出 exit={proc.returncode}")
            return False
        time.sleep(4)
    return False


def clear_checkpoints():
    root = os.path.join(APP_CWD, "savepoints")
    if os.path.isdir(root):
        shutil.rmtree(root, ignore_errors=True)
        log(f"已清理历史 checkpoint 目录 {root}")


def wait_converged(timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with db(SRC, SCHEMA) as c1, db(TGT, SCHEMA) as c2:
            cur1, cur2 = c1.cursor(), c2.cursor()
            cur1.execute("SELECT COUNT(*) FROM `t_order`")
            cur2.execute("SELECT COUNT(*) FROM `t_order`")
            s, t = cur1.fetchone()[0], cur2.fetchone()[0]
        if s == t:
            log(f"已收敛 src={s} tgt={t}")
            return True
        time.sleep(5)
    return False


def cleanup_test_rows():
    log("--- 清理测试行（源端删除，CDC 同步到目标）---")
    prefixes = ("lat_", "upd_", "del_", "batch_")
    ph = " OR ".join(["order_no LIKE %s"] * len(prefixes))
    params = [p + "%" for p in prefixes]
    with db(SRC, SCHEMA) as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM `t_order` WHERE {ph}", params)
    time.sleep(8)
    log("测试行清理完成")


# ---------------------------------------------------------------- 主流程
def main():
    global proc
    log("==== 实时同步时效性基准测试开始 ====")
    clear_checkpoints()
    start_app()
    if not wait_app_ready():
        log("应用启动失败，终止")
        return
    log("应用就绪，等待 geo_source.t_order 收敛 ...")
    wait_converged(timeout=200)

    results = {}
    results["insert"] = measure_insert(60)
    results["update"] = measure_update(30)
    results["delete"] = measure_delete(30)
    results["ddl"] = measure_ddl(3)
    results["batch"] = measure_batch(100)

    cleanup_test_rows()

    results["meta"] = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "engine": "GeoDRSync (Flink CDC 3.7 / Flink 2.2 MiniCluster)",
        "source": "127.0.0.1:13306 geo_source.t_order",
        "target": "127.0.0.1:13307 geo_source.t_order",
        "checkpoint_interval_s": 3,
        "jvm": "-Xmx2g",
    }
    with open(REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    log("==== 实时同步时效性基准测试完成 ====")
    log(f"JSON 报告: {REPORT_JSON}")


if __name__ == "__main__":
    main()
