# -*- coding: utf-8 -*-
"""
Docker 环境全自动化端到端验证（GeoDRSync / Flink CDC 模式）
============================================================
环境（前期已用过的老容器，root/123456）：
  - drp-src-mysql  127.0.0.1:13306  源库（mysql:8.0, binlog ROW）
  - drp-tgt-mysql  127.0.0.1:13307  目标库（mysql:8.0）
  - mysql8         127.0.0.1:3306   应用元数据库 DRPlatform（mysql:8.4）

验证阶段：
  A. 初始快照一致性 —— 清空老表后播种源库，等待全量快照收敛，逐表比对行数+内容哈希
  B. 增量同步实时性 —— 逐行 INSERT / UPDATE / DELETE，测量目标库落库延迟（要求 < 10s）
  B2. DDL 同步      —— ALTER TABLE ADD COLUMN + 新列数据写入，验证目标库结构/数据一致
  C. 断网恢复一致性 —— docker pause 目标容器（目标库不可达）期间持续写源库，
                       解除后验证最终收敛、不丢不重
  D. 断电恢复一致性 —— 持续写入过程中 kill -9 应用进程，重启后从 checkpoint 恢复，
                       验证行数精确一致 + 内容哈希一致

用法（Git Bash）：
  /c/Users/Administrator/.workbuddy/binaries/python/envs/default/Scripts/python.exe \
      tests/verify_docker_e2e.py
"""
import os
import sys
import time
import json
import shutil
import hashlib
import subprocess
import urllib.request
from datetime import datetime

import pymysql

# ---------------------------------------------------------------- 常量
DOCKER = r"C:\Program Files\Docker\Docker\resources\bin\docker.exe"
JAVA = r"D:\software\jdk-21.0.11\bin\java.exe"
APP_JAR = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync\target\remote-data-sync.jar"
APP_CWD = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync"
REPORT_MD = os.path.join(APP_CWD, "target", "docker-e2e-report.md")
LOG_FILE = os.path.join(APP_CWD, "target", "docker-e2e-run.log")
COOKIE = os.path.join(APP_CWD, "target", "e2e-cookie.txt")
APP_URL = "http://127.0.0.1:8899"

SRC = {"host": "127.0.0.1", "port": 13306}
TGT = {"host": "127.0.0.1", "port": 13307}
AUTH = {"user": "root", "password": "123456"}
USER_DBS = ["geo_source", "biz_source"]
LAG_LIMIT = 10.0          # 增量同步延迟硬指标（秒）
PAUSE_SECONDS = 15        # 断网持续时间

RESULTS = []              # [(phase, item, pass, detail)]


def log(msg):
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def record(phase, item, ok, detail=""):
    RESULTS.append((phase, item, ok, detail))
    log(f"  {'PASS' if ok else 'FAIL'} | {phase} | {item} | {detail}")


# ---------------------------------------------------------------- 数据库工具
def db(cfg, database=None):
    return pymysql.connect(
        host=cfg["host"], port=cfg["port"], user=AUTH["user"], password=AUTH["password"],
        database=database, charset="utf8mb4", autocommit=True,
        connect_timeout=5, read_timeout=60,
    )


def user_tables(cfg):
    """返回 {db: [table,...]}（仅基表，排除视图）"""
    out = {}
    with db(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_type='BASE TABLE' AND table_schema IN (%s,%s)",
                tuple(USER_DBS),
            )
            for d, t in cur.fetchall():
                out.setdefault(d, []).append(t)
    return out


def table_columns(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type, extra FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (schema, table),
        )
        return cur.fetchall()  # [(name, type, extra)]


def table_snapshot(cfg, schema, table):
    """(行数, 内容哈希)。主键/全部列排序后逐行取值做 md5，对内容字节级敏感。"""
    with db(cfg) as conn:
        cols = [c[0] for c in table_columns(conn, schema, table)]
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{schema}`.`{table}`")
            cnt = cur.fetchone()[0]
            if cnt == 0:
                return 0, hashlib.md5(b"EMPTY").hexdigest()
            collist = ",".join(f"`{c}`" for c in cols)
            cur.execute(f"SELECT {collist} FROM `{schema}`.`{table}` ORDER BY {collist}")
            h = hashlib.md5()
            for row in cur.fetchall():
                line = "\x1f".join("" if v is None else str(v) for v in row)
                h.update(line.encode("utf-8", "replace"))
                h.update(b"\x1e")
            return cnt, h.hexdigest()


def compare_all(tag):
    """全库逐表比对，返回 (全部一致?, 明细文本)"""
    src_tabs, tgt_tabs = user_tables(SRC), user_tables(TGT)
    ok_all, lines = True, []
    for d in USER_DBS:
        s_tabs, t_tabs = set(src_tabs.get(d, [])), set(tgt_tabs.get(d, []))
        if s_tabs != t_tabs:
            ok_all = False
            lines.append(f"  {d}: 表集合不一致 src-tgt={s_tabs - t_tabs} tgt-src={t_tabs - s_tabs}")
            continue
        for t in sorted(s_tabs):
            sc, sh = table_snapshot(SRC, d, t)
            tc, th = table_snapshot(TGT, d, t)
            same = (sc == tc and sh == th)
            ok_all &= same
            lines.append(f"  {d}.{t}: src={sc}({sh[:8]}) tgt={tc}({th[:8]}) {'OK' if same else 'MISMATCH'}")
    log(f"[{tag}] 一致性比对:\n" + "\n".join(lines))
    return ok_all, "\n".join(lines)


def wait_converged(tag, timeout=180, interval=4):
    """轮询直到全库一致或超时。返回 (是否收敛, 明细)"""
    detail = ""
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok, detail = compare_all(tag)
        if ok:
            log(f"[{tag}] 已收敛")
            return True, detail
        time.sleep(interval)
    return False, detail


def insert_row(schema, table, cols, i, extra_cols=None):
    """按列类型生成一行数据并写入源库，返回完整列值 dict"""
    with db(SRC) as conn:
        coldefs = table_columns(conn, schema, table)
        names, vals = [], []
        for name, dtype, extra in coldefs:
            if "auto_increment" in (extra or ""):
                continue
            names.append(name)
            if dtype in ("int", "bigint", "smallint", "tinyint"):
                vals.append(10000 + i)
            elif dtype in ("decimal", "double", "float"):
                vals.append(round(10.5 + i, 2))
            elif dtype in ("datetime", "timestamp"):
                vals.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            elif dtype == "date":
                vals.append(datetime.now().strftime("%Y-%m-%d"))
            else:
                vals.append(f"v{i}_{extra_cols.get(name)}" if extra_cols and name in extra_cols else f"v{i}")
        collist = ",".join(f"`{n}`" for n in names)
        ph = ",".join(["%s"] * len(vals))
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO `{schema}`.`{table}` ({collist}) VALUES ({ph})", vals)
    return time.time()


# ---------------------------------------------------------------- App 进程管理
def app_env():
    env = {k: v for k, v in os.environ.items() if k not in ("SERVER__PORT", "SERVER_PORT")}
    return env


def free_port_8899():
    """启动前确保 8899 未被占用（残留进程会干扰验证）"""
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
    app_out = open(os.path.join(APP_CWD, "target", "app-e2e.log"), "ab")
    proc_ = subprocess.Popen(
        [JAVA, "-Xmx2g", "-jar", APP_JAR, "--server.port=8899"],
        cwd=APP_CWD, env=app_env(),
        stdout=app_out, stderr=subprocess.STDOUT,
    )
    return proc_


def http_get(path, timeout=15, method="GET", body=None):
    """用 curl 直连（本机有 HTTP_PROXY，urllib 在沙箱下不可靠）；自动带登录会话"""
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
            log(f"  {path} 返回失败: {str(obj.get('message'))[:120]}")
            return None
        if isinstance(obj, dict) and "data" in obj:
            return obj["data"]
        return obj
    except Exception as e:
        log(f"  http_get({path}) 异常: {e}")
        return None


def login():
    """登录（默认种子账号 admin/admin123），写入 cookie jar"""
    r = subprocess.run(
        ["curl", "-s", "--noproxy", "*", "--max-time", "10", "-c", COOKIE,
         "-X", "POST", "-H", "Content-Type: application/json",
         "-d", json.dumps({"username": "admin", "password": "admin123"}),
         APP_URL + "/auth/login"],
        capture_output=True, timeout=20,
    )
    txt = r.stdout.decode("utf-8", "replace").strip()
    try:
        return bool(json.loads(txt).get("success"))
    except Exception:
        return False


def wait_app_ready(timeout=180):
    """就绪判定：能登录 + /sync/cdc/jobs 返回成功（鉴权与 CDC 状态 API 均可用）"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if login():
            jobs = http_get("/sync/cdc/jobs")
            if jobs is not None:
                return True
        if proc is not None and proc.poll() is not None:
            log(f"应用进程提前退出 exit={proc.returncode}")
            return False
        time.sleep(4)
    return False


def kill_app_hard():
    """kill -9 / taskkill /F（Windows 强杀）"""
    if proc is None or proc.poll() is not None:
        return
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                   capture_output=True)
    try:
        proc.wait(timeout=15)
    except Exception:
        pass
    log(f"应用已强杀 pid={proc.pid}")


def cdc_job_state():
    jobs = http_get("/sync/cdc/jobs")
    if isinstance(jobs, list) and jobs:
        return jobs[0] if isinstance(jobs[0], dict) else None
    return None


# ---------------------------------------------------------------- Docker 工具
def docker_cmd(*args, timeout=60):
    r = subprocess.run([DOCKER, *args], capture_output=True, timeout=timeout)
    return r.stdout.decode("utf-8", "replace").strip(), r.stderr.decode("utf-8", "replace").strip(), r.returncode


# ---------------------------------------------------------------- 阶段实现
def clear_checkpoints():
    """清掉历史 checkpoint/savepoint，保证阶段 A 走真实全量快照（不被旧位点跳过）"""
    root = os.path.join(APP_CWD, "savepoints")
    if os.path.isdir(root):
        shutil.rmtree(root, ignore_errors=True)
        log(f"已清理历史 checkpoint 目录 {root}")


def phase_prepare():
    log("=== 阶段 0：环境重置与播种 ===")
    clear_checkpoints()
    with db(SRC) as conn:
        for d in USER_DBS:
            tabs = user_tables(SRC).get(d, [])
            with conn.cursor() as cur:
                for t in tabs:
                    cur.execute(f"TRUNCATE TABLE `{d}`.`{t}`")
    # 目标库同样清空（老数据残留会导致快照后多余行误判）
    with db(TGT) as conn:
        for d in USER_DBS:
            for t in user_tables(TGT).get(d, []):
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE `{d}`.`{t}`")
    # 幂等清理：上一轮 DDL 阶段新增的 remark 列
    try_exec(SRC, "ALTER TABLE geo_source.t_user DROP COLUMN remark")
    try_exec(TGT, "ALTER TABLE geo_source.t_user DROP COLUMN remark")
    # 播种：geo_source.t_order 3 行 / geo_source.t_user 2 行 / biz_source.t_customer 2 行
    seeds = [("geo_source", "t_order", 3), ("geo_source", "t_user", 2), ("biz_source", "t_customer", 2)]
    n = 0
    for d, t, k in seeds:
        for i in range(k):
            insert_row(d, t, None, n)
            n += 1
    log(f"播种完成 共 {n} 行")


def phase_a_snapshot():
    log("=== 阶段 A：初始快照一致性 ===")
    t0 = time.time()
    ok, detail = wait_converged("A-快照", timeout=240, interval=5)
    record("A-快照", "全库行数+内容哈希一致", ok, f"耗时 {time.time()-t0:.0f}s; {detail.splitlines()[-1] if detail else ''}")
    return ok


def phase_b_incremental():
    log("=== 阶段 B：增量同步实时性 ===")
    latencies = {}

    def measure(tag, op, expect_sql, timeout=30):
        """执行写操作后轮询目标库直到满足 expect_sql，返回延迟"""
        t = op()
        deadline = time.time() + timeout
        while time.time() < deadline:
            with db(TGT) as conn:
                with conn.cursor() as cur:
                    cur.execute(expect_sql)
                    if cur.fetchone()[0] > 0:
                        lat = time.time() - t
                        latencies[tag] = lat
                        log(f"  {tag}: 延迟 {lat:.2f}s")
                        return lat
            time.sleep(0.3)
        latencies[tag] = None
        return None

    # 10 行逐条 INSERT，逐条测延迟
    max_lat = 0.0
    all_ok = True
    for i in range(10):
        j = 100 + i
        lat = measure(
            f"insert#{j}",
            lambda jj=j: insert_row("geo_source", "t_order", None, jj),
            f"SELECT COUNT(*) FROM geo_source.t_order WHERE order_no='v{j}'",
        )
        if lat is None:
            all_ok = False
            break
        max_lat = max(max_lat, lat)
    record("B-增量", "10 行 INSERT 全部到达且 max 延迟 < 10s", all_ok and max_lat < LAG_LIMIT,
           f"max={max_lat:.2f}s (阈值 {LAG_LIMIT}s)" if all_ok else "有行超时未到达")

    # UPDATE（t_order 无 customer_name，用 status 列）
    lat = measure(
        "update",
        lambda: (_exec_src("UPDATE geo_source.t_order SET status=777 WHERE order_no='v105'"), time.time())[1],
        "SELECT COUNT(*) FROM geo_source.t_order WHERE order_no='v105' AND status=777",
    )
    record("B-增量", "UPDATE 同步延迟 < 10s", lat is not None and lat < LAG_LIMIT,
           f"{lat:.2f}s" if lat else "超时未到达")

    # DELETE（反向断言：目标库该行消失）
    _exec_src("DELETE FROM geo_source.t_order WHERE order_no='v103'")
    t0 = time.time()
    gone = False
    deadline = time.time() + 30
    while time.time() < deadline:
        with db(TGT) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM geo_source.t_order WHERE order_no='v103'")
            if cur.fetchone()[0] == 0:
                gone = True
                break
        time.sleep(0.3)
    lat_del = time.time() - t0 if gone else None
    record("B-增量", "DELETE 同步延迟 < 10s", gone and lat_del < LAG_LIMIT,
           f"{lat_del:.2f}s" if lat_del else "超时未删除")

    # 收敛终态
    ok, detail = wait_converged("B-收敛", timeout=60, interval=4)
    record("B-增量", "增量后全库终态一致", ok, "")
    return all_ok


def _exec_src(sql):
    with db(SRC) as conn, conn.cursor() as cur:
        cur.execute(sql)


def try_exec(cfg, sql):
    """尽力执行，失败忽略（用于幂等清理）"""
    try:
        with db(cfg) as conn, conn.cursor() as cur:
            cur.execute(sql)
        return True
    except Exception as e:
        log(f"  忽略: {sql} -> {str(e)[:80]}")
        return False


def phase_b2_ddl():
    log("=== 阶段 B2：DDL 同步 ===")
    _exec_src("ALTER TABLE geo_source.t_user ADD COLUMN remark VARCHAR(64) NULL")
    t0 = time.time()
    ok_col = False
    deadline = time.time() + 30
    while time.time() < deadline:
        with db(TGT) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema='geo_source' AND table_name='t_user' AND column_name='remark'")
            if cur.fetchone()[0] == 1:
                ok_col = True
                break
        time.sleep(0.5)
    lat = time.time() - t0
    record("B2-DDL", "ADD COLUMN 同步 < 10s", ok_col and lat < LAG_LIMIT, f"{lat:.2f}s")

    # 新列数据写入（通用生成器会填充 remark 列）
    insert_row("geo_source", "t_user", None, 900)
    ok, _ = wait_converged("B2-收敛", timeout=60, interval=4)
    record("B2-DDL", "新列数据写入后全库一致", ok, "")
    return ok_col


def _has_user_name_col():
    with db(SRC) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='geo_source' "
                    "AND table_name='t_user' AND column_name='user_name'")
        return cur.fetchone()[0] == 1


def phase_c_network_cut():
    log("=== 阶段 C：断网（目标容器 pause）一致性 ===")
    out, err, rc = docker_cmd("pause", "drp-tgt-mysql")
    if rc != 0:
        record("C-断网", "docker pause", False, f"{out} {err}")
        return False
    log(f"目标库已 pause，持续 {PAUSE_SECONDS}s，期间写入 20 行")

    write_fail = 0
    for i in range(20):
        try:
            insert_row("geo_source", "t_order", None, 500 + i)
        except Exception as e:
            write_fail += 1
            log(f"  源库写入失败 #{i}: {e}")
        time.sleep(0.5)
    record("C-断网", "断网期间源库写入不受影响", write_fail == 0, f"失败 {write_fail}/20")

    with db(SRC) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM geo_source.t_order")
        src_cnt_pause = cur.fetchone()[0]

    out, err, rc = docker_cmd("unpause", "drp-tgt-mysql")
    if rc != 0:
        record("C-断网", "docker unpause", False, f"{out} {err}")
        return False
    log("目标库已恢复，等待收敛 ...")
    ok, detail = wait_converged("C-收敛", timeout=240, interval=6)
    # 终态行数精确断言
    with db(SRC) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM geo_source.t_order")
        src_cnt = cur.fetchone()[0]
    record("C-断网", "恢复后全库一致（不丢不重）", ok and src_cnt == src_cnt_pause,
           f"src={src_cnt} 期望={src_cnt_pause}")
    # lag 恢复（轮询最多 90s，作业可能短暂 RESTARTING）
    lag_ok = False
    job_desc = "无作业状态"
    deadline = time.time() + 90
    while time.time() < deadline:
        job = cdc_job_state()
        if job is not None and not job.get("lagging", False):
            lag_ok = True
            job_desc = json.dumps(job, ensure_ascii=False)[:200]
            break
        time.sleep(5)
    record("C-断网", "CDC 作业 lag 状态恢复正常", lag_ok, job_desc)
    return ok


def phase_d_power_cut():
    global proc
    log("=== 阶段 D：断电（kill -9 应用）一致性 ===")
    # 快速连续写入，中途强杀
    kill_after = 8
    written = []
    for i in range(20):
        try:
            insert_row("geo_source", "t_order", None, 700 + i)
            written.append(700 + i)
            if i + 1 == kill_after:
                kill_app_hard()
                log(f"已写入 {i+1} 行后强杀应用")
                break
        except Exception as e:
            log(f"  写入异常: {e}")
            break
        time.sleep(0.2)

    with db(SRC) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM geo_source.t_order")
        src_cnt_kill = cur.fetchone()[0]

    # 重启
    log("重启应用（从 checkpoint 恢复）...")
    proc = start_app()
    if not wait_app_ready(timeout=150):
        record("D-断电", "应用重启成功", False, "等待就绪超时")
        return False
    record("D-断电", "应用重启成功（checkpoint 恢复）", True, "")

    ok, detail = wait_converged("D-收敛", timeout=240, interval=6)
    with db(SRC) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM geo_source.t_order")
        src_cnt_final = cur.fetchone()[0]
    record("D-断电", "重启后收敛且行数精确一致", ok and src_cnt_final == src_cnt_kill,
           f"src={src_cnt_final} kill时={src_cnt_kill}")

    # 重启后增量能力仍在
    t0 = time.time()
    insert_row("geo_source", "t_order", None, 800)
    arrived = False
    deadline = time.time() + 30
    while time.time() < deadline:
        with db(TGT) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM geo_source.t_order WHERE order_no='v800'")
            if cur.fetchone()[0] > 0:
                arrived = True
                break
        time.sleep(0.3)
    lat = time.time() - t0
    record("D-断电", "重启后增量同步延迟 < 10s", arrived and (time.time() - t0) < LAG_LIMIT,
           f"{lat:.2f}s" if arrived else "超时未到达")
    return ok


def main():
    global proc
    t_start = time.time()
    try:
        os.remove(LOG_FILE)
    except OSError:
        pass
    log("==== Docker E2E 验证开始 ====")

    # 前置检查
    out, _, rc = docker_cmd("ps", "--format", "{{.Names}} {{.Status}}")
    log(f"容器状态: {out}")
    if rc != 0:
        log("docker 不可用，终止")
        sys.exit(2)

    phase_prepare()

    # 启动应用
    global proc
    proc = start_app()
    if not wait_app_ready():
        record("前置", "应用启动", False, "等待就绪超时，查看 app 日志")
        kill_app_hard()
        write_report(t_start)
        sys.exit(3)
    record("前置", "应用启动（8899）", True, "")

    try:
        phase_a_snapshot()
        phase_b_incremental()
        phase_b2_ddl()
        phase_c_network_cut()
        phase_d_power_cut()
    finally:
        kill_app_hard()

    write_report(t_start)
    fails = [r for r in RESULTS if not r[2]]
    log(f"==== 验证结束：{'全部通过' if not fails else f'{len(fails)} 项失败'} ====")
    sys.exit(0 if not fails else 1)


def write_report(t_start):
    os.makedirs(os.path.dirname(REPORT_MD), exist_ok=True)
    fails = [r for r in RESULTS if not r[2]]
    lines = [
        "# GeoDRSync Docker 全自动化端到端验证报告",
        "",
        f"- 时间：{datetime.now():%Y-%m-%d %H:%M:%S}",
        f"- 总耗时：{time.time()-t_start:.0f}s",
        f"- 结论：**{'全部通过 ✅' if not fails else f'{len(fails)} 项失败 ❌'}**（共 {len(RESULTS)} 项）",
        "",
        "| 阶段 | 验证项 | 结果 | 明细 |",
        "|---|---|---|---|",
    ]
    for ph, item, ok, det in RESULTS:
        det = det.replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {ph} | {item} | {'✅' if ok else '❌'} | {det} |")
    lines += ["", "## 环境", "",
              "- 源库：drp-src-mysql (127.0.0.1:13306, mysql 8.0, binlog ROW)",
              "- 目标库：drp-tgt-mysql (127.0.0.1:13307, mysql 8.0)",
              "- 元数据库：mysql8 (127.0.0.1:3306/DRPlatform)",
              f"- 延迟硬指标：< {LAG_LIMIT}s（增量落库）",
              ""]
    with open(REPORT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log(f"报告已写入 {REPORT_MD}")


if __name__ == "__main__":
    main()
