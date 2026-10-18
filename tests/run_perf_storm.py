# -*- coding: utf-8 -*-
"""DRPlatform 增量风暴补测：对已全量同步的 45 库 3015 表实例做持续写入压测。

背景：run_perf_45db.py 首轮压测中增量风暴因 tag 超列长/类型不匹配基本无效，
本脚本修复后补测「海量数据 + 持续增量」场景：
  1. 启动应用（perflab profile，水位已在元库，直接进入增量稳态）
  2. 180s 持续 INSERT/UPDATE/DELETE（按列类型生成合法值）
  3. 等收敛后精确 COUNT 比对源/目标行数（验证反压逐步同步、最终一致、不崩溃）

用法：
    python run_perf_storm.py                # 完整风暴补测
    python run_perf_storm.py --seconds 300  # 自定义风暴时长
"""
import argparse
import os
import subprocess
import sys
import threading
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from run_perf_45db import (  # noqa: E402
    SRC, TGT, META, DBS, TABLES_PER_DB, DB_PREFIX, TOTAL_TABLES,
    BASE_URL, ADMIN_USER, ADMIN_PASS, APP_JAR, APP_CWD, APP_LOG, JAVA,
    HEAP_LIMIT_MB, db_conn, db_query, db_exec, log, session, login,
    find_app_pid, kill_pid, verify_consistency,
)

STORM_REPORT = r"D:\WorkSpace\flink-cdc-sync\tests\perf-storm-report.txt"
APP_PROC = None


def log2(msg):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(STORM_REPORT, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def start_app():
    global APP_PROC
    log2("启动同步服务（perflab profile，-Xmx2g，复用已有水位）…")
    pid = find_app_pid()
    if pid:
        kill_pid(pid)
        time.sleep(3)
    env = dict(os.environ, SPRING_PROFILES_ACTIVE="perflab")
    env.pop("SERVER__PORT", None)
    env.pop("SERVER_PORT", None)
    args = [JAVA, f"-Xmx{HEAP_LIMIT_MB}m", "-Xms512m",
            "-XX:+HeapDumpOnOutOfMemoryError",
            f"-XX:HeapDumpPath={APP_CWD}\\heapdump-storm.hprof",
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
            log2(f"应用就绪，耗时 {time.time() - t0:.0f}s（pid={APP_PROC.pid}）")
            return time.time()
        except Exception:
            continue
    raise RuntimeError("应用启动超时（300s）")


def wait_steady(timeout=600):
    """等同步引擎进入稳态：水位数与目标端行数连续 3 次采样（15s）稳定。"""
    log2("等待同步引擎进入稳态（加载已有水位，进入增量模式）…")
    t0 = time.time()
    last_sig, stable = None, 0
    while time.time() - t0 < timeout:
        try:
            wm = int(db_query(META, "SELECT COUNT(*) AS c FROM sync_watermark "
                                    "WHERE SOURCE_DB LIKE 'perf_db_%'", "drplatform")[0]["c"])
            trows = int(db_query(TGT, "SELECT COALESCE(SUM(TABLE_ROWS),0) AS r "
                                      "FROM information_schema.TABLES "
                                      "WHERE TABLE_SCHEMA LIKE 'perf_db_%'")[0]["r"])
            sig = (wm, trows)
            stable = stable + 1 if sig == last_sig else 0
            last_sig = sig
            log2(f"  稳态观察: 水位 {wm}/{TOTAL_TABLES}，目标约 {trows} 行"
                 f"（稳定 {stable}/3）")
            if stable >= 3:
                log2(f"✅ 已进入稳态（水位 {wm}，目标约 {trows} 行）")
                return
        except Exception as e:
            log2(f"  稳态观察异常（继续）: {str(e)[:120]}")
        time.sleep(5)
    log2("⚠️ 稳态等待超时（继续风暴）")


def monitor_resources(duration, interval=5):
    """风暴期间周期性采样 JVM 线程/堆（线程安全写入列表）。"""
    import json

    import requests
    s = requests.Session()
    s.trust_env = False
    rows = []
    t_end = time.time() + duration
    while time.time() < t_end:
        try:
            r1 = s.get(f"{BASE_URL}/actuator/metrics/jvm.threads.live", timeout=10)
            r2 = s.get(f"{BASE_URL}/actuator/metrics/jvm.memory.used?tag=area:heap", timeout=10)
            if r1.status_code == 200 and r2.status_code == 200:
                th = json.loads(r1.text)["measurements"][0]["value"]
                heap = json.loads(r2.text)["measurements"][0]["value"] / 1048576
                rows.append((datetime.now().strftime("%H:%M:%S"), int(th), round(heap)))
        except Exception:
            pass
        time.sleep(interval)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=180)
    ap.add_argument("--keep-app", action="store_true")
    args = ap.parse_args()

    with open(STORM_REPORT, "w", encoding="utf-8") as f:
        f.write(f"DRPlatform 增量风暴补测 {datetime.now()}\n")

    t_ready = start_app()
    wait_steady()

    # 并发启动资源监控
    mon_result = {}

    def _mon():
        mon_result["rows"] = monitor_resources(args.seconds + 120)

    mon = threading.Thread(target=_mon, daemon=True)
    mon.start()

    # 风暴（复用 run_perf_45db 的修复版逻辑：按列类型生成合法值）
    import run_perf_45db as R
    R.INC_STORM_SECONDS = args.seconds
    R.log = log2  # 风暴日志写入本报告
    R.incremental_storm(None, t_ready)

    # 收敛 + 校验
    time.sleep(5)
    R.verify_consistency()
    mon.join(timeout=30)
    rows = mon_result.get("rows", [])

    alive = APP_PROC is not None and APP_PROC.poll() is None
    peak_th = max((r[1] for r in rows), default=0)
    peak_heap = max((r[2] for r in rows), default=0)
    lines = [
        "",
        "=" * 64,
        "DRPlatform 增量风暴补测报告（45 库 × 67 表 = 3015 表）",
        "=" * 64,
        f"风暴时长           : {args.seconds}s",
        f"服务进程           : {'存活 ✅' if alive else '已退出 ❌'}",
        f"OOM                : {'未发生' if alive else '需检查 ❌'}",
        f"峰值活动线程数     : {peak_th}",
        f"峰值 JVM 堆        : {peak_heap} MB / {HEAP_LIMIT_MB} MB",
        f"资源采样           : {len(rows)} 点（每 5s）",
        "=" * 64,
    ]
    for ln in lines:
        log2(ln)

    if not args.keep_app:
        pid = find_app_pid()
        if pid:
            kill_pid(pid)
            log2("应用已停止")
    log2(f"详细报告: {STORM_REPORT}")


if __name__ == "__main__":
    main()
