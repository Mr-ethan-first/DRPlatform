# -*- coding: utf-8 -*-
"""DRPlatform 故障注入 · 数据最终一致性自动化测试（隔离实验室）。

本文件在 tests/docker-lab 搭建的隔离环境上运行：
  - 源 MySQL  : 127.0.0.1:13306 (容器 drp-src-mysql)
  - 目标 MySQL: 127.0.0.1:13307 (容器 drp-tgt-mysql)
  - 同步服务  : 本地 127.0.0.1:8899 (SPRING_PROFILES_ACTIVE=dockerlab)

用 docker stop/start 真实模拟『源库断电/断网』『目标库断电/断网』，
用进程 kill 真实模拟『同步服务进程崩溃 / 主机断电重启』，
验证在各类异常下：已同步数据不丢失、异常恢复后新变更最终一致。

核心断言：
  1) 任何单点故障下，同步服务本身不崩溃（可继续响应管控接口）；
  2) 故障恢复后，源端的新增/更新/删除最终都反映到目标端（最终一致性）；
  3) 服务重启（内存水位丢失→全量重扫）后，依靠 upsert 幂等不重复、不丢数据。
"""
import os
import time
import uuid
import json
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
APP_LOG = r"D:\WorkSpace\flink-cdc-sync\tests\docker-lab\app-lab.log"
JAVA = r"D:\software\jdk-21.0.11\bin\java.exe"
SRC_CTR = "drp-src-mysql"
TGT_CTR = "drp-tgt-mysql"
SYNC_POLL_S = 2  # application-dockerlab.yml engine.poll-interval-ms=2000

# 由本测试用 subprocess.Popen 托管的应用子进程句柄（精确跟踪 PID，避免误杀其它进程）
_APP_PROC = None


# ----------------------------- 基础设施辅助 -----------------------------

def docker(args, timeout=150):
    r = subprocess.run(["docker"] + args, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} 失败: {r.stderr.strip()}")
    return r.stdout.strip()


def db_conn(c, db=None, timeout=10):
    last = None
    for _ in range(6):  # 容器刚启动/握手抖动时重试，避免 (2013) Lost connection
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


def db_count(c, db, table, where=None):
    conn = db_conn(c, db)
    try:
        with conn.cursor() as cur:
            sql = f"SELECT COUNT(1) FROM `{table}`" + (f" WHERE {where}" if where else "")
            cur.execute(sql)
            return cur.fetchone()[0]
    except Exception:
        return -1  # 表尚未建好 / 库不可用
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
    """等待 MySQL 容器可连接（用于 docker start 后）。"""
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

    关键：绝不使用 text=True。中文 Windows 下 PowerShell/tasklist 等按 GBK 输出，
    而本进程 stdout 编码为 UTF-8，text=True 会在 subprocess 的 reader 线程里抛
    UnicodeDecodeError —— 异常不冒泡，调用方只看到 stdout 为空，
    从而把「命令失败/有告警」误判为「进程不存在」（静默假阴性）。
    统一取 bytes 后用 errors="replace" 宽容解码。
    """
    r = subprocess.run(cmd, capture_output=True, timeout=timeout)
    return r.returncode, r.stdout.decode("utf-8", "replace"), r.stderr.decode("utf-8", "replace")


def find_app_pid():
    """查找监听 8899 的进程 PID。找不到返回 None。

    主路径用 netstat -ano（Windows 输出为英文状态名，纯 ASCII，不受 GBK 编码影响）；
    仅当 netstat 不可用时回退 PowerShell（并做宽容解码）。
    """
    rc, out, _ = _run_native(["netstat", "-ano", "-p", "TCP"])
    if rc == 0 and out:
        for line in out.splitlines():
            parts = line.split()
            # TCP  <local> <foreign> <state> <pid>
            if len(parts) >= 5 and parts[0].upper() == "TCP":
                local, state = parts[1], parts[3].upper()
                if state == "LISTENING" and local.rsplit(":", 1)[-1] == "8899":
                    try:
                        return int(parts[4])
                    except ValueError:
                        continue
        return None
    # 回退：PowerShell（宽容解码）
    ps = (r"Get-NetTCPConnection -LocalPort 8899 -State Listen -ErrorAction SilentlyContinue "
          r"| Select-Object -ExpandProperty OwningProcess")
    _, out, _ = _run_native(["powershell", "-NoProfile", "-Command", ps])
    for line in out.splitlines():
        line = line.strip()
        if line.isdigit():
            return int(line)
    return None


def _kill_pid(pid):
    """强杀 PID：先 Stop-Process，失败再退回 taskkill（list 形式，不过 shell，无斜杠坑）。"""
    if not pid:
        return
    rc, _, err = _run_native(["powershell", "-NoProfile", "-Command",
                              f"Stop-Process -Id {pid} -Force"])
    if rc != 0:
        _run_native(["taskkill", "/F", "/PID", str(pid)])
    time.sleep(1)


def stop_app(timeout=60):
    """停止同步服务，并确保 8899 端口确实无人监听后才返回 True。

    必须反复扫描端口并杀掉所有占用者 —— 上一轮运行残留的实例若不被清理，
    新实例会因端口被占而静默起不来，导致后续断言误判。
    """
    global _APP_PROC
    if _APP_PROC is not None and _APP_PROC.poll() is None:
        _kill_pid(_APP_PROC.pid)
    deadline = time.time() + timeout
    while time.time() < deadline:
        pid = find_app_pid()
        if pid is not None:
            _kill_pid(pid)
            continue
        # 端口无人监听：确认 Popen 记录的进程也已退出
        if _APP_PROC is None or _APP_PROC.poll() is not None:
            _APP_PROC = None
            return True
        time.sleep(1)
    _APP_PROC = None
    return find_app_pid() is None


def start_app():
    """启动同步服务（dockerlab profile），等待就绪后返回。

    用 subprocess.Popen 全权托管子进程（而非依赖外部手动启动的实例），
    保证在整个测试运行期内存活，并精确记录 PID 供 stop_app 停止。
    """
    global _APP_PROC
    env = dict(os.environ, SPRING_PROFILES_ACTIVE="dockerlab")
    env.pop("SERVER__PORT", None)
    env.pop("SERVER_PORT", None)
    # 启动前彻底清空 8899：残留实例会占住端口，导致新实例静默起不来
    for _ in range(10):
        occ = find_app_pid()
        if occ is None:
            break
        _kill_pid(occ)
    with open(APP_LOG, "a", encoding="utf-8") as log:
        _APP_PROC = subprocess.Popen([JAVA, "-jar", APP_JAR, "--server.port=8899"],
                                     cwd=APP_CWD, env=env, stdout=log, stderr=subprocess.STDOUT)
    # 等待登录可用（Spring Boot 启动 + 引擎初始化）
    deadline = time.time() + 180
    while time.time() < deadline:
        time.sleep(3)
        if _APP_PROC.poll() is not None:
            raise RuntimeError(f"同步服务进程意外退出（exit={_APP_PROC.poll()}），见 {APP_LOG}")
        try:
            _login()
        except Exception:
            continue  # 应用尚未就绪，继续等待（登录断言失败不是致命错误）
        # 登录成功后再校验端口归属，避免被残留实例「冒充」就绪
        holder = find_app_pid()
        if holder is not None and holder != _APP_PROC.pid:
            raise RuntimeError(f"8899 监听进程 {holder} 与本次启动的 {_APP_PROC.pid} 不一致（存在残留实例）")
        return True
    raise RuntimeError("同步服务启动超时（180s）")


def _direct_session():
    """绕过本机 HTTP_PROXY 直连 127.0.0.1。

    本机存在 HTTP_PROXY=http://127.0.0.1:3195 且未配 no_proxy，
    requests 默认会把 127.0.0.1:8899 也发给代理，应用未就绪时返回
    502 upstream connect failed（os error 10061），把「应用还在启动」误判成登录失败。
    trust_env=False 可彻底禁用环境代理。
    """
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
    """服务存活探针：管控接口能否响应。

    注意：dockerlab profile 开启了鉴权，/sync/status 在「未携带凭证」时返回 401，
    但 401 同样是『服务在响应』的证明。因此只要能拿到任意 HTTP 响应即视为存活；
    只有连接被拒绝 / 超时（服务进程不在）才是真正的宕机。
    """
    try:
        _direct_session().get(f"{LAB_BASE_URL}/sync/status", timeout=10)
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def session():
    """模块级：确保实验室与同步服务就绪，返回已登录会话。
    关键：app 由本测试以子进程方式全权托管（而非依赖外部手动启动的实例），
    保证在整个测试运行期内存活，避免被外层 shell 回收导致误判“服务崩溃”。
    """
    # 1) 确保 docker 容器运行
    try:
        docker(["inspect", "-f", "{{.State.Running}}", SRC_CTR])
    except Exception:
        docker(["compose", "-f", r"D:\WorkSpace\flink-cdc-sync\tests\docker-lab\docker-compose.yml", "up", "-d"])
    docker(["start", SRC_CTR])
    docker(["start", TGT_CTR])
    wait_mysql(SRC)
    wait_mysql(TGT)
    # 2) 杀掉任何残留 app 实例，统一由本测试启动托管
    stop_app()
    start_app()
    s = _login()
    yield s
    # teardown：停止托管的 app，释放 8899 端口
    try:
        stop_app()
    except Exception:
        pass


def marker():
    return "FI_" + uuid.uuid4().hex[:14]


# ----------------------------- 测试用例 -----------------------------

def test_00_baseline_insert_update_delete(session):
    """基线：源端增/改/删均最终一致到目标端。"""
    m = marker()
    # 插入
    db_exec(SRC, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,1.00,1,'base')",
            "geo_source", (m,))
    wait_for(lambda: db_count(TGT, "geo_source", "t_order", f"order_no='{m}'") == 1, 40, "基线插入同步")
    # 更新
    db_exec(SRC, "UPDATE t_order SET amount=9.99 WHERE order_no=%s", "geo_source", (m,))
    wait_for(lambda: abs(_tgt_amount(m) - 9.99) < 0.01, 40, "基线更新同步")
    # 删除
    db_exec(SRC, "DELETE FROM t_order WHERE order_no=%s", "geo_source", (m,))
    wait_for(lambda: db_count(TGT, "geo_source", "t_order", f"order_no='{m}'") == 0, 40, "基线删除同步")
    # 清理（源端已删，目标端若残留则清掉）
    db_exec(TGT, "DELETE FROM t_order WHERE order_no=%s", "geo_source", (m,))


def _tgt_amount(m):
    conn = db_conn(TGT, "geo_source")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT amount FROM t_order WHERE order_no=%s", (m,))
            row = cur.fetchone()
            return float(row[0]) if row else float("nan")
    finally:
        conn.close()


def test_01_source_db_power_off_and_recover(session):
    """异常场景①：源库断电/断网 —— 已同步数据不丢，恢复后新变更最终一致。"""
    m1 = marker()
    db_exec(SRC, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,1.00,1,'src-down')",
            "geo_source", (m1,))
    wait_for(lambda: db_count(TGT, "geo_source", "t_order", f"order_no='{m1}'") == 1, 40, "源宕机前同步 m1")

    # 模拟源库断电
    docker(["stop", SRC_CTR])
    try:
        # 服务不应崩溃，管控接口仍可响应
        assert app_alive(), "源库宕机后同步服务应仍然存活可响应"
        # 源库不可达，目标端已同步数据必须保持完整（不应被误删/损坏）
        time.sleep(15)
        assert db_count(TGT, "geo_source", "t_order", f"order_no='{m1}'") == 1, "源宕机期间目标端已同步数据应保持不变"
    finally:
        docker(["start", SRC_CTR])
    wait_mysql(SRC)

    # 恢复后：新变更应最终一致
    m2 = marker()
    db_exec(SRC, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,1.00,1,'src-recover')",
            "geo_source", (m2,))
    wait_for(lambda: db_count(TGT, "geo_source", "t_order", f"order_no='{m2}'") == 1, 60, "源库恢复后 m2 同步")
    # 清理
    db_exec(SRC, "DELETE FROM t_order WHERE order_no IN (%s,%s)", "geo_source", (m1, m2))
    db_exec(TGT, "DELETE FROM t_order WHERE order_no IN (%s,%s)", "geo_source", (m1, m2))


def test_02_target_db_power_off_and_recover(session):
    """异常场景②：目标库断电/断网 —— 服务不崩，恢复后源端累积变更最终补齐（无永久丢失）。"""
    m1 = marker()
    db_exec(SRC, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,1.00,1,'tgt-down')",
            "geo_source", (m1,))
    wait_for(lambda: db_count(TGT, "geo_source", "t_order", f"order_no='{m1}'") == 1, 40, "目标宕机前同步 m1")

    # 模拟目标库断电
    docker(["stop", TGT_CTR])
    try:
        assert app_alive(), "目标库宕机后同步服务应仍然存活可响应"
        # 源端继续产生变更（此时无法写入目标端，应被缓冲/重试/全量补偿，而非崩溃）
        m2 = marker()
        db_exec(SRC, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,1.00,1,'tgt-recover')",
                "geo_source", (m2,))
        time.sleep(20)
    finally:
        docker(["start", TGT_CTR])
    wait_mysql(TGT)

    # 恢复后：m2 必须最终落到目标端（死信重试 + 全量重扫补偿，验证不永久丢失）
    wait_for(lambda: db_count(TGT, "geo_source", "t_order", f"order_no='{m2}'") == 1, 120,
             "目标库恢复后 m2 最终补齐")
    # 清理
    db_exec(SRC, "DELETE FROM t_order WHERE order_no IN (%s,%s)", "geo_source", (m1, m2))
    db_exec(TGT, "DELETE FROM t_order WHERE order_no IN (%s,%s)", "geo_source", (m1, m2))


def test_03_sync_service_restart(session):
    """异常场景③：同步服务进程被杀（主机断电重启） —— 已同步数据不丢，重启后恢复同步。"""
    m1 = marker()
    db_exec(SRC, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,1.00,1,'svc-down')",
            "geo_source", (m1,))
    wait_for(lambda: db_count(TGT, "geo_source", "t_order", f"order_no='{m1}'") == 1, 40, "服务被杀前同步 m1")

    # 杀掉同步服务进程（模拟断电/崩溃）
    assert stop_app(), "应成功停止同步服务进程"
    assert not app_alive(), "停止后服务应不可达"

    # 重启服务（内存水位丢失→全量重扫，依赖 upsert 幂等）
    start_app()
    assert app_alive(), "重启后服务应恢复可响应"

    # 已同步数据在目标端必须保持（不丢、不重复）
    assert db_count(TGT, "geo_source", "t_order", f"order_no='{m1}'") == 1, "重启后已同步数据应保留，不丢失"

    # 重启后新变更应继续同步
    m2 = marker()
    db_exec(SRC, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,1.00,1,'svc-recover')",
            "geo_source", (m2,))
    wait_for(lambda: db_count(TGT, "geo_source", "t_order", f"order_no='{m2}'") == 1, 60, "服务重启后 m2 同步")
    # 断言 m1 仍为 1（无重复，upsert 幂等）
    assert db_count(TGT, "geo_source", "t_order", f"order_no='{m1}'") == 1, "重启全量重扫后 m1 不应重复"
    # 清理
    db_exec(SRC, "DELETE FROM t_order WHERE order_no IN (%s,%s)", "geo_source", (m1, m2))
    db_exec(TGT, "DELETE FROM t_order WHERE order_no IN (%s,%s)", "geo_source", (m1, m2))


def test_04_multi_cycle_restart_stability(session):
    """稳定性：连续多次『杀进程→重启』循环，每次重启后数据均不重复、不丢失。"""
    m = marker()
    db_exec(SRC, "INSERT INTO t_customer(name,phone,balance) VALUES(%s,'13900000000',100.00)",
            "biz_source", (m,))
    wait_for(lambda: db_count(TGT, "biz_source", "t_customer", f"name='{m}'") == 1, 40, "初始化同步 m")

    for cycle in range(3):
        stop_app()
        start_app()
        # 每次重启后该记录应恰好 1 行（全量重扫幂等）
        wait_for(lambda: db_count(TGT, "biz_source", "t_customer", f"name='{m}'") == 1,
                 40, f"第{cycle+1}次重启后 m 恰好 1 行")
    # 清理
    db_exec(SRC, "DELETE FROM t_customer WHERE name=%s", "biz_source", (m,))
    db_exec(TGT, "DELETE FROM t_customer WHERE name=%s", "biz_source", (m,))
