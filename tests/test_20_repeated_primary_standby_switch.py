# -*- coding: utf-8 -*-
"""DRPlatform 主备「反复切换」数据一致性自动化测试（隔离实验室）。

本文件在 tests/docker-lab 搭建的隔离环境上运行：
  - 源主库  : 127.0.0.1:13306 (drp-src-mysql)
  - 源备库  : 127.0.0.1:13308 (drp-src-standby-mysql)
  - 目标库  : 127.0.0.1:13307 (drp-tgt-mysql)
  - 管控元库: 127.0.0.1:3306/drplatform
  - 同步服务: 本地 127.0.0.1:8899 (SPRING_PROFILES_ACTIVE=dockerlab，映射已配置 sourceStandbyHost=13308)

覆盖场景：**主库 ⇄ 备库 连续往返切换 ≥5 次**，每轮切换后写入新数据并校验一致性。

模拟真实主备语义：
  - 新写入只发生在「当前源」；
  - 之后把该批行按<b>原主键原 update_time</b>镜像到另一端（等价于 MySQL 主从复制逐行复制），
    从而让两个源保持数据等价；自增位点则各自独立（这正是反复切换时最容易出问题的地方）。

核心断言（每轮 + 最终）：
  1. 切换接口返回 switched=true，且 sourceHost/sourcePort 确实在 13306 / 13308 之间交替；
  2. 切换后同步服务仍存活并继续工作；
  3. 灾备目标中「全部已写入业务行」一条不少、不重复（按 order_no 集合比对）；
  4. 灾备目标中业务唯一键 order_no 无重复行（防主键复用导致的覆盖/串行）；
  5. 切换次数 ≥ 5。
"""
import os
import time
import uuid
import subprocess
import pytest
import requests
import pymysql

LAB_BASE_URL = os.environ.get("DRPLATFORM_LAB_URL", "http://127.0.0.1:8899")
SRC = dict(host="127.0.0.1", port=13306, user="root", password="123456")          # 主
SRC_STANDBY = dict(host="127.0.0.1", port=13308, user="root", password="123456")  # 备
TGT = dict(host="127.0.0.1", port=13307, user="root", password="123456")
CONTROL = dict(host="127.0.0.1", port=3306, user="root", password="123456")
ADMIN_USER = os.environ.get("DRPLATFORM_ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("DRPLATFORM_ADMIN_PASS", "admin123")
APP_JAR = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync\target\remote-data-sync.jar"
APP_CWD = r"D:\WorkSpace\flink-cdc-sync\remote-data-sync"
APP_LOG = r"D:\WorkSpace\flink-cdc-sync\tests\docker-lab\app-lab-20.log"
JAVA = r"D:\software\jdk-21.0.11\bin\java.exe"
SRC_CTR = "drp-src-mysql"
SRC_STANDBY_CTR = "drp-src-standby-mysql"
TGT_CTR = "drp-tgt-mysql"
COMPOSE = r"D:\WorkSpace\flink-cdc-sync\tests\docker-lab\docker-compose.yml"
# instanceKey 形如 127.0.0.1->127.0.0.1（主备同主机不同端口，切换不改变该 key）
INSTANCE_KEY = f"{SRC['host']}->{TGT['host']}"

# 本轮测试的写入痕迹前缀（便于一次性清理）
PREFIX = "SW_"
# 切换次数：至少 5 次（按用户要求）
SWITCH_ROUNDS = int(os.environ.get("DRPLATFORM_SWITCH_ROUNDS", "5"))
# 每轮写入的行数（不宜过多，避免测试过长；足以暴露主键/覆盖类问题）
ROWS_PER_ROUND = 5

APP_PROC = None
# 记录本轮测试写入的全部 order_no，用于最终一次性核对
_ALL_MARKERS = []


# ----------------------------- 基础设施辅助 -----------------------------

def _run_native(cmd, timeout=30):
    """调用 Windows 原生命令：绝不用 text=True（GBK/UTF-8 解码陷阱会导致 stdout 静默为空）。"""
    r = subprocess.run(cmd, capture_output=True, timeout=timeout)
    return r.returncode, r.stdout.decode("utf-8", "replace"), r.stderr.decode("utf-8", "replace")


def find_app_pid():
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
    """绕过本机 HTTP_PROXY 直连 127.0.0.1（本机代理未配 no_proxy，会把 8899 也发给代理）。"""
    s = requests.Session()
    s.trust_env = False
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
            continue
        holder = find_app_pid()
        if holder is not None and holder != APP_PROC.pid:
            raise RuntimeError(f"8899 监听进程 {holder} 与本次启动的 {APP_PROC.pid} 不一致（存在残留实例）")
        return True
    raise RuntimeError("同步服务启动超时（180s）")


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
                                   connect_timeout=timeout, read_timeout=30, write_timeout=30)
        except Exception as e:
            last = e
            time.sleep(2)
    raise last


def db_exec(c, sql, db=None, args=None):
    conn = db_conn(c, db)
    try:
        with conn.cursor() as cur:
            # 注意：pymysql 仅在 args 为真值时才做 mogrify。若无参数也传空元组，
            # SQL 里的 LIKE 'xx%' 会被当成占位符而抛 "not enough arguments for format string"。
            if args:
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
    raise AssertionError(f"MySQL {c['host']}:{c['port']} 在 {timeout}s 内不可达")


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


def count_target_markers(markers):
    """统计灾备目标中命中本测试写入痕迹的行数。"""
    if not markers:
        return 0
    conn = db_conn(TGT, "geo_source")
    try:
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(markers))
            cur.execute(f"SELECT COUNT(1) FROM t_order WHERE order_no IN ({ph})", tuple(markers))
            return cur.fetchone()[0]
    finally:
        conn.close()


def count_target_dup_groups():
    """返回灾备目标中 order_no 出现多行（重复）的分组数——应为 0。"""
    conn = db_conn(TGT, "geo_source")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM (SELECT order_no FROM t_order "
                        f"WHERE order_no LIKE '{PREFIX}%' GROUP BY order_no HAVING COUNT(*) > 1) x")
            return cur.fetchone()[0]
    finally:
        conn.close()


def marker(tag):
    return f"{PREFIX}{tag}_" + uuid.uuid4().hex[:12]


def mirror_rows(src_conn, dst_conn, order_nos):
    """把源端这批行按「原主键 + 原 update_time」复制到另一端，模拟 MySQL 主从复制。"""
    if not order_nos:
        return
    src_rows = db_query(src_conn, "SELECT id,order_no,user_id,amount,status,remark,update_time "
                                  "FROM t_order WHERE order_no IN (" +
                              ",".join(["%s"] * len(order_nos)) + ")", "geo_source", tuple(order_nos))
    assert len(src_rows) == len(order_nos), \
        f"镜像源端行数不符: 取到 {len(src_rows)}，期望 {len(order_nos)}"
    for r in src_rows:
        db_exec(dst_conn,
                "INSERT INTO t_order(id,order_no,user_id,amount,status,remark,update_time) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s)", "geo_source",
                (r["id"], r["order_no"], r["user_id"], r["amount"],
                 r["status"], r["remark"], r["update_time"]))


def current_source_port(session):
    """查询当前同步源端口（13306=主 / 13308=备）。"""
    r = session.get(f"{LAB_BASE_URL}/sync/mappings", timeout=20)
    r.raise_for_status()
    j = r.json()
    data = j.get("data") or []
    for m in data:
        if m.get("instanceKey") == INSTANCE_KEY or (m.get("sourceHost") == SRC["host"]):
            port = m.get("sourcePort")
            if port:
                return int(port)
    raise AssertionError(f"未能在 /sync/mappings 中定位同步源: {j}")


def do_switch(session):
    """执行一次主备切换，返回切换后响应体。"""
    r = session.post(f"{LAB_BASE_URL}/sync/mapping/switchSource",
                     json={"instanceKey": INSTANCE_KEY}, timeout=60)
    assert r.status_code == 200, f"切换接口失败 {r.status_code}: {r.text[:300]}"
    body = r.json()
    assert body.get("success") is True, f"切换未成功: {body}"
    assert body.get("data", {}).get("switched") is True, f"switched != true: {body}"
    return body.get("data", {})


# ----------------------------- 模块准备 -----------------------------

@pytest.fixture(scope="module")
def session():
    try:
        docker(["inspect", "-f", "{{.State.Running}}", SRC_CTR])
    except Exception:
        docker(["compose", "-f", COMPOSE, "up", "-d"])
    docker(["start", SRC_CTR])
    docker(["start", SRC_STANDBY_CTR])
    docker(["start", TGT_CTR])
    wait_mysql(SRC)
    wait_mysql(SRC_STANDBY)
    wait_mysql(TGT)
    # 清掉历史遗留痕迹，避免影响计数
    for c in (SRC, SRC_STANDBY, TGT):
        db_exec(c, f"DELETE FROM t_order WHERE order_no LIKE '{PREFIX}%'", "geo_source")
    stop_app()
    start_app()
    s = _login()
    yield s
    try:
        stop_app()
    except Exception:
        pass
    # 重启主库（若测试中被停过），便于实验室重复运行
    try:
        docker(["start", SRC_CTR])
        wait_mysql(SRC)
    except Exception:
        pass
    # 清理本轮写入
    for c in (SRC, SRC_STANDBY, TGT):
        try:
            db_exec(c, f"DELETE FROM t_order WHERE order_no LIKE '{PREFIX}%'", "geo_source")
        except Exception:
            pass


# ----------------------------- 测试 -----------------------------

def test_repeated_primary_standby_switch(session):
    """主备 ⇄ 备主 反复切换 ≥5 次，每轮校验灾备目标数据一致性（不丢 / 不重 / 不被覆盖）。"""
    # 起点：确认当前源为主库（13306）
    port0 = current_source_port(session)
    assert port0 == SRC["port"], f"初始源应为 {SRC['port']}，实际 {port0}"

    # 先写入一批基线并镜像到备库，让两端等价起步
    base = [marker("BASE") for _ in range(ROWS_PER_ROUND)]
    _ALL_MARKERS.extend(base)
    for m in base:
        db_exec(SRC, "INSERT INTO t_order(order_no,user_id,amount,status,remark) VALUES(%s,1,1.00,1,'sw-base')",
                "geo_source", (m,))
    wait_for(lambda: count_target_markers(base) == len(base), 90, "基线同步到灾备目标")
    mirror_rows(SRC, SRC_STANDBY, base)
    wait_for(lambda: count_target_markers(base) == len(base), 60, "基线一致性确认")

    total_switches = 0
    expect_port = SRC_STANDBY["port"]  # 第一次切到备库(13308)，之后来回交替

    for rnd in range(1, SWITCH_ROUNDS + 1):
        # --- 本轮：向「当前源」写入新数据，等待同步到灾备目标 ---
        cur_port = current_source_port(session)
        cur_conn = SRC if cur_port == SRC["port"] else SRC_STANDBY
        batch = [marker(f"R{rnd}") for _ in range(ROWS_PER_ROUND)]
        _ALL_MARKERS.extend(batch)
        for m in batch:
            db_exec(cur_conn, "INSERT INTO t_order(order_no,user_id,amount,status,remark) "
                              "VALUES(%s,1,2.00,1,%s)", "geo_source", (m, f"sw-round{rnd}"))
        # 新数据必须到达灾备目标
        wait_for(lambda: count_target_markers(batch) == len(batch), 90, f"第{rnd}轮新数据同步")

        # 关键：本轮<b>刻意不镜像</b>到另一端，制造「两端数据分歧」。
        # 这模拟真实主备的异步复制延迟 —— 备库落后于灾备目标已同步的数据。
        # 正是在这种分歧下，切换时新源自增位点若低于灾备目标已存在的主键，
        # 新写入会复用旧 id，与目标端「不同业务行同主键」冲突并被 upsert 覆盖 → 数据丢失。
        # 因此本测试同时验证：① 主键对齐防护 ② 换源期删除静默期 ③ 双向反复切换。

        # --- 执行一次切换 ---
        data = do_switch(session)
        total_switches += 1

        # 切换后端口应与预期交替
        got_port = int(data.get("sourcePort"))
        assert got_port == expect_port, \
            f"第{rnd}轮切换后源端口应为 {expect_port}，实际 {got_port}（响应: {data}）"
        # 校验接口声明的 standby 已换为上一个源（体现可反复切换）
        assert int(data.get("standbyPort")) != got_port, \
            f"第{rnd}轮切换后 standby 端口应与当前源不同: {data}"

        # 切换后服务仍存活
        assert app_alive(), f"第{rnd}轮切换后同步服务不应崩溃"

        # 切换后历史数据仍完整（不丢、不重复）
        assert count_target_markers(_ALL_MARKERS) == len(_ALL_MARKERS), \
            f"第{rnd}轮切换后灾备目标数据不完整: 期望{len(_ALL_MARKERS)}，" \
            f"实际{count_target_markers(_ALL_MARKERS)}"
        assert count_target_dup_groups() == 0, \
            f"第{rnd}轮切换后灾备目标出现重复 order_no（主键复用/覆盖风险）"

        print(f"[switch#{total_switches}] round={rnd} -> new source port={got_port} "
              f"(standby={data.get('standbyPort')}), target rows OK={len(_ALL_MARKERS)}")

        # 下一轮期望端口来回交替
        expect_port = SRC["port"] if got_port == SRC_STANDBY["port"] else SRC_STANDBY["port"]

    # 切换次数满足要求（≥5）
    assert total_switches >= 5, f"切换次数不足: {total_switches} < 5"

    # 最终：一次切换后再校验全局一致
    do_switch(session)
    assert count_target_markers(_ALL_MARKERS) == len(_ALL_MARKERS), \
        f"最终灾备目标数据不完整: 期望{len(_ALL_MARKERS)}，实际{count_target_markers(_ALL_MARKERS)}"
    assert count_target_dup_groups() == 0, "最终灾备目标存在重复 order_no"
    print(f"[final] switches={total_switches + 1}, target markers OK={len(_ALL_MARKERS)}")


if __name__ == "__main__":
    pytest.main(["-v", __file__])