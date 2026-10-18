# -*- coding: utf-8 -*-
"""DRPlatform DDL 同步 + DDL 之后数据同步一致性自动化测试（隔离实验室）。

环境：
  - 源主库  : 127.0.0.1:13306 (drp-src-mysql)     ← 在这里执行 DDL
  - 目标库  : 127.0.0.1:13307 (drp-tgt-mysql)     ← 验证 DDL 是否同步过来
  - 管控元库: 127.0.0.1:3306/drplatform
  - 同步服务: 本地 127.0.0.1:8899 (SPRING_PROFILES_ACTIVE=dockerlab)

覆盖场景：
  1. 新建表（CREATE TABLE）→ 目标端建表 → 表内数据正常同步
  2. 新增列（ADD COLUMN）→ 目标端结构变更 → **新增列的数据必须能同步**（易错点）
  3. 修改列（MODIFY COLUMN）→ 目标端结构变更 → 数据仍正常
  4. 删除列（DROP COLUMN）→ 目标端列消失 → 数据仍正常
  5. 删除表（DROP TABLE）→ 目标端表消失
  6. 一连串 DDL 之后，增/改/删数据同步仍然正常

同步引擎的 DDL 处理逻辑（SyncHostJob.detectAndApplyAlter）：
  检测到列签名变化 → DROP 目标表 → 按源端结构重建 → <b>重置该表水位</b> → 下一轮全量重扫回填数据。
因此 DDL 之后数据「应能自动回填」，本测试重点验证这一点确实成立。
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
APP_LOG = r"D:\WorkSpace\flink-cdc-sync\tests\docker-lab\app-lab-21.log"
JAVA = r"D:\software\jdk-21.0.11\bin\java.exe"
SRC_CTR = "drp-src-mysql"
TGT_CTR = "drp-tgt-mysql"
COMPOSE = r"D:\WorkSpace\flink-cdc-sync\tests\docker-lab\docker-compose.yml"
DB = "geo_source"
PREFIX = "DDL_"
PROBE_TABLE = "t_ddl_probe"
NEW_COL = "ddl_note"

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
    s.trust_env = False   # 绕过本机 HTTP_PROXY
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
            raise RuntimeError(f"8899 监听进程 {holder} 与本次启动的 {APP_PROC.pid} 不一致")
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
            # 无参数时不要传空元组，否则 LIKE 'xx%' 的 % 会被当成占位符
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


def table_exists(conn, db, table):
    rows = db_query(conn, "SELECT COUNT(1) AS c FROM information_schema.TABLES "
                          "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", None, (db, table))
    return int(rows[0]["c"]) > 0


def column_exists(conn, db, table, col):
    rows = db_query(conn, "SELECT COUNT(1) AS c FROM information_schema.COLUMNS "
                          "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s", None, (db, table, col))
    return int(rows[0]["c"]) > 0


def tgt_cell(marker, col):
    """读取目标端某行的指定列值。"""
    rows = db_query(TGT, f"SELECT `{col}` AS v FROM `{DB}`.`t_order` WHERE order_no=%s", None, (marker,))
    return rows[0]["v"] if rows else None


def marker(tag):
    return f"{PREFIX}{tag}_" + uuid.uuid4().hex[:10]


# ----------------------------- 模块准备 -----------------------------

@pytest.fixture(scope="module")
def session():
    try:
        docker(["inspect", "-f", "{{.State.Running}}", SRC_CTR])
    except Exception:
        docker(["compose", "-f", COMPOSE, "up", "-d"])
    docker(["start", SRC_CTR])
    docker(["start", TGT_CTR])
    wait_mysql(SRC)
    wait_mysql(TGT)
    # 清场：保证从干净状态开始
    for c in (SRC, TGT):
        db_exec(c, f"DELETE FROM `{DB}`.`t_order` WHERE order_no LIKE '{PREFIX}%'", DB)
        try:
            db_exec(c, f"DROP TABLE IF EXISTS `{DB}`.`{PROBE_TABLE}`")
        except Exception:
            pass
        try:
            db_exec(c, f"ALTER TABLE `{DB}`.`t_order` DROP COLUMN IF EXISTS `{NEW_COL}`")
        except Exception:
            pass
    stop_app()
    start_app()
    s = _login()
    yield s
    try:
        stop_app()
    except Exception:
        pass


# ----------------------------- 测试 -----------------------------

def test_ddl_01_create_table_and_data(session):
    """DDL①：源端新建表 → 目标端自动建表 → 新表内数据正常同步。"""
    assert app_alive(), "同步服务应存活"
    m1, m2 = marker("T1A"), marker("T1B")
    db_exec(SRC, f"CREATE TABLE IF NOT EXISTS `{DB}`.`{PROBE_TABLE}` ("
                 "id BIGINT NOT NULL AUTO_INCREMENT, "
                 "tag VARCHAR(64) NOT NULL, "
                 "score INT DEFAULT NULL, "
                 "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                 "PRIMARY KEY(id), UNIQUE KEY uk_tag(tag)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", DB)

    # 目标端应自动建表
    wait_for(lambda: table_exists(TGT, DB, PROBE_TABLE), 90, "目标端自动建表")
    print(f"[ddl] CREATE TABLE {PROBE_TABLE} 已同步到目标端")

    # 新表数据同步
    db_exec(SRC, f"INSERT INTO `{PROBE_TABLE}`(tag,score) VALUES(%s,%s)", DB, (m1, 10))
    db_exec(SRC, f"INSERT INTO `{PROBE_TABLE}`(tag,score) VALUES(%s,%s)", DB, (m2, 20))
    wait_for(lambda: db_query(TGT, "SELECT COUNT(1) AS c FROM t_ddl_probe", DB)[0]["c"] == 2,
             90, "新建表内数据同步")
    rows = db_query(TGT, "SELECT tag,score FROM t_ddl_probe ORDER BY tag", DB)
    assert len(rows) == 2, f"目标端数据行数不符: {rows}"
    scores = {r["tag"]: r["score"] for r in rows}
    assert scores.get(m1) == 10 and scores.get(m2) == 20, f"数据内容不符: {scores}"
    print(f"[ddl] 新建表内数据同步正确: {scores}")


def test_ddl_02_add_column_and_data(session):
    """DDL②：源端新增列 → 目标端结构同步 → **新增列的数据必须能同步**（本次核心易错点）。"""
    assert app_alive(), "同步服务应存活"
    db_exec(SRC, f"ALTER TABLE `{DB}`.`t_order` ADD COLUMN `{NEW_COL}` VARCHAR(64) NULL", DB)

    # 目标端应出现该列
    wait_for(lambda: column_exists(TGT, DB, "t_order", NEW_COL), 90, "目标端新增列")
    print(f"[ddl] ADD COLUMN {NEW_COL} 已同步到目标端")

    # DDL 之后：写入使用新列的数据，并同步一条已存在行的更新
    m = marker("T2")
    val_new = "v-" + uuid.uuid4().hex[:6]
    db_exec(SRC, f"INSERT INTO `{DB}`.`t_order`(order_no,user_id,amount,status,remark,`{NEW_COL}`) "
                 "VALUES(%s,1,3.14,1,'ddl-add',%s)", DB, (m, val_new))

    # 核心断言：新列的值必须落到目标端（若 Sink 结构缓存过期，此处会为 NULL）
    wait_for(lambda: tgt_cell(m, NEW_COL) == val_new, 120, f"新增列数据同步（{NEW_COL}={val_new}）")
    print(f"[ddl] 新增列数据同步正确: {NEW_COL}={tgt_cell(m, NEW_COL)}")

    # DDL 会导致目标表被 DROP 并按新结构重建 → 该表此前已同步的数据必须被全量重扫回填。
    # 这里用「目标端行数 == 源端行数」作为回填完整的强断言。
    src_total = db_query(SRC, "SELECT COUNT(1) AS c FROM t_order", DB)[0]["c"]
    wait_for(lambda: db_query(TGT, "SELECT COUNT(1) AS c FROM t_order", DB)[0]["c"] == src_total,
             120, "DDL 重建后历史数据全量回填（目标行数==源行数）")
    print(f"[ddl] 重建后全量回填正确: 源={src_total} 行, 目标={db_query(TGT, 'SELECT COUNT(1) AS c FROM t_order', DB)[0]['c']} 行")


def test_ddl_03_modify_column(session):
    """DDL③：源端修改列定义 → 目标端结构同步 → 数据仍正常。"""
    assert app_alive(), "同步服务应存活"
    db_exec(SRC, f"ALTER TABLE `{DB}`.`t_order` MODIFY COLUMN `{NEW_COL}` VARCHAR(128) NULL", DB)
    # 目标端列长度应随之变化
    wait_for(lambda: int(db_query(TGT, "SELECT CHARACTER_MAXIMUM_LENGTH AS len FROM information_schema.COLUMNS "
                                     "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='t_order' AND COLUMN_NAME=%s",
                                     None, (DB, NEW_COL))[0]["len"]) == 128,
             90, "目标端列定义同步(VARCHAR(128))")
    print("[ddl] MODIFY COLUMN VARCHAR(128) 已同步到目标端")
    # 数据同步仍正常
    m = marker("T3")
    val = "m-" + uuid.uuid4().hex[:6]
    db_exec(SRC, f"INSERT INTO `{DB}`.`t_order`(order_no,user_id,amount,status,remark,`{NEW_COL}`) "
                 "VALUES(%s,1,1.11,1,'ddl-mod',%s)", DB, (m, val))
    wait_for(lambda: tgt_cell(m, NEW_COL) == val, 90, "改列后数据同步")


def test_ddl_04_drop_column(session):
    """DDL④：源端删除列 → 目标端列消失 → 数据仍正常。"""
    assert app_alive(), "同步服务应存活"
    db_exec(SRC, f"ALTER TABLE `{DB}`.`t_order` DROP COLUMN `{NEW_COL}`", DB)
    wait_for(lambda: not column_exists(TGT, DB, "t_order", NEW_COL), 90, "目标端删除列")
    print(f"[ddl] DROP COLUMN {NEW_COL} 已同步到目标端")
    # 删除列后数据同步仍正常
    m = marker("T4")
    db_exec(SRC, f"INSERT INTO `{DB}`.`t_order`(order_no,user_id,amount,status,remark) "
                 "VALUES(%s,1,2.22,1,'ddl-drop')", DB, (m,))
    wait_for(lambda: db_query(TGT, "SELECT COUNT(1) AS c FROM t_order WHERE order_no=%s", DB, (m,))[0]["c"] == 1,
             90, "删列后数据同步")


def test_ddl_05_drop_table(session):
    """DDL⑤：源端删除表 → 目标端表同步删除。"""
    assert app_alive(), "同步服务应存活"
    db_exec(SRC, f"DROP TABLE IF EXISTS `{DB}`.`{PROBE_TABLE}`", DB)
    wait_for(lambda: not table_exists(TGT, DB, PROBE_TABLE), 120, "目标端删除表")
    print(f"[ddl] DROP TABLE {PROBE_TABLE} 已同步到目标端")


def test_ddl_06_data_sync_after_all_ddl(session):
    """DDL⑥：经历一连串 DDL 之后，增/改/删数据同步仍然完全正常。"""
    assert app_alive(), "同步服务应存活"
    # 增
    m = marker("T6")
    db_exec(SRC, f"INSERT INTO `{DB}`.`t_order`(order_no,user_id,amount,status,remark) "
                 "VALUES(%s,1,5.55,1,'after-ddl')", DB, (m,))
    wait_for(lambda: db_query(TGT, "SELECT COUNT(1) AS c FROM t_order WHERE order_no=%s", DB, (m,))[0]["c"] == 1,
             90, "DDL 后新增同步")
    # 改
    db_exec(SRC, f"UPDATE `{DB}`.`t_order` SET amount=6.66, remark='after-ddl-upd' WHERE order_no=%s", DB, (m,))
    wait_for(lambda: float(db_query(TGT, "SELECT amount AS a FROM t_order WHERE order_no=%s", DB, (m,))[0]["a"]) == 6.66,
             90, "DDL 后更新同步")
    # 删
    db_exec(SRC, f"DELETE FROM `{DB}`.`t_order` WHERE order_no=%s", DB, (m,))
    wait_for(lambda: db_query(TGT, "SELECT COUNT(1) AS c FROM t_order WHERE order_no=%s", DB, (m,))[0]["c"] == 0,
             120, "DDL 后删除同步")
    print("[ddl] DDL 之后 增/改/删 三种数据同步均正常")


if __name__ == "__main__":
    pytest.main(["-v", __file__])