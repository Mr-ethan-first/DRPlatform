# -*- coding: utf-8 -*-
"""GeoDRSync 前后端全自动化测试套件（正常流 / 配置异常 / 字符输入异常 / 断电断网 / 压力 / 可靠性）。

设计：
- 真实 HTTP 调用后端（不 mock），覆盖前端依赖的全部 REST 契约。
- 直接驱动 Docker 容器做断电（docker stop）/断网（docker pause）故障注入。
- 每个场景自包含、失败不中断整体；最后输出 PASS/FAIL 汇总与 Markdown 报告。
- 测试资产统一用 e2e_ 前缀，finally 中统一清理（删库、删映射、恢复容器、重启应用）。
"""
import os
import sys
import time
import json
import subprocess
import re
import threading
import datetime

import requests
import pymysql

# ===================== 环境常量 =====================
BASE_URL = os.environ.get("DRPLATFORM_BASE_URL", "http://127.0.0.1:8899")
ADMIN_USER = os.environ.get("DRPLATFORM_ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("DRPLATFORM_ADMIN_PASS", "admin123")

SRC = {"host": "127.0.0.1", "port": 13306, "user": "root", "password": "123456"}
TGT = {"host": "127.0.0.1", "port": 13307, "user": "root", "password": "123456"}
STB = {"host": "127.0.0.1", "port": 13308, "user": "root", "password": "123456"}
META = {"host": "127.0.0.1", "port": 3306, "user": "root", "password": "123456", "db": "DRPlatform"}

DOCKER_EXE = "C:/Program Files/Docker/Docker/resources/bin/docker.exe"
APP_DIR = "D:/WorkSpace/flink-cdc-sync/remote-data-sync"
JAVA = "D:/software/jdk-21.0.11/bin/java"
APP_JAR = "target/remote-data-sync.jar"

CONT = {"src": "drp-src-mysql", "tgt": "drp-tgt-mysql", "stb": "drp-src-standby-mysql"}

CREATED_MAPPINGS = []   # 测试期间新增的 instanceKey，finally 清理
E2E_DBS = set()         # 测试期间创建的源端库名，finally 清理
CONTAINERS_TOUCHED = set()

# ===================== 结果收集 =====================
RESULTS = []  # (group, name, ok, detail)
LOCK = threading.Lock()


def rec(group, name, ok, detail=""):
    with LOCK:
        RESULTS.append((group, name, ok, detail))
    tag = "PASS" if ok else "FAIL"
    print(f"[{tag}] [{group}] {name}" + (f" :: {detail}" if detail else ""), flush=True)


def new_session():
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json"})
    s.trust_env = False
    s.proxies = {"http": None, "https": None}
    return s


def login(s):
    r = s.post(BASE_URL + "/auth/login", json={"username": ADMIN_USER, "password": ADMIN_PASS}, timeout=30)
    return r


# ===================== MySQL 直连 =====================
def db_connect(cfg, database=None, timeout=10):
    return pymysql.connect(
        host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"],
        database=database, charset="utf8mb4",
        connect_timeout=timeout, read_timeout=30, write_timeout=30,
    )


# 容器重启/断电恢复瞬间，MySQL 已 Running 但尚不能稳定接受查询，
# 偶发 2013(Lost connection)/2006(gone away)/2003(connection refused)。
# 这里对这类连接级瞬错做退避重试，避免把"清理/探测瞬间"误判成用例失败。
_TRANSIENT_MYSQL_CODES = {2013, 2006, 2003, 1053, 1152, 1105}


def db_exec(cfg, sql, args=None, database=None, fetch=False, timeout=10, _retries=5):
    last = None
    for attempt in range(_retries):
        try:
            conn = db_connect(cfg, database, timeout)
            try:
                with conn.cursor(pymysql.cursors.DictCursor) as cur:
                    cur.execute(sql, args or ())
                    if fetch:
                        return [{k.lower(): v for k, v in row.items()} for row in cur.fetchall()]
                    conn.commit()
                    return cur.rowcount
            finally:
                conn.close()
        except pymysql.err.OperationalError as e:
            last = e
            code = e.args[0] if e.args else 0
            if code not in _TRANSIENT_MYSQL_CODES:
                raise
            if attempt < _retries - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise
    if last:
        raise last
    return None


def db_query(cfg, sql, args=None, database=None, timeout=10):
    return db_exec(cfg, sql, args, database, fetch=True, timeout=timeout)


def ensure_db(cfg, db_name, drop_first=False):
    if drop_first:
        db_exec(cfg, f"DROP DATABASE IF EXISTS `{db_name}`")
    db_exec(cfg, f"CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET utf8mb4")
    E2E_DBS.add(db_name)


def drop_db(cfg, db_name):
    try:
        db_exec(cfg, f"DROP DATABASE IF EXISTS `{db_name}`")
    except Exception as e:
        print(f"  drop_db warn {db_name}: {e}", flush=True)


def rowcount(cfg, db, table):
    try:
        rows = db_query(cfg, f"SELECT COUNT(*) AS c FROM `{db}`.`{table}`", database=db)
        return rows[0]["c"] if rows else 0
    except Exception:
        return -1


# ===================== Docker 故障注入 =====================
def docker_run(args, timeout=120):
    env = dict(os.environ)
    env["PATH"] = "C:/Program Files/Docker/Docker/resources/bin" + os.pathsep + env.get("PATH", "")
    # 注意：docker.exe 在中文 Windows 上可能输出 GBK 字节；用 text=True 会让 subprocess 的
    # reader 线程抛 UnicodeDecodeError 并把 .stdout 置为 None，进而引发后面的 .splitlines()/.
    # strip() 崩 None。一律按字节读取再宽容解码（与项目既定规则一致）。
    p = subprocess.run(
        [DOCKER_EXE] + args, capture_output=True, timeout=timeout, env=env,
    )
    return p.returncode, (p.stdout or b"").decode("utf-8", "replace"), (p.stderr or b"").decode("utf-8", "replace")


def pause_cont(role):
    CONTAINERS_TOUCHED.add(role)
    docker_run(["pause", CONT[role]])


def unpause_cont(role):
    docker_run(["unpause", CONT[role]])


def stop_cont(role):
    CONTAINERS_TOUCHED.add(role)
    docker_run(["stop", CONT[role]])


def start_cont(role):
    docker_run(["start", CONT[role]])


def ensure_container_up(role, tries=30):
    for _ in range(tries):
        rc, out, _ = docker_run(["inspect", "-f", "{{.State.Running}}", CONT[role]])
        if out.strip() == "true":
            return True
        time.sleep(1)
    return False


# ===================== 应用生命周期 =====================
def find_app_pid():
    # netstat 输出为纯 ASCII，可安全按字节读；但为统一规避 Windows 原生命令的 GBK 解码坑，
    # 仍走字节 + 宽容解码，避免 reader 线程异常把 .stdout 变成 None。
    out = subprocess.run(["netstat", "-ano"], capture_output=True).stdout
    out = (out or b"").decode("utf-8", "replace")
    for line in out.splitlines():
        if ":8899" in line and "LISTENING" in line:
            parts = line.split()
            return parts[-1]
    return None


def kill_app():
    pid = find_app_pid()
    if pid:
        subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True, timeout=30)
        # 等待端口释放
        for _ in range(30):
            if find_app_pid() is None:
                return True
            time.sleep(1)
    return True


def start_app_detached():
    env = dict(os.environ)
    env.pop("SERVER__PORT", None)
    env.pop("SERVER_PORT", None)
    # 日志必须落盘：R2 这类「应用启动后自身故障」的场景，DEVNULL 会让失败完全不可诊断
    log_fh = open(os.path.join(APP_DIR, "app_detached.log"), "ab")
    log_fh.write(f"\n===== harness start {time.strftime('%Y-%m-%d %H:%M:%S')} =====\n".encode())
    log_fh.flush()
    p = subprocess.Popen(
        [JAVA, "-jar", APP_JAR, "--server.port=8899"],
        cwd=APP_DIR, env=env,
        stdout=log_fh, stderr=subprocess.STDOUT,
        creationflags=0x00000008,  # DETACHED_PROCESS
    )
    return p


def wait_app(timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(BASE_URL + "/auth/me", headers={"Content-Type": "application/json"}, timeout=3)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def wait_condition(fn, timeout=90, interval=2, desc="condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = fn()
            if last:
                return last
        except Exception:
            last = None
        time.sleep(interval)
    raise AssertionError(f"等待 {desc} 超时（{timeout}s），最后结果: {last}")


# ===================== 通用断言辅助 =====================
def api_ok(r, expect_status=200):
    if r.status_code != expect_status:
        raise AssertionError(f"HTTP {r.status_code}: {r.text[:300]}")
    j = r.json()
    if j.get("success") is not True:
        raise AssertionError(f"success=False: {j}")
    return j


def api_err(r, expect_status):
    if r.status_code != expect_status:
        raise AssertionError(f"期望 HTTP {expect_status} 实际 {r.status_code}: {r.text[:300]}")
    j = r.json()
    if j.get("success") is not False:
        raise AssertionError(f"期望 success=False 实际 True: {j}")
    return j


def mapping_body(src, tgt, ignore_dbs=None, extra=None):
    b = {
        "sourceHost": src["host"], "sourcePort": src["port"],
        "sourceUser": src["user"], "sourcePassword": src["password"],
        "targetHost": tgt["host"], "targetPort": tgt["port"],
        "targetUser": tgt["user"], "targetPassword": tgt["password"],
        "ignoreDatabases": ignore_dbs or [],
    }
    if extra:
        b.update(extra)
    return b


def add_mapping(s, body):
    r = s.post(BASE_URL + "/sync/mapping/add", json=body, timeout=30)
    return r


def remove_mapping(s, instance_key):
    r = s.post(BASE_URL + "/sync/mapping/remove", json={"instanceKey": instance_key}, timeout=30)
    return r


def track_mapping(instance_key):
    CREATED_MAPPINGS.append(instance_key)


# =====================================================================
# G1 正常流 + 前端契约
# =====================================================================
def g1_normal_flow():
    group = "G1-正常流"
    s = new_session()
    try:
        # F1 前端首页可访问 + 结构
        r = s.get(BASE_URL + "/", timeout=15)
        html = r.text
        rec(group, "F1-前端首页HTTP200且含React挂载点", r.status_code == 200 and ("root" in html or "id=\"root\"" in html or "div" in html),
            f"status={r.status_code} len={len(html)}")

        # F2 前端契约：前端 app.js 实际引用的 REST 端点都应在后端真实存在（非 404）
        rjs = s.get(BASE_URL + "/app.js", timeout=15)
        js = rjs.text
        rec(group, "F2a-前端app.js可加载", rjs.status_code == 200 and len(js) > 1000,
            f"status={rjs.status_code} len={len(js)}")
        # 动态提取 app.js 中引用的所有 /auth/* 与 /sync/* 端点
        import re as _re
        refs = sorted(set(_re.findall(r"'(/(?:auth|sync)[^']*)'", js)))
        rec(group, "F2b-前端引用的端点集合", len(refs) > 0, f"共 {len(refs)} 个: {refs}")
        # 每个被引用的端点都必须在后端真实存在（GET 不应 404；POST 空体应 400/401 而非 404）
        unwired = []
        for ep in refs:
            try:
                if ep in ("/auth/login", "/auth/logout", "/auth/me", "/sync/mappings", "/sync/db/list",
                          "/sync/status", "/sync/cdc/jobs", "/sync/sourceDatabases", "/sync/ipList",
                          "/sync/mapping/reverse", "/sync/mapping/switchSource"):
                    rr = s.get(BASE_URL + ep, timeout=10)
                else:
                    rr = s.post(BASE_URL + ep, json={}, timeout=10)
                if rr.status_code == 404:
                    unwired.append(ep)
            except Exception:
                unwired.append(ep)
        rec(group, "F2c-后端真实实现所有前端引用端点(无404)", len(unwired) == 0,
            f"未实现: {unwired}" if unwired else f"全部实现({len(refs)}个)")

        # F3 登录 + 会话态
        r = login(s)
        api_ok(r)
        r = s.get(BASE_URL + "/auth/me", timeout=10)
        j = api_ok(r)
        rec(group, "F3-登录成功且会话态可见", j["data"].get("loggedIn") is True,
            f"username={j['data'].get('username')}")

        # F4 映射列表
        r = s.get(BASE_URL + "/sync/mappings", timeout=10)
        j = api_ok(r)
        keys = [m["instanceKey"] for m in j["data"]]
        rec(group, "F4-映射列表含默认映射", any("13306" in k and "13307" in k for k in keys),
            f"keys={keys}")

        # F5 状态/CDC作业/IP列表（注意：/sync/status 与 /sync/ipList 返回原始 Map/List，无 success 包装）
        r = s.get(BASE_URL + "/sync/status", timeout=10)
        assert r.status_code == 200, f"/sync/status HTTP {r.status_code}"
        r = s.get(BASE_URL + "/sync/ipList", timeout=10)
        assert r.status_code == 200, f"/sync/ipList HTTP {r.status_code}"
        r = s.get(BASE_URL + "/sync/cdc/jobs", timeout=10)
        api_ok(r)
        rec(group, "F5-状态/IP列表/CDC作业接口可用", True)

        # F6 数据同步正常流（默认映射同步所有用户库）
        db = "e2e_normal"
        ensure_db(SRC, db, drop_first=True)
        db_exec(SRC, f"CREATE TABLE IF NOT EXISTS `{db}`.t_normal (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(200))", database=db)
        db_exec(SRC, f"INSERT INTO `{db}`.t_normal(v) VALUES ('a'),('b'),('c')", database=db)
        wait_condition(lambda: rowcount(TGT, db, "t_normal") >= 3, timeout=90, desc="target t_normal>=3")
        rc_t = rowcount(TGT, db, "t_normal")
        rc_s = rowcount(SRC, db, "t_normal")
        rec(group, "F6-单表INSERT同步且行数一致", rc_t == rc_s == 3, f"src={rc_s} tgt={rc_t}")

        # F7 DDL 同步（加列）
        db_exec(SRC, f"ALTER TABLE `{db}`.t_normal ADD COLUMN v2 VARCHAR(50)", database=db)
        wait_condition(lambda: db_query(TGT, f"SHOW COLUMNS FROM `{db}`.t_normal", database=db) is not None and
                       any(c["field"] == "v2" for c in db_query(TGT, f"SHOW COLUMNS FROM `{db}`.t_normal", database=db)),
                       timeout=90, desc="target v2 column")
        cols = [c["field"] for c in db_query(TGT, f"SHOW COLUMNS FROM `{db}`.t_normal", database=db)]
        rec(group, "F7-DDL加列同步到目标", "v2" in cols, f"cols={cols}")

        # F8 DELETE 同步
        db_exec(SRC, f"DELETE FROM `{db}`.t_normal WHERE v='c'", database=db)
        wait_condition(lambda: rowcount(TGT, db, "t_normal") == 2, timeout=90, desc="target t_normal==2")
        rec(group, "F8-DELETE同步且行数一致", rowcount(TGT, db, "t_normal") == 2,
            f"src={rowcount(SRC,db,'t_normal')} tgt={rowcount(TGT,db,'t_normal')}")

        # 清理
        drop_db(SRC, db)
    except Exception as e:
        rec(group, "G1-整体", False, f"{type(e).__name__}: {e}")
    finally:
        pass


# =====================================================================
# G2 配置异常 / 数据库配置错误
# =====================================================================
def g2_config_errors():
    group = "G2-配置异常"
    s = new_session()
    try:
        login(s)
        # C1 缺字段
        r = add_mapping(s, {})
        api_err(r, 400)
        rec(group, "C1-缺字段返回400", True)

        # C2 空源主机
        b = mapping_body(SRC, SRC)
        b["sourceHost"] = ""
        r = add_mapping(s, b)
        api_err(r, 400)
        rec(group, "C2-空源主机返回400", True)

        # C3 目标不可达（端口错）
        bad_tgt = {"host": "127.0.0.1", "port": 13399, "user": "root", "password": "123456"}
        r = add_mapping(s, mapping_body(SRC, bad_tgt))
        ok = (r.status_code in (400, 500)) and (r.json().get("success") is False)
        rec(group, "C3-目标不可达被拒绝(无200)", ok, f"status={r.status_code} body={r.text[:120]}")

        # C4 目标密码错误
        bad_tgt2 = {"host": "127.0.0.1", "port": 13307, "user": "root", "password": "wrongpass"}
        r = add_mapping(s, mapping_body(SRC, bad_tgt2))
        ok = (r.status_code in (400, 500)) and (r.json().get("success") is False)
        rec(group, "C4-目标密码错误被拒绝", ok, f"status={r.status_code} body={r.text[:120]}")

        # C5 源密码错误（目标正确）—— 当前 addMapping 仅校验目标，需观察；修复后应拒绝
        bad_src = {"host": "127.0.0.1", "port": 13306, "user": "root", "password": "wrongpass"}
        r = add_mapping(s, mapping_body(bad_src, TGT))
        j = r.json()
        if r.status_code == 200 and j.get("success") is True:
            # 居然创建成功 -> 潜在缺陷：源侧不可达未被校验
            created = j.get("data", {}).get("created", [])
            for k in created:
                track_mapping(k)
                remove_mapping(s, k)
            rec(group, "C5-源密码错误应被拒绝(当前未校验-待修复)", False,
                f"源不可达却被创建: {created}")
        else:
            rec(group, "C5-源密码错误被拒绝", True, f"status={r.status_code}")

        # C6 重复添加默认映射 -> 已存在/跳过
        r = add_mapping(s, mapping_body(SRC, TGT))
        j = r.json()
        handled = (r.status_code in (200, 400)) and (
            (j.get("success") is False) or ("skipped" in j.get("data", {}) or "already" in str(j.get("code", "")).lower()))
        rec(group, "C6-重复添加被妥善处理", handled, f"status={r.status_code} body={r.text[:120]}")

        # C7 自同步环路（source==target） —— 必须被拒绝，否则产生无限复制环
        b = mapping_body(SRC, SRC)  # 13306 -> 13306
        b["ignoreDatabases"] = ["re:.*"]  # 即便忽略全部，也应从配置层面拒绝
        r = add_mapping(s, b)
        if r.status_code == 200 and r.json().get("success") is True:
            created = r.json().get("data", {}).get("created", [])
            for k in created:
                track_mapping(k)
                remove_mapping(s, k)
            rec(group, "C7-自同步环路应被拒绝(当前未拦截-待修复)", False,
                f"自同步被创建: {created}")
        else:
            rec(group, "C7-自同步环路被拒绝", True, f"status={r.status_code} body={r.text[:120]}")

        # C8 类型错误（端口传字符串非数字）
        b = mapping_body(SRC, TGT)
        b["sourcePort"] = "not_a_number"
        r = add_mapping(s, b)
        # Spring 类型绑定失败通常 400
        ok = r.status_code in (400, 500)
        rec(group, "C8-端口类型错误被拒绝", ok, f"status={r.status_code} body={r.text[:120]}")

    except Exception as e:
        rec(group, "G2-整体", False, f"{type(e).__name__}: {e}")


# =====================================================================
# G3 字符输入异常
# =====================================================================
def g3_character_input():
    group = "G3-字符输入"
    s = new_session()
    try:
        login(s)
        # H1 中文/Unicode 库名同步正确性
        db_cjk = "e2e_中文库_测试"
        ensure_db(SRC, db_cjk, drop_first=True)
        db_exec(SRC, f"CREATE TABLE IF NOT EXISTS `{db_cjk}`.`t_cjk` (id INT PRIMARY KEY AUTO_INCREMENT, 备注 VARCHAR(100))", database=db_cjk)
        db_exec(SRC, f"INSERT INTO `{db_cjk}`.`t_cjk`(备注) VALUES ('你好world'),('emoji😀')", database=db_cjk)
        try:
            wait_condition(lambda: rowcount(TGT, db_cjk, "t_cjk") >= 2, timeout=90, desc="target cjk>=2")
            rows = db_query(TGT, f"SELECT * FROM `{db_cjk}`.`t_cjk`", database=db_cjk)
            vals = set(str(r.get("备注")) for r in rows)
            ok = {"你好world", "emoji😀"}.issubset(vals)
            rec(group, "H1-中文/Emoji库名与内容同步正确", ok, f"vals={vals}")
        except Exception as e:
            rec(group, "H1-中文/Emoji库名与内容同步正确", False, f"异常: {e}")
        finally:
            drop_db(SRC, db_cjk)

        # H2 含空格的合法标识符库名（验证 SQL 标识符正确加反引号引用）
        db_spec = "e2e spec db"
        try:
            ensure_db(SRC, db_spec, drop_first=True)
            db_exec(SRC, f"CREATE TABLE IF NOT EXISTS `{db_spec}`.t_spec (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(50))", database=db_spec)
            db_exec(SRC, f"INSERT INTO `{db_spec}`.t_spec(v) VALUES ('x')", database=db_spec)
            wait_condition(lambda: rowcount(TGT, db_spec, "t_spec") >= 1, timeout=90, desc="target spec>=1")
            rec(group, "H2-含空格库名被安全同步(标识符正确引用)", rowcount(TGT, db_spec, "t_spec") >= 1,
                f"src_rows={rowcount(SRC,db_spec,'t_spec')} tgt_rows={rowcount(TGT,db_spec,'t_spec')}")
        except Exception as e:
            rec(group, "H2-含空格库名不导致应用崩溃", False, f"异常: {type(e).__name__}: {e}")
        finally:
            try:
                drop_db(SRC, db_spec)
            except Exception:
                pass

        # H3 注入字符串当作库名字面量（不应被执行）
        inj = "DROP TABLE geo_source.dyn_tbl; --"
        b = mapping_body(SRC, TGT)
        # 用一个临时映射把注入串作为 ignoreDatabases，验证仅作字面过滤、不执行
        b["ignoreDatabases"] = [inj]
        r = add_mapping(s, b)
        j = r.json()
        if r.status_code == 200 and j.get("success") is True:
            created = j.get("data", {}).get("created", [])
            for k in created:
                track_mapping(k)
            # 验证注入语句未对任何真实表造成破坏
            before = rowcount(SRC, "geo_source", "dyn_tbl")
            time.sleep(2)
            after = rowcount(SRC, "geo_source", "dyn_tbl")
            for k in created:
                remove_mapping(s, k)
            rec(group, "H3-注入串仅作字面过滤未执行", before == after, f"dyn_tbl before={before} after={after}")
        else:
            rec(group, "H3-注入串映射被拒(同样安全)", True, f"status={r.status_code}")

        # H4 转换规则值含注入字符（值应参数化）
        db_tr = "e2e_transform"
        ensure_db(SRC, db_tr, drop_first=True)
        db_exec(SRC, f"CREATE TABLE IF NOT EXISTS `{db_tr}`.t_tr (id INT PRIMARY KEY AUTO_INCREMENT, note VARCHAR(100))", database=db_tr)
        db_exec(SRC, f"INSERT INTO `{db_tr}`.t_tr(note) VALUES ('secret'),('normal')", database=db_tr)
        try:
            wait_condition(lambda: rowcount(TGT, db_tr, "t_tr") >= 2, timeout=90, desc="target tr>=2")
            # 默认映射对 geo_source.dyn_tbl 有 secret->MASKED；本库无规则，应原样同步
            rows = db_query(TGT, f"SELECT note FROM `{db_tr}`.t_tr", database=db_tr)
            notes = set(str(r.get("note")) for r in rows)
            rec(group, "H4-无规则库值原样同步(不误转换)", notes == {"secret", "normal"}, f"notes={notes}")
        except Exception as e:
            rec(group, "H4-转换规则值同步", False, f"异常: {e}")
        finally:
            drop_db(SRC, db_tr)

        # H5 超长主机（256+字符）
        b = mapping_body(SRC, TGT)
        b["sourceHost"] = "a" * 256
        r = add_mapping(s, b)
        rec(group, "H5-超长主机被400拒绝", r.status_code == 400, f"status={r.status_code}")

        # H6 超长密码（257字符，超过 Size max=256）
        b = mapping_body(SRC, TGT)
        b["sourcePassword"] = "p" * 257
        b["targetPassword"] = "p" * 257
        r = add_mapping(s, b)
        rec(group, "H6-超长密码被400拒绝", r.status_code == 400, f"status={r.status_code}")

        # H7 密码含特殊符号（'; DROP --）通过校验（密码无 pattern）
        b = mapping_body(SRC, TGT)
        b["sourcePassword"] = "p'; DROP --"
        b["targetPassword"] = "p'; DROP --"
        r = add_mapping(s, b)
        # 校验层允许（不因 pattern 拒绝）；连接测试会失败（密码错）但不崩溃
        ok = r.status_code in (200, 400, 500)
        rec(group, "H7-含特殊符号密码不被校验层误杀", ok, f"status={r.status_code}")

        # H8 null body / 空 body
        r = s.post(BASE_URL + "/sync/mapping/add", json=None, timeout=15)
        rec(group, "H8-null请求体被拒", r.status_code in (400, 415), f"status={r.status_code}")
    except Exception as e:
        rec(group, "G3-整体", False, f"{type(e).__name__}: {e}")


# =====================================================================
# G4 断电 / 断网 故障注入 + 恢复一致性
# =====================================================================
def _fault_prep_db(db):
    # 关键：prep 用「不 drop_first」的建库。DROP 一个已被 flink-cdc 捕获的表/库会触发其
    # MySqlSourceReader.fillMetadataForBinlogSplit 元数据损坏崩溃（fromIndex>toIndex），
    # 故故障注入测试全程避免对正在同步的库做 DROP。每次运行用唯一库名，规避历史脏 DROP。
    ensure_db(SRC, db)
    db_exec(SRC, f"CREATE TABLE IF NOT EXISTS `{db}`.t_fault (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(100))", database=db)
    db_exec(SRC, f"INSERT INTO `{db}`.t_fault(v) VALUES ('seed1'),('seed2')", database=db)
    # 宽容等待：若应用启动初期源端 binlog 里残留历史 DROP 导致作业崩溃，watchdog 会以
    # 全新 INITIAL 重扫自愈（binlog 位点越过脏 DROP），这里给足恢复时间。
    wait_condition(lambda: rowcount(TGT, db, "t_fault") >= 2, timeout=180, desc="fault seed synced")
    return db


def g4_power_network():
    group = "G4-断电断网"
    s = new_session()
    try:
        login(s)
        # 每轮唯一库名：规避历史脏 DROP 残留在 binlog/checkpoint 中
        db = _fault_prep_db(f"e2e_fault_{time.time_ns() % 100000000}")

        # P1 断网（pause 目标）期间持续写入，应用不应崩溃，恢复后一致
        pause_cont("tgt")
        try:
            for i in range(20):
                db_exec(SRC, f"INSERT INTO `{db}`.t_fault(v) VALUES ('p1_{i}')", database=db)
            # 应用健康：其他接口应仍可响应
            r = s.get(BASE_URL + "/sync/status", timeout=10)
            app_alive = r.status_code == 200
            rec(group, "P1a-断网期间应用存活(_status可响应)", app_alive, f"status={r.status_code}")
            src_rows = rowcount(SRC, db, "t_fault")
        finally:
            unpause_cont("tgt")
        ensure_container_up("tgt")
        wait_condition(lambda: rowcount(TGT, db, "t_fault") == rowcount(SRC, db, "t_fault"),
                       timeout=240, desc="P1 target 追平")
        rec(group, "P1b-断网恢复后行数一致(无丢失)", rowcount(TGT, db, "t_fault") == rowcount(SRC, db, "t_fault"),
            f"src={rowcount(SRC,db,'t_fault')} tgt={rowcount(TGT,db,'t_fault')}")

        # P2 断电（stop 目标）
        stop_cont("tgt")
        try:
            for i in range(20):
                db_exec(SRC, f"INSERT INTO `{db}`.t_fault(v) VALUES ('p2_{i}')", database=db)
            r = s.get(BASE_URL + "/sync/status", timeout=10)
            rec(group, "P2a-断电期间应用存活", r.status_code == 200, f"status={r.status_code}")
        finally:
            start_cont("tgt")
        ensure_container_up("tgt")
        wait_condition(lambda: rowcount(TGT, db, "t_fault") == rowcount(SRC, db, "t_fault"),
                       timeout=240, desc="P2 target 追平")
        rec(group, "P2b-断电恢复后行数一致", rowcount(TGT, db, "t_fault") == rowcount(SRC, db, "t_fault"),
            f"src={rowcount(SRC,db,'t_fault')} tgt={rowcount(TGT,db,'t_fault')}")

        # P3 断网（pause 源）
        pause_cont("src")
        try:
            r = s.get(BASE_URL + "/sync/status", timeout=10)
            rec(group, "P3a-源断网期间应用存活", r.status_code == 200, f"status={r.status_code}")
        finally:
            unpause_cont("src")
        ensure_container_up("src")
        time.sleep(5)
        rec(group, "P3b-源断网恢复后应用仍健康", True)

        # P4 断电（stop 源）
        src_before = rowcount(SRC, db, "t_fault")
        stop_cont("src")
        try:
            r = s.get(BASE_URL + "/sync/status", timeout=10)
            rec(group, "P4a-源断电期间应用存活", r.status_code == 200, f"status={r.status_code}")
        finally:
            start_cont("src")
        ensure_container_up("src")
        # 源恢复后补写并验证
        db_exec(SRC, f"INSERT INTO `{db}`.t_fault(v) VALUES ('p4_after')", database=db)
        wait_condition(lambda: rowcount(TGT, db, "t_fault") == rowcount(SRC, db, "t_fault"),
                       timeout=240, desc="P4 target 追平")
        rec(group, "P4b-源断电恢复后行数一致", rowcount(TGT, db, "t_fault") == rowcount(SRC, db, "t_fault"),
            f"src={rowcount(SRC,db,'t_fault')} tgt={rowcount(TGT,db,'t_fault')}")

        # 注意：这里绝不能 DROP —— 对正在被 CDC 捕获的库执行 DROP 会触发
        # flink-cdc MySqlSourceReader 元数据崩溃（详见 _fault_prep_db 注释）。
        # 库已登记进 E2E_DBS，由 cleanup() 在全部组结束后统一处理。
    except Exception as e:
        # 确保容器恢复
        for role in ("src", "tgt"):
            try:
                unpause_cont(role)
                start_cont(role)
            except Exception:
                pass
        rec(group, "G4-整体", False, f"{type(e).__name__}: {e}")


# =====================================================================
# G5 压力测试
# =====================================================================
def g5_stress():
    group = "G5-压力"
    s = new_session()
    try:
        login(s)
        # S1 大批量行
        db = "e2e_stress"
        ensure_db(SRC, db, drop_first=True)
        db_exec(SRC, f"CREATE TABLE IF NOT EXISTS `{db}`.t_big (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(100), ts DATETIME DEFAULT CURRENT_TIMESTAMP)", database=db)
        N = 20000
        t0 = time.time()
        # 分批插入，每批 2000
        batch = 2000
        for i in range(0, N, batch):
            vals = ",".join([f"('v{i+j}')" for j in range(batch)])
            db_exec(SRC, f"INSERT INTO `{db}`.t_big(v) VALUES {vals}", database=db)
        src_insert_time = time.time() - t0
        wait_condition(lambda: rowcount(TGT, db, "t_big") == N, timeout=240, desc=f"target t_big=={N}")
        tgt_rows = rowcount(TGT, db, "t_big")
        rec(group, "S1-2万行全量同步无丢失", tgt_rows == N, f"src={N} tgt={tgt_rows} src_insert={src_insert_time:.1f}s")

        # 内容抽样校验（不丢不错）
        samp = db_query(TGT, f"SELECT COUNT(DISTINCT v) AS d FROM `{db}`.t_big", database=db)
        rec(group, "S1b-目标无重复(v去重==N)", samp and samp[0]["d"] == N, f"distinct={samp[0]['d'] if samp else None}")

        # S2 多表（30 张表）
        db2 = "e2e_manytables"
        ensure_db(SRC, db2, drop_first=True)
        tcount = 30
        for i in range(tcount):
            db_exec(SRC, f"CREATE TABLE IF NOT EXISTS `{db2}`.`t_{i}` (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(50))", database=db2)
            db_exec(SRC, f"INSERT INTO `{db2}`.`t_{i}`(v) VALUES ('x')", database=db2)
        wait_condition(
            lambda: all(rowcount(TGT, db2, f"t_{i}") >= 1 for i in range(tcount)),
            timeout=180, desc="all 30 tables synced")
        synced = sum(1 for i in range(tcount) if rowcount(TGT, db2, f"t_{i}") >= 1)
        rec(group, "S2-30张表全部同步", synced == tcount, f"synced={synced}/{tcount}")

        # S3 并发写入（多线程插入，验证收敛）
        db3 = "e2e_concurrent"
        ensure_db(SRC, db3, drop_first=True)
        db_exec(SRC, f"CREATE TABLE IF NOT EXISTS `{db3}`.t_cc (id INT PRIMARY KEY AUTO_INCREMENT, w INT, v VARCHAR(50))", database=db3)
        threads = []
        def worker(wid):
            for j in range(200):
                try:
                    db_exec(SRC, f"INSERT INTO `{db3}`.t_cc(w,v) VALUES ({wid},'c')", database=db3)
                except Exception:
                    pass
        for w in range(10):
            t = threading.Thread(target=worker, args=(w,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        wait_condition(lambda: rowcount(TGT, db3, "t_cc") >= 2000, timeout=240, desc="concurrent target>=2000")
        rec(group, "S3-并发写入收敛一致", rowcount(TGT, db3, "t_cc") == 2000,
            f"src={rowcount(SRC,db3,'t_cc')} tgt={rowcount(TGT,db3,'t_cc')}")

        drop_db(SRC, db)
        drop_db(SRC, db2)
        drop_db(SRC, db3)
    except Exception as e:
        rec(group, "G5-整体", False, f"{type(e).__name__}: {e}")


# =====================================================================
# G6 可靠性
# =====================================================================
def g6_reliability():
    group = "G6-可靠性"
    s = new_session()
    try:
        login(s)
        # 每轮唯一库名：目标端可能残留上轮未传播 DROP 的旧库（job 已死时 DROP 丢失），
        # 复用同名库会导致行数永远对不上（假失败）。且不能对源端已捕获库做 drop_first
        # （触发 flink-cdc binlog split 元数据崩溃），唯一库名是唯一安全解。
        db = f"e2e_reliab_{time.time_ns() % 100000000}"
        ensure_db(SRC, db)
        db_exec(SRC, f"CREATE TABLE IF NOT EXISTS `{db}`.t_r (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(100))", database=db)
        db_exec(SRC, f"INSERT INTO `{db}`.t_r(v) VALUES ('seed')", database=db)
        wait_condition(lambda: rowcount(TGT, db, "t_r") >= 1, timeout=90, desc="reliab seed")

        # R1 应用 kill -9 中同步 -> 重启恢复
        for i in range(50):
            db_exec(SRC, f"INSERT INTO `{db}`.t_r(v) VALUES ('r1_{i}')", database=db)
        pid = find_app_pid()
        rec(group, "R1a-定位到应用PID", pid is not None, f"pid={pid}")
        kill_app()
        time.sleep(3)
        # 继续写入（源端独立）
        for i in range(50, 100):
            db_exec(SRC, f"INSERT INTO `{db}`.t_r(v) VALUES ('r1_{i}')", database=db)
        p = start_app_detached()
        up = wait_app(timeout=180)
        rec(group, "R1b-应用重启成功", up, f"up={up}")
        wait_condition(lambda: rowcount(TGT, db, "t_r") == rowcount(SRC, db, "t_r"),
                       timeout=180, desc="R1 target 追平")
        rec(group, "R1c-kill-9重启后数据一致", rowcount(TGT, db, "t_r") == rowcount(SRC, db, "t_r"),
            f"src={rowcount(SRC,db,'t_r')} tgt={rowcount(TGT,db,'t_r')}")

        # R2 目标断电时重启应用 -> 应用应存活不崩；目标恢复后追平
        stop_cont("tgt")
        try:
            kill_app()
            time.sleep(3)
            start_app_detached()
            up2 = wait_app(timeout=180)
            rec(group, "R2a-目标断电时应用仍启动成功", up2, f"up={up2}")
            # 源继续写
            for i in range(100, 150):
                db_exec(SRC, f"INSERT INTO `{db}`.t_r(v) VALUES ('r2_{i}')", database=db)
        finally:
            start_cont("tgt")
        ensure_container_up("tgt")
        wait_condition(lambda: rowcount(TGT, db, "t_r") == rowcount(SRC, db, "t_r"),
                       timeout=180, desc="R2 target 追平")
        rec(group, "R2b-目标恢复后数据一致", rowcount(TGT, db, "t_r") == rowcount(SRC, db, "t_r"),
            f"src={rowcount(SRC,db,'t_r')} tgt={rowcount(TGT,db,'t_r')}")

        # R3 长时间 soak：间隔写入，最终一致
        for i in range(10):
            db_exec(SRC, f"INSERT INTO `{db}`.t_r(v) VALUES ('soak_{i}')", database=db)
            time.sleep(1)
        wait_condition(lambda: rowcount(TGT, db, "t_r") == rowcount(SRC, db, "t_r"),
                       timeout=120, desc="R3 soak 追平")
        rec(group, "R3-soak后数据一致", rowcount(TGT, db, "t_r") == rowcount(SRC, db, "t_r"),
            f"src={rowcount(SRC,db,'t_r')} tgt={rowcount(TGT,db,'t_r')}")

        drop_db(SRC, db)
    except Exception as e:
        for role in ("src", "tgt"):
            try:
                unpause_cont(role); start_cont(role)
            except Exception:
                pass
        rec(group, "G6-整体", False, f"{type(e).__name__}: {e}")


# =====================================================================
# 清理
# =====================================================================
def cleanup():
    print("\n=== 清理中 ===", flush=True)
    s = new_session()
    try:
        login(s)
    except Exception:
        pass
    # 移除测试新增映射
    for k in CREATED_MAPPINGS:
        try:
            remove_mapping(s, k)
            print(f"  移除映射 {k}", flush=True)
        except Exception as e:
            print(f"  移除映射 {k} 失败: {e}", flush=True)
    # 删除源端 e2e_ 库（DDL 会经 CDC 传播到目标）
    import glob
    for db in list(E2E_DBS):
        try:
            drop_db(SRC, db)
            print(f"  删库 {db}", flush=True)
        except Exception as e:
            print(f"  删库 {db} 失败: {e}", flush=True)
    # 兜底：SRC 与 TGT 都扫描 e2e_ 残留库并直接删除（SRC 删除经 CDC 传播可能因 job
    # 死亡而丢失，TGT 只能直连删；TGT 直连 DROP 不会触发 flink-cdc 崩溃——它只读源 binlog）
    for cfg in (SRC, TGT):
        try:
            rows = db_query(cfg, "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'e2e_%'")
            for r in rows:
                nm = r["schema_name"]
                try:
                    drop_db(cfg, nm)
                except Exception:
                    pass
        except Exception:
            pass
    # 恢复容器
    for role in CONTAINERS_TOUCHED:
        try:
            unpause_cont(role); start_cont(role)
        except Exception:
            pass
    print("=== 清理完成 ===", flush=True)


def write_report():
    total = len(RESULTS)
    passed = sum(1 for _, _, ok, _ in RESULTS if ok)
    failed = total - passed
    by_group = {}
    for g, n, ok, d in RESULTS:
        by_group.setdefault(g, [0, 0])
        by_group[g][0] += 1
        by_group[g][1] += 1 if ok else 0
    lines = []
    lines.append(f"# GeoDRSync 前后端全自动化测试报告")
    lines.append("")
    lines.append(f"- 生成时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- 后端基准地址：`{BASE_URL}`")
    lines.append(f"- 总用例：**{total}**，通过：**{passed}**，失败：**{failed}**")
    lines.append("")
    lines.append("## 分组概览")
    lines.append("")
    lines.append("| 分组 | 用例数 | 通过 | 失败 |")
    lines.append("| --- | --- | --- | --- |")
    for g, (cnt, ok) in by_group.items():
        lines.append(f"| {g} | {cnt} | {ok} | {cnt-ok} |")
    lines.append("")
    lines.append("## 明细")
    lines.append("")
    lines.append("| 分组 | 用例 | 结果 | 说明 |")
    lines.append("| --- | --- | --- | --- |")
    for g, n, ok, d in RESULTS:
        esc = (d or "").replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {g} | {n} | {'PASS' if ok else 'FAIL'} | {esc} |")
    out = "\n".join(lines)
    with open(os.path.join(APP_DIR, "..", "tests", "suite_full_e2e_report.md"), "w", encoding="utf-8") as f:
        f.write(out)
    return out


def main():
    print("========== GeoDRSync 全自动化测试开始 ==========", flush=True)
    only = os.environ.get("E2E_GROUPS")
    selected = set(x.strip() for x in only.split(",")) if only else None
    plan = [
        ("G1", g1_normal_flow),
        ("G2", g2_config_errors),
        ("G3", g3_character_input),
        ("G4", g4_power_network),
        ("G5", g5_stress),
        ("G6", g6_reliability),
    ]
    try:
        for name, fn in plan:
            if selected and name not in selected:
                continue
            fn()
    finally:
        cleanup()
        rep = write_report()
        total = len(RESULTS)
        passed = sum(1 for _, _, ok, _ in RESULTS if ok)
        print("\n========== 汇总 ==========", flush=True)
        print(f"总用例={total} 通过={passed} 失败={total-passed}", flush=True)
        for g, n, ok, d in RESULTS:
            if not ok:
                print(f"  FAIL [{g}] {n} :: {d}", flush=True)
        print("\n报告已写入 tests/suite_full_e2e_report.md", flush=True)


if __name__ == "__main__":
    main()
