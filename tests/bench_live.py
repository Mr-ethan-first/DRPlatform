# -*- coding: utf-8 -*-
"""现场实测：全量同步收敛速度 + 增量实时延时（对当前运行中的应用，端口 8899）"""
import pymysql, time, uuid, statistics, re, datetime, sys

SRC = dict(host='127.0.0.1', port=13306, user='root', password='123456')
TGT = dict(host='127.0.0.1', port=13307, user='root', password='123456')
LOG_PATH = 'D:/WorkSpace/flink-cdc-sync/remote-data-sync/app_run.log'

def log(m):
    print("[%s] %s" % (time.strftime('%H:%M:%S'), m), flush=True)

def conn(p):
    return pymysql.connect(host=p['host'], port=p['port'], user=p['user'], password=p['password'],
                           charset='utf8mb4', connect_timeout=10, autocommit=True)

src = conn(SRC); tgt = conn(TGT)
sc = src.cursor(); tc = tgt.cursor()
t_run_start = datetime.datetime.now()

DDL = "(`id` BIGINT NOT NULL PRIMARY KEY, `note` VARCHAR(100), `ts` DATETIME(3))"

def wait_rows(db, table, want, timeout=240, poll=0.05):
    """轮询目标直到行数>=want，返回耗时秒；超时返回 None"""
    t0 = time.time()
    last = -1
    while time.time() - t0 < timeout:
        try:
            tc.execute("SELECT COUNT(*) FROM `%s`.`%s`" % (db, table))
            n = tc.fetchone()[0]
            if n != last:
                log("    ...目标 %s.%s 行数=%d (want=%d)" % (db, table, n, want))
                last = n
            if n >= want:
                return time.time() - t0
        except Exception:
            pass
        time.sleep(poll)
    return None

try:
    # ---------- 基线 ----------
    log("=== 基线：两侧存量库核对 ===")
    sc.execute("SHOW DATABASES")
    src_dbs = [r[0] for r in sc.fetchall() if r[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')]
    for db in src_dbs:
        sc.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s", (db,))
        tbls = [r[0] for r in sc.fetchall()]
        tot = 0
        for t in tbls:
            sc.execute("SELECT COUNT(*) FROM `%s`.`%s`" % (db, t)); tot += sc.fetchone()[0]
        log("  %s: %d 表 / %d 行" % (db, len(tbls), tot))

    run_id = uuid.uuid4().hex[:6]
    inc_db = "bench_inc_%s" % run_id
    full_db = "bench_full_%s" % run_id

    # ---------- Phase 0：建增量测试库，等首次全量 ----------
    log("=== Phase 0：新建 %s（触发自动发现+全量） ===" % inc_db)
    sc.execute("CREATE DATABASE `%s`" % inc_db)
    sc.execute("CREATE TABLE `%s`.`t1` %s" % (inc_db, DDL))
    sc.execute("INSERT INTO `%s`.`t1` VALUES (1,'seed',NOW(3))" % inc_db)
    dt = wait_rows(inc_db, 't1', 1, timeout=240)
    log("新库首次全量收敛耗时: %s" % ("%.1fs" % dt if dt is not None else "超时失败"))

    # ---------- Phase B：增量实时延时 ----------
    log("=== Phase B：增量单行 INSERT 延时（10 次） ===")
    lat = []
    for i in range(2, 12):
        t0 = time.time()
        sc.execute("INSERT INTO `%s`.`t1` VALUES (%%s,%%s,NOW(3))" % inc_db, (i, "lat-%d" % i))
        t_commit = time.time()
        while True:
            tc.execute("SELECT COUNT(*) FROM `%s`.`t1` WHERE id=%%s" % inc_db, (i,))
            if tc.fetchone()[0] == 1:
                break
            if time.time() - t_commit > 60:
                log("  行 %d 60s 未可见!" % i); break
            time.sleep(0.05)
        lat.append(time.time() - t0)
        log("  行 %-2d 端到端 %.0f ms" % (i, lat[-1] * 1000))
    log("单行延时: avg=%.0fms  min=%.0fms  max=%.0fms  p50=%.0fms" % (
        statistics.mean(lat) * 1000, min(lat) * 1000, max(lat) * 1000,
        statistics.median(lat) * 1000))

    log("=== Phase B2：批量 1000 行 ===")
    vals = ",".join(["(%s,%s,NOW(3))"] * 1000)
    args = []
    for i in range(1000):
        args += [1001 + i, "batch-%d" % i]
    t0 = time.time()
    sc.execute("INSERT INTO `%s`.`t1` VALUES %s" % (inc_db, vals), args)
    t_commit = time.time()
    log("  源端写入 1000 行耗时 %.0fms" % ((t_commit - t0) * 1000))
    while True:
        tc.execute("SELECT COUNT(*) FROM `%s`.`t1` WHERE id BETWEEN 1001 AND 2000" % inc_db)
        if tc.fetchone()[0] == 1000:
            break
        if time.time() - t_commit > 120:
            log("  批量 120s 未齐!"); break
        time.sleep(0.05)
    t_all = time.time()
    log("批量 1000 行端到端 %.2fs（提交后 %.2fs 可见，端到端吞吐 %.0f rows/s）" % (
        t_all - t0, t_all - t_commit, 1000 / (t_all - t0)))

    # ---------- Phase A：全量同步（新库 20000 行） ----------
    log("=== Phase A：全量同步——新建 %s 写入 20000 行 ===" % full_db)
    t_create = time.time()
    sc.execute("CREATE DATABASE `%s`" % full_db)
    sc.execute("CREATE TABLE `%s`.`t1` %s" % (full_db, DDL))
    for c in range(20):
        vals = ",".join(["(%s,%s,NOW(3))"] * 1000)
        args = []
        for i in range(1000):
            args += [c * 1000 + i + 1, "full-%d-%d" % (c, i)]
        sc.execute("INSERT INTO `%s`.`t1` VALUES %s" % (full_db, vals), args)
    t_write = time.time()
    sc.execute("SELECT COUNT(*) FROM `%s`.`t1`" % full_db)
    log("  源端 20000 行写入耗时 %.2fs（%.0f rows/s），源端确认 %d 行" % (
        t_write - t_create, 20000 / (t_write - t_create), sc.fetchone()[0]))
    dt = wait_rows(full_db, 't1', 20000, timeout=300)
    log("全量收敛（建库→目标 20000 行可见）: %s" % ("%.1fs" % dt if dt is not None else "超时失败"))

    # ---------- 日志标记：拆分 发现+重启 vs 纯快照 ----------
    log("=== 作业重启/快照日志标记 ===")
    try:
        with open(LOG_PATH, 'rb') as f:
            for raw in f:
                line = raw.decode('utf-8', 'replace')
                m = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})', line)
                if not m:
                    continue
                ts = datetime.datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S.%f')
                if ts < t_run_start - datetime.timedelta(seconds=10):
                    continue
                if re.search(r'forceInitial|Snapshotting|snapshot.*(start|complete|finish)|Submitting|restart|检出|新表|发现', line, re.I):
                    log("  LOG| " + line.rstrip()[:220])
    except FileNotFoundError:
        log("  (日志文件不存在)")

    # ---------- 清理：DROP 源端 bench 库，等目标端对账删除 ----------
    log("=== 清理：DROP 源端 bench 库，等待目标端删除对账 ===")
    t_drop = time.time()
    sc.execute("DROP DATABASE `%s`" % inc_db)
    sc.execute("DROP DATABASE `%s`" % full_db)
    gone = False
    while time.time() - t_drop < 200:
        tc.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name IN (%s,%s)", (inc_db, full_db))
        if tc.fetchone()[0] == 0:
            gone = True
            break
        time.sleep(1)
    log("目标端 bench 库已删除: %s（耗时 %.0fs）" % (gone, time.time() - t_drop))

    # ---------- 终态一致性 ----------
    log("=== 终态：存量库逐表核对 ===")
    sc.execute("SHOW DATABASES")
    src_dbs = [r[0] for r in sc.fetchall() if r[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')]
    bad = 0
    for db in src_dbs:
        sc.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s", (db,))
        for (t,) in sc.fetchall():
            sc.execute("SELECT COUNT(*) FROM `%s`.`%s`" % (db, t)); a = sc.fetchone()[0]
            tc.execute("SELECT COUNT(*) FROM `%s`.`%s`" % (db, t)); b = tc.fetchone()[0]
            flag = "OK " if a == b else "MISMATCH"
            if a != b:
                bad += 1
            log("  %-12s %-20s src=%-7d tgt=%-7d %s" % (db, t, a, b, flag))
    log("终态一致性: %s（%d 处不一致）" % ("全部一致" if bad == 0 else "存在不一致", bad))
    log("=== 实测结束 ===")

except Exception as e:
    log("脚本异常: %r" % e)
    # 兜底清理
    for db in (locals().get('inc_db'), locals().get('full_db')):
        if db:
            try:
                sc.execute("DROP DATABASE IF EXISTS `%s`" % db)
            except Exception:
                pass
    sys.exit(1)
