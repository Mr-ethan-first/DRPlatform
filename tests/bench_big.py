# -*- coding: utf-8 -*-
"""大表全量基准：单表 N 行（默认 20 万），测 建库→首行可见→全量收敛 全时间线"""
import pymysql, time, uuid, sys

N = int(sys.argv[1]) if len(sys.argv) > 1 else 200000

SRC = dict(host='127.0.0.1', port=13306, user='root', password='123456')
TGT = dict(host='127.0.0.1', port=13307, user='root', password='123456')

def log(m):
    print("[%s] %s" % (time.strftime('%H:%M:%S'), m), flush=True)

def conn(p):
    return pymysql.connect(host=p['host'], port=p['port'], user=p['user'], password=p['password'],
                           charset='utf8mb4', connect_timeout=10, autocommit=True)

src = conn(SRC); tgt = conn(TGT)
sc = src.cursor(); tc = tgt.cursor()
DDL = "(`id` BIGINT NOT NULL PRIMARY KEY, `note` VARCHAR(100), `ts` DATETIME(3))"

try:
    db = "bench_big_%s" % uuid.uuid4().hex[:6]
    log("=== 大表全量基准：%s.t1 × %d 行 ===" % (db, N))
    t_create = time.time()
    sc.execute("CREATE DATABASE `%s`" % db)
    sc.execute("CREATE TABLE `%s`.`t1` %s" % (db, DDL))
    CHUNK = 2000
    t0 = time.time()
    done = 0
    while done < N:
        k = min(CHUNK, N - done)
        vals = ",".join(["(%s,%s,NOW(3))"] * k)
        args = []
        for i in range(k):
            args += [done + i + 1, "big-%d" % (done + i)]
        sc.execute("INSERT INTO `%s`.`t1` VALUES %s" % (db, vals), args)
        done += k
    t_write = time.time()
    log("源端写入 %d 行耗时 %.2fs（%.0f rows/s）" % (N, t_write - t0, N / (t_write - t0)))

    # 轮询目标
    t_first = None
    last_log = -1
    t_full = None
    while True:
        try:
            tc.execute("SELECT COUNT(*) FROM `%s`.`t1`" % db)
            n = tc.fetchone()[0]
        except Exception:
            n = -1
        if n > 0 and t_first is None:
            t_first = time.time()
            log("首行可见（建库后 %.1fs）" % (t_first - t_create))
        if n >= 10000 and n - last_log >= 20000:
            log("  目标行数=%d（首行后 %.1fs）" % (n, time.time() - t_first if t_first else -1))
            last_log = n
        if n >= N:
            t_full = time.time()
            break
        if time.time() - t_write > 300:
            log("300s 未收敛，放弃"); break
        time.sleep(0.2)
    if t_full:
        log("全量收敛：建库→%d 行可见共 %.1fs" % (N, t_full - t_create))
        if t_first:
            log("  ├─ 发现+重启 machinery: %.1fs" % (t_first - t_create))
            log("  └─ 纯快照导出+落库:     %.1fs（%.0f rows/s）" % (t_full - t_first, N / (t_full - t_first)))

    # 清理
    log("=== 清理 DROP %s ===" % db)
    t_drop = time.time()
    sc.execute("DROP DATABASE `%s`" % db)
    gone = False
    while time.time() - t_drop < 200:
        tc.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name=%s", (db,))
        if tc.fetchone()[0] == 0:
            gone = True; break
        time.sleep(1)
    log("目标端已删除: %s（%.0fs）" % (gone, time.time() - t_drop))

    # 终态核对
    sc.execute("SHOW DATABASES")
    src_dbs = [r[0] for r in sc.fetchall() if r[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')]
    bad = 0
    for d in src_dbs:
        sc.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s", (d,))
        for (t,) in sc.fetchall():
            sc.execute("SELECT COUNT(*) FROM `%s`.`%s`" % (d, t)); a = sc.fetchone()[0]
            tc.execute("SELECT COUNT(*) FROM `%s`.`%s`" % (d, t)); b = tc.fetchone()[0]
            if a != b:
                bad += 1; log("  MISMATCH %s.%s src=%d tgt=%d" % (d, t, a, b))
    log("终态一致性: %s" % ("全部一致" if bad == 0 else "%d 处不一致" % bad))
    log("=== 结束 ===")

except Exception as e:
    log("脚本异常: %r" % e)
    db = locals().get('db')
    if db:
        try: sc.execute("DROP DATABASE IF EXISTS `%s`" % db)
        except Exception: pass
    sys.exit(1)
