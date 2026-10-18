# -*- coding: utf-8 -*-
"""DRPlatform 压测数据生成器：45 个数据库 × 67 张表 ≈ 3015 张表。

设计目标（对齐用户压测需求「45 个数据库 + 3000 表并发同步」）：
  - 库名 perf_db_00 ~ perf_db_44（45 个），表名 perf_t_000 ~ perf_t_066（每库 67 张）
  - 5 种表结构变体循环（订单型/设备型/无 update_time 型/小表/宽表），贴近真实业务差异
  - 行数分层：普通表 200~600 行；每 25 张表出现一张 3000 行大表（拉长同步窗口）
  - update_time 显式打散到过去 24 小时（避免同秒批量插入把增量扫描逼入极限同时间戳场景，
    同时保留部分表「无 update_time」走全量兜底路径）
  - 总量约 150 万行，灌入源端 13306；目标端 13307 保持干净（由同步服务自动建库建表）

用法：
    python gen_perf_data.py            # 已存在则跳过（幂等）
    python gen_perf_data.py --force    # 强制重建（先 DROP 再灌）
"""
import os
import sys
import time
from datetime import datetime, timedelta

import pymysql

SRC = dict(host="127.0.0.1", port=13306, user="root", password="123456")
DBS = 45
TABLES_PER_DB = 67
DB_PREFIX = "perf_db_"
T_PREFIX = "perf_t_"
BIG_EVERY = 25          # 每 25 张表一张大表
BIG_ROWS = 3000
INSERT_CHUNK = 1000

BASE_NOW = datetime.now().replace(microsecond=0)


def conn(db=None):
    return pymysql.connect(host=SRC["host"], port=SRC["port"], user=SRC["user"],
                           password=SRC["password"], database=db, charset="utf8mb4",
                           connect_timeout=10, read_timeout=600, write_timeout=600)


def ddl_for(variant):
    """5 种表结构变体。"""
    if variant == 0:      # 订单型（带 update_time + 唯一业务键）
        return ("CREATE TABLE `{t}` ("
                "id BIGINT NOT NULL AUTO_INCREMENT, order_no VARCHAR(32) NOT NULL, "
                "user_id BIGINT DEFAULT NULL, amount DECIMAL(12,2) DEFAULT NULL, "
                "status TINYINT DEFAULT NULL, remark VARCHAR(128) DEFAULT NULL, "
                "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                "PRIMARY KEY(id), UNIQUE KEY uk_order_no(order_no)) "
                "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
    if variant == 1:      # 设备遥测型
        return ("CREATE TABLE `{t}` ("
                "id BIGINT NOT NULL AUTO_INCREMENT, device_code VARCHAR(32) NOT NULL, "
                "metric DOUBLE DEFAULT NULL, collected_at DATETIME DEFAULT NULL, "
                "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                "PRIMARY KEY(id), KEY idx_device(device_code)) "
                "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
    if variant == 2:      # 无 update_time（走全量兜底路径）
        return ("CREATE TABLE `{t}` ("
                "id BIGINT NOT NULL AUTO_INCREMENT, code VARCHAR(32) NOT NULL, "
                "name VARCHAR(64) DEFAULT NULL, qty INT DEFAULT NULL, "
                "PRIMARY KEY(id), UNIQUE KEY uk_code(code)) "
                "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
    if variant == 3:      # 小表
        return ("CREATE TABLE `{t}` ("
                "id INT NOT NULL AUTO_INCREMENT, k VARCHAR(16) NOT NULL, v INT DEFAULT NULL, "
                "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                "PRIMARY KEY(id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
    # variant == 4    宽表（12 个业务列 + update_time）
    cols = ", ".join(f"c{i:02d} VARCHAR(32) DEFAULT NULL" for i in range(1, 13))
    return ("CREATE TABLE `{t}` ("
            "id BIGINT NOT NULL AUTO_INCREMENT, " + cols + ", "
            "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
            "PRIMARY KEY(id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")


def rows_for(variant, n, tidx):
    """生成 n 行数据（返回列名元组与值元组列表）。"""
    out = []
    for i in range(n):
        ut = BASE_NOW - timedelta(seconds=(i * 37 + tidx * 11) % 86400)
        uts = ut.strftime("%Y-%m-%d %H:%M:%S")
        if variant == 0:
            out.append((f"PO{tidx:03d}_{i:06d}", 10000 + i, round((i % 997) / 7.0, 2), i % 5,
                        f"remark-{i % 100}", uts))
        elif variant == 1:
            out.append((f"DV{tidx:03d}{(i % 512):04d}", (i % 1000) / 3.0,
                        (BASE_NOW - timedelta(minutes=i % 1440)).strftime("%Y-%m-%d %H:%M:%S"), uts))
        elif variant == 2:
            out.append((f"CD{tidx:03d}_{i:06d}", f"name-{i % 500}", i * 2))
        elif variant == 3:
            out.append((f"k{i % 64}", i, uts))
        else:
            out.append(tuple(f"v{i % 97}-{c:02d}" for c in range(1, 13)) + (uts,))
    return out


COLS = {
    0: "(order_no,user_id,amount,status,remark,update_time)",
    1: "(device_code,metric,collected_at,update_time)",
    2: "(code,name,qty)",
    3: "(k,v,update_time)",
    4: "(" + ",".join(f"c{i:02d}" for i in range(1, 13)) + ",update_time)",
}


def main():
    force = "--force" in sys.argv
    c = conn()
    cur = c.cursor()
    existing = cur.execute(
        "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'perf_db_%'")
    n_exist = cur.fetchone()[0]
    if n_exist and not force:
        print(f"[gen] 源端已存在 {n_exist} 个 perf_db_* 库，跳过生成（--force 可强制重建）")
        return
    if force:
        print("[gen] --force：先清理旧 perf_db_* 库…")
        cur.execute("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'perf_db_%'")
        for (name,) in cur.fetchall():
            cur.execute(f"DROP DATABASE `{name}`")
        c.commit()

    t0 = time.time()
    total_tables = 0
    total_rows = 0
    for d in range(DBS):
        db = f"{DB_PREFIX}{d:02d}"
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}` DEFAULT CHARACTER SET utf8mb4")
        cur.execute(f"USE `{db}`")
        for t in range(TABLES_PER_DB):
            table = f"{T_PREFIX}{t:03d}"
            variant = t % 5
            nrows = BIG_ROWS if (t % BIG_EVERY == 0) else 200 + (t % 5) * 100
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(ddl_for(variant).format(t=table))
            rows = rows_for(variant, nrows, t)
            sql = f"INSERT INTO `{table}` {COLS[variant]} VALUES ({','.join(['%s'] * len(rows[0]))})"
            for i in range(0, len(rows), INSERT_CHUNK):
                cur.executemany(sql, rows[i:i + INSERT_CHUNK])
            total_tables += 1
            total_rows += nrows
        c.commit()
        if (d + 1) % 5 == 0 or d == 0:
            el = time.time() - t0
            print(f"[gen] {d + 1}/{DBS} 库完成（{total_tables} 表 / {total_rows} 行），耗时 {el:.0f}s")
    c.close()
    print(f"[gen] ✅ 完成：{DBS} 库 / {total_tables} 表 / {total_rows} 行，总耗时 {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
