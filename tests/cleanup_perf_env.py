# -*- coding: utf-8 -*-
"""压测环境清理：删除 perf_db_* 库（源/目标）+ 元库 perf 水位/进度行。

用法：python cleanup_perf_env.py
前置：同步应用已停止（回归/压测均会自行停应用）。
"""
import sys

import pymysql

SRC = dict(host="127.0.0.1", port=13306, user="root", password="123456")
TGT = dict(host="127.0.0.1", port=13307, user="root", password="123456")
META = dict(host="127.0.0.1", port=3306, user="root", password="123456")


def main():
    # 1) 列出土/两端 perf 库
    def list_dbs(cfg):
        conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                               password=cfg["password"], charset="utf8mb4")
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW DATABASES")
                return [r[0] for r in cur.fetchall() if r[0].startswith("perf_db_")]
        finally:
            conn.close()

    for name, cfg in (("源端 13306", SRC), ("目标端 13307", TGT)):
        dbs = list_dbs(cfg)
        print(f"{name}: 待删除 {len(dbs)} 个 perf 库", flush=True)
        conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                               password=cfg["password"], charset="utf8mb4")
        try:
            with conn.cursor() as cur:
                for db in dbs:
                    cur.execute(f"DROP DATABASE IF EXISTS `{db}`")
            conn.commit()
        finally:
            conn.close()
        print(f"  ✅ {name} 已删除 {len(dbs)} 个库", flush=True)

    # 2) 元库清理 perf 水位/进度/binlog 信息
    conn = pymysql.connect(host=META["host"], port=META["port"], user=META["user"],
                           password=META["password"], database="drplatform", charset="utf8mb4")
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sync_watermark WHERE SOURCE_DB LIKE %s", ("perf_db_%",))
            print(f"元库 sync_watermark: 删除 {cur.rowcount} 行", flush=True)
            cur.execute("DELETE FROM sync_progress WHERE SOURCE_DB_NAME LIKE %s", ("perf_db_%",))
            print(f"元库 sync_progress: 删除 {cur.rowcount} 行", flush=True)
            try:
                cur.execute("DELETE FROM source_latest_binlog_info WHERE SOURCE_DB LIKE %s",
                            ("perf_db_%",))
                print(f"元库 source_latest_binlog_info: 删除 {cur.rowcount} 行", flush=True)
            except pymysql.err.Error as e:
                print(f"  source_latest_binlog_info 跳过: {str(e)[:80]}", flush=True)
        conn.commit()
    finally:
        conn.close()

    # 3) 复查
    for name, cfg in (("源端 13306", SRC), ("目标端 13307", TGT)):
        left = list_dbs(cfg)
        print(f"复查 {name}: 剩余 perf 库 {len(left)}", flush=True)
    conn = pymysql.connect(host=META["host"], port=META["port"], user=META["user"],
                           password=META["password"], database="drplatform", charset="utf8mb4")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sync_watermark WHERE SOURCE_DB LIKE %s", ("perf_db_%",))
        print(f"复查元库 perf 水位行: {cur.fetchone()[0]}", flush=True)
    conn.close()
    print("✅ 压测环境清理完成", flush=True)


if __name__ == "__main__":
    main()
