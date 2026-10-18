# -*- coding: utf-8 -*-
"""清理因时区错位滞留源端的风暴行（update_time 远低于水位的 S17% 污染行）。

背景（2026-10-17 压测根因）：
  - MySQL 容器时区为 UTC；gen_perf_data 的 uts 用 Python 本地时间（UTC+8）字面值；
  - 旧版风暴 INSERT 省略 update_time → 数据库填 CURRENT_TIMESTAMP（UTC 字面值，09:xx），
    比水位（本地字面值，15:56）早约 7 小时 → keyset 增量扫描永远看不到 → 368 张表各滞留 19 行。
  本脚本按「tag LIKE 'S17%' AND update_time < 阈值」精确删除这些污染行。
  双保险：原始行业务值（PO/DV/CD/k/v 前缀）不可能以 S17 开头；阈值仅防未来误伤。

用法：python cleanup_backdated.py
"""
import sys

sys.path.insert(0, r"D:\WorkSpace\flink-cdc-sync\tests")
from run_perf_45db import SRC, DBS, TABLES_PER_DB, DB_PREFIX, db_query, db_exec  # noqa: E402

# UTC 字面值的旧风暴行在 09:xx；本地字面值的原始行/新风暴行在 15:xx/17:xx → 12:00 是安全分界
CUTOFF = "2026-10-17 12:00:00"


def main():
    total, tables = 0, 0
    for d in range(DBS):
        db = f"{DB_PREFIX}{d:02d}"
        for t in range(TABLES_PER_DB):
            tbl = f"perf_t_{t:03d}"
            try:
                cols = [r["COLUMN_NAME"] for r in db_query(
                    SRC, "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
                    None, (db, tbl))]
                if "update_time" not in cols:
                    continue  # 无 update_time 的表走全量兜底，风暴行已正常同步，不清理
                biz = [c for c in cols if c not in ("id", "update_time")]
                if not biz:
                    continue
                col = biz[0]
                n = db_exec(SRC, f"DELETE FROM `{db}`.`{tbl}` "
                                 f"WHERE `{col}` LIKE %s AND `update_time` < %s",
                            None, ("S17%", CUTOFF))
                if n:
                    tables += 1
                    total += n
            except Exception as e:
                print(f"  skip {db}.{tbl}: {str(e)[:100]}", flush=True)
        print(f"  {db} done（累计 {tables} 表 / {total} 行）", flush=True)
    print(f"✅ 清理完成：{tables} 张表，删除 {total} 行滞留风暴数据", flush=True)


if __name__ == "__main__":
    main()
