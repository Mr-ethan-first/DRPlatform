# -*- coding: utf-8 -*-
"""诊断：定位有风暴数据的表，对比源/目标行级差异。"""
import sys

sys.path.insert(0, r"D:\WorkSpace\flink-cdc-sync\tests")
from run_perf_45db import SRC, TGT, DBS, TABLES_PER_DB, DB_PREFIX, db_query  # noqa: E402


def main():
    for d in range(DBS):
        db = f"{DB_PREFIX}{d:02d}"
        for t in range(TABLES_PER_DB):
            tbl = f"perf_t_{t:03d}"
            try:
                cols = [r["COLUMN_NAME"] for r in db_query(
                    SRC, "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
                    None, (db, tbl))]
                biz = [c for c in cols if c not in ("id", "update_time")]
                if not biz:
                    continue
                col = biz[0]
                sql = (f"SELECT COUNT(*) AS c FROM `{db}`.`{tbl}` "
                       f"WHERE `{col}` LIKE 'S17%'")
                n = int(db_query(SRC, sql, None)[0]["c"])
                if n > 0:
                    nt = int(db_query(TGT, sql, None)[0]["c"])
                    print(f"{db}.{tbl} (col={col}): src={n} tgt={nt} diff={n - nt}", flush=True)
                    # 列出源端有目标端没有的 tag（最多 5 个）
                    miss = db_query(
                        SRC, f"SELECT `{col}` AS v FROM `{db}`.`{tbl}` "
                        f"WHERE `{col}` LIKE 'S17%' ORDER BY id DESC LIMIT 5", None)
                    tgt_vals = {r["v"] for r in db_query(
                        TGT, f"SELECT `{col}` AS v FROM `{db}`.`{tbl}` "
                        f"WHERE `{col}` LIKE 'S17%'", None)}
                    for r in miss:
                        mark = "MISSING" if r["v"] not in tgt_vals else "ok"
                        print(f"    {mark}: {r['v']}", flush=True)
                    # 对比源/目标最大 update_time
                    sm = db_query(SRC, f"SELECT MAX(update_time) AS m FROM `{db}`.`{tbl}`", None)[0]["m"]
                    tm = db_query(TGT, f"SELECT MAX(update_time) AS m FROM `{db}`.`{tbl}`", None)[0]["m"]
                    print(f"    src max update_time={sm}  tgt max update_time={tm}", flush=True)
                    return
            except Exception as e:
                print(f"  skip {db}.{tbl}: {str(e)[:80]}", flush=True)
    print("未找到有风暴数据的表", flush=True)


if __name__ == "__main__":
    main()
