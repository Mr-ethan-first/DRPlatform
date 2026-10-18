# -*- coding: utf-8 -*-
"""诊断：全表级 COUNT 对比，定位风暴后源/目标行数差异的具体表与行性质。

步骤：
  1. 每库一条 UNION ALL 查询拿 67 张表 COUNT（源/目标各 45 条查询）
  2. 对比列出差异表（src != tgt）
  3. 对差异表统计 S17% 风暴行数（判定缺失的是风暴行还是原始行）
  4. 对差异表对比 MAX(update_time)（判定是否为滞后）
  5. 汇总：差异行总数、风暴行差异、目标多出的行（删除未同步）
"""
import sys
import time
from datetime import datetime

sys.path.insert(0, r"D:\WorkSpace\flink-cdc-sync\tests")
from run_perf_45db import SRC, TGT, DBS, TABLES_PER_DB, DB_PREFIX, db_query  # noqa: E402


def counts_per_db(side, db):
    """一条 UNION ALL 查询返回 {table: count}。"""
    parts = []
    for t in range(TABLES_PER_DB):
        tbl = f"perf_t_{t:03d}"
        parts.append(f"SELECT '{tbl}' AS tbl, COUNT(*) AS c FROM `{db}`.`{tbl}`")
    sql = "\nUNION ALL\n".join(parts)
    rows = db_query(side, sql, None)
    return {r["tbl"]: int(r["c"]) for r in rows}


def storm_count(side, db, tbl, col):
    sql = f"SELECT COUNT(*) AS c FROM `{db}`.`{tbl}` WHERE `{col}` LIKE 'S17%'"
    return int(db_query(side, sql, None)[0]["c"])


def max_ut(side, db, tbl):
    try:
        r = db_query(side, f"SELECT MAX(update_time) AS m FROM `{db}`.`{tbl}`", None)
        return r[0]["m"] if r else None
    except Exception:
        return None  # 无 update_time 列


def biz_col(db, tbl):
    cols = [r["COLUMN_NAME"] for r in db_query(
        SRC, "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
        None, (db, tbl))]
    biz = [c for c in cols if c not in ("id", "update_time")]
    return biz[0] if biz else None


def main():
    t0 = time.time()
    print(f"=== 全表 COUNT 对比开始 {datetime.now()} ===", flush=True)
    diff_tables = []          # (db, tbl, src, tgt)
    tot_src = tot_tgt = 0
    for d in range(DBS):
        db = f"{DB_PREFIX}{d:02d}"
        try:
            cs = counts_per_db(SRC, db)
            ct = counts_per_db(TGT, db)
        except Exception as e:
            print(f"  ❌ {db} 查询失败: {str(e)[:150]}", flush=True)
            continue
        for tbl in sorted(cs):
            tot_src += cs[tbl]
            tot_tgt += ct.get(tbl, -1)
            if cs[tbl] != ct.get(tbl, -1):
                diff_tables.append((db, tbl, cs[tbl], ct.get(tbl, -1)))
        print(f"  {db}: src={sum(cs.values())} tgt={sum(ct.values())} "
              f"diff_tables_so_far={len(diff_tables)}", flush=True)
    print(f"\n=== 总计: 源 {tot_src} 行 / 目标 {tot_tgt} 行 / 差 {tot_src - tot_tgt} ===", flush=True)
    print(f"=== 差异表数: {len(diff_tables)} / {DBS * TABLES_PER_DB} ===", flush=True)

    if not diff_tables:
        print("✅ 全部表行数一致", flush=True)
        return

    # 汇总差异方向
    tgt_less = [(d, t, s, g) for d, t, s, g in diff_tables if g < s]
    tgt_more = [(d, t, s, g) for d, t, s, g in diff_tables if g > s]
    print(f"目标偏少: {len(tgt_less)} 张, 合计少 {sum(s - g for _, _, s, g in tgt_less)} 行", flush=True)
    print(f"目标偏多: {len(tgt_more)} 张, 合计多 {sum(g - s for _, _, s, g in tgt_more)} 行", flush=True)

    # 逐差异表深入分析（上限 40 张，避免输出爆炸）
    print("\n=== 差异表明细（前 40 张）===", flush=True)
    storm_src = storm_tgt = 0
    lag_tables = 0
    for db, tbl, s, g in diff_tables[:40]:
        col = biz_col(db, tbl)
        ss = storm_count(SRC, db, tbl, col) if col else -1
        st = storm_count(TGT, db, tbl, col) if col else -1
        storm_src += max(ss, 0)
        storm_tgt += max(st, 0)
        sm, tm = max_ut(SRC, db, tbl), max_ut(TGT, db, tbl)
        lag = "" if (sm is None or tm is None or sm == tm) else " ←目标滞后"
        if sm is not None and tm is not None and sm != tm:
            lag_tables += 1
        print(f"{db}.{tbl}: src={s} tgt={g} diff={s - g} | 风暴行 src={ss} tgt={st} "
              f"(差{ss - st}) | max_ut src={sm} tgt={tm}{lag}", flush=True)
    print(f"\n前 40 张差异表风暴行合计: src={storm_src} tgt={storm_tgt} 差={storm_src - storm_tgt}", flush=True)
    print(f"目标滞后(max_ut 落后)表数: {lag_tables}", flush=True)
    print(f"=== 诊断耗时 {time.time() - t0:.0f}s ===", flush=True)


if __name__ == "__main__":
    main()
