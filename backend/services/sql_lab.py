"""SQL & indexing lab: REAL MySQL timings with and without an index, plus EXPLAIN / EXPLAIN ANALYZE.

Two identical tables are filled with the same rows; only `lab_orders_indexed` has secondary indexes.
Timings are wall-clock measurements on THIS MySQL server; they vary with caching and hardware, so the
dashboard reports medians over several repetitions."""
from __future__ import annotations

import json
import statistics
import time

from backend.database import connect

TABLES = ("lab_orders_plain", "lab_orders_indexed")


def setup_lab(rows: int = 100_000) -> dict:
    if not 1_000 <= rows <= 2_000_000:
        raise ValueError("rows must be between 1,000 and 2,000,000")
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("SET SESSION cte_max_recursion_depth = %s", (rows + 10,))
        for t in TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {t}")
            cur.execute(f"""CREATE TABLE {t} (
                order_id INT UNSIGNED NOT NULL PRIMARY KEY,
                customer_id INT UNSIGNED NOT NULL,
                status VARCHAR(12) NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                created_at DATE NOT NULL) ENGINE=InnoDB""")
        cur.execute(f"""INSERT INTO lab_orders_plain
            WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < %s)
            SELECT n, 1 + (n * 7919) % 5000,
                   ELT(1 + n % 4, 'NEW', 'PAID', 'SHIPPED', 'CANCELLED'),
                   ROUND(5 + (n * 31) % 995 + (n % 100) / 100, 2),
                   DATE_ADD('2024-01-01', INTERVAL n % 730 DAY) FROM seq""", (rows,))
        cur.execute("INSERT INTO lab_orders_indexed SELECT * FROM lab_orders_plain")
        cur.execute("SELECT COUNT(*) FROM lab_orders_indexed")
        n = cur.fetchone()[0]
        # Build the indexes AFTER the load so index build time can be measured honestly.
        t0 = time.perf_counter()
        cur.execute("CREATE INDEX idx_customer ON lab_orders_indexed (customer_id)")
        cur.execute("CREATE INDEX idx_status_date ON lab_orders_indexed (status, created_at)")
        build_ms = (time.perf_counter() - t0) * 1000
        for t in TABLES:
            cur.execute(f"ANALYZE TABLE {t}")
            cur.fetchall()
        conn.commit()
        return {"rows": n, "index_build_ms": build_ms}
    finally:
        conn.close()


QUERIES = {
    "point_lookup": ("WHERE customer_id = 1234", "SELECT COUNT(*), SUM(amount) FROM {t} WHERE customer_id = 1234"),
    "range_scan": ("WHERE status='PAID' AND created_at BETWEEN '2024-03-01' AND '2024-03-31'",
                   "SELECT COUNT(*) FROM {t} WHERE status = 'PAID' AND created_at BETWEEN '2024-03-01' AND '2024-03-31'"),
    "group_by": ("GROUP BY status", "SELECT status, COUNT(*) FROM {t} GROUP BY status"),
}


def _median_ms(cur, sql: str, reps: int) -> float:
    times = []
    for _ in range(reps):
        t0 = time.perf_counter()
        cur.execute(sql)
        cur.fetchall()
        times.append((time.perf_counter() - t0) * 1000)
    return statistics.median(times)


def _plan(cur, sql: str) -> tuple[str, str]:
    cur.execute("EXPLAIN FORMAT=TREE " + sql)
    tree = "\n".join(r[0] for r in cur.fetchall())
    try:
        cur.execute("EXPLAIN ANALYZE " + sql)
        analyzed = "\n".join(r[0] for r in cur.fetchall())
    except Exception as exc:
        analyzed = f"EXPLAIN ANALYZE unavailable: {exc}"
    return tree, analyzed


def run_index_experiment(reps: int = 7) -> dict:
    if not 1 <= reps <= 50:
        raise ValueError("reps must be between 1 and 50")
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM lab_orders_plain")
        rows = cur.fetchone()[0]
        out = {"rows": rows, "queries": []}
        for qname, (label, template) in QUERIES.items():
            entry = {"query": qname, "predicate": label}
            for t in TABLES:
                sql = template.format(t=t)
                _median_ms(cur, sql, 1)  # warm-up
                ms = _median_ms(cur, sql, reps)
                tree, analyzed = _plan(cur, sql)
                key = "indexed" if "indexed" in t else "plain"
                entry[key] = {"median_ms": ms, "plan": tree, "explain_analyze": analyzed}
            entry["speedup"] = entry["plain"]["median_ms"] / max(entry["indexed"]["median_ms"], 1e-6)
            out["queries"].append(entry)
        # write cost: bulk insert into both tables
        write = {}
        for t in TABLES:
            t0 = time.perf_counter()
            cur.execute(f"INSERT INTO {t} SELECT order_id + 10000000, customer_id, status, amount, created_at "
                        f"FROM {t} LIMIT 20000")
            write[t] = (time.perf_counter() - t0) * 1000
            cur.execute(f"DELETE FROM {t} WHERE order_id >= 10000000")
        out["insert_20k_ms"] = {"plain": write["lab_orders_plain"], "indexed": write["lab_orders_indexed"]}
        cur.execute("""SELECT table_name, ROUND((data_length)/1024/1024,2), ROUND((index_length)/1024/1024,2)
                       FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN (%s,%s)""", TABLES)
        out["size_mb"] = {r[0]: {"data": float(r[1]), "index": float(r[2])} for r in cur.fetchall()}
        conn.commit()
        return out
    finally:
        conn.close()


def run_and_save(rows: int = 100_000, reps: int = 7) -> dict:
    setup = setup_lab(rows)
    res = run_index_experiment(reps)
    res["index_build_ms"] = setup["index_build_ms"]
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO experiments (name, category, source, algorithm, workload, txn_count, parameters)"
                    " VALUES (%s,'sql_index','measured_mysql','index_vs_scan','lab_orders',%s,%s)",
                    (f"sql index lab / {res['rows']} rows", res["rows"], json.dumps({"reps": reps})))
        exp_id = cur.lastrowid
        for q in res["queries"]:
            for key in ("plain", "indexed"):
                cur.callproc("sp_record_metric", (exp_id, f"{q['query']}_{key}_ms", q[key]["median_ms"], "ms"))
                cur.execute("INSERT INTO transaction_logs (experiment_id, seq, event_type, detail) VALUES (%s,"
                            "(SELECT COALESCE(MAX(s.seq),0)+1 FROM transaction_logs s WHERE s.experiment_id=%s),'EXPLAIN',%s)",
                            (exp_id, exp_id, f"{q['query']} [{key}]\n{q[key]['plan']}"[:1000]))
            cur.callproc("sp_record_metric", (exp_id, f"{q['query']}_speedup", q["speedup"], "x"))
        for key, v in res["insert_20k_ms"].items():
            cur.callproc("sp_record_metric", (exp_id, f"insert20k_{key}_ms", v, "ms"))
        cur.callproc("sp_record_metric", (exp_id, "index_build_ms", res["index_build_ms"], "ms"))
        cur.execute("UPDATE experiments SET status='COMPLETED' WHERE experiment_id=%s", (exp_id,))
        conn.commit()
        res["experiment_id"] = exp_id
    finally:
        conn.close()
    return res
