"""Read-side queries used by the dashboard (all parameterised)."""
from __future__ import annotations

import pandas as pd

from backend.database import query


def _df(sql: str, params: tuple = ()) -> pd.DataFrame:
    return pd.DataFrame(query(sql, params))


def history(category: str | None = None, source: str | None = None, algorithm: str | None = None,
            include_seed: bool = False, limit: int = 500) -> pd.DataFrame:
    sql = "SELECT * FROM v_experiment_summary WHERE 1=1"
    params: list = []
    if category:
        sql += " AND category = %s"; params.append(category)
    if source:
        sql += " AND source = %s"; params.append(source)
    if algorithm:
        sql += " AND algorithm = %s"; params.append(algorithm)
    if not include_seed:
        sql += " AND is_seed = 0"
    sql += " ORDER BY experiment_id DESC LIMIT %s"
    params.append(int(limit))
    return _df(sql, tuple(params))


def metrics_for(exp_ids: list[int]) -> pd.DataFrame:
    if not exp_ids:
        return pd.DataFrame(columns=["experiment_id", "algorithm", "metric_name", "metric_value", "unit"])
    marks = ",".join(["%s"] * len(exp_ids))
    return _df(f"SELECT e.experiment_id, e.algorithm, m.metric_name, m.metric_value, m.unit FROM experiment_metrics m "
               f"JOIN experiments e ON e.experiment_id = m.experiment_id WHERE m.experiment_id IN ({marks})", tuple(exp_ids))


def transaction_runs() -> pd.DataFrame:
    """Completed real-engine transaction runs (no seeds)."""
    return _df("SELECT experiment_id, algorithm, workload, workers, txn_count, invariant_ok, throughput_tps, elapsed_ms, "
               "committed, aborted_attempts, created_at FROM v_experiment_summary "
               "WHERE category='transaction' AND status='COMPLETED' AND is_seed=0 ORDER BY experiment_id")


def latency_percentiles(exp_ids: list[int]) -> pd.DataFrame:
    if not exp_ids:
        return pd.DataFrame()
    marks = ",".join(["%s"] * len(exp_ids))
    return _df(f"SELECT e.experiment_id, e.algorithm, e.workers, m.metric_name, m.metric_value FROM experiment_metrics m "
               f"JOIN experiments e ON e.experiment_id=m.experiment_id WHERE m.experiment_id IN ({marks}) "
               f"AND m.metric_name IN ('avg_latency_us','p50_latency_us','p95_latency_us','p99_latency_us')", tuple(exp_ids))


def txn_samples(exp_id: int) -> pd.DataFrame:
    return _df("SELECT txn_ref, attempts, latency_us, reads_done, writes_done FROM transactions WHERE experiment_id=%s", (exp_id,))


def logs_for(exp_id: int) -> pd.DataFrame:
    return _df("SELECT seq, txn_ref, event_type, detail, passed FROM transaction_logs WHERE experiment_id=%s ORDER BY seq", (exp_id,))


def jobs_for(exp_id: int) -> pd.DataFrame:
    return _df("SELECT job_id, arrival, burst, priority, completion, turnaround, waiting, response "
               "FROM job_results WHERE experiment_id=%s ORDER BY job_id", (exp_id,))


def experiment_row(exp_id: int) -> dict | None:
    rows = query("SELECT * FROM experiments WHERE experiment_id=%s", (exp_id,))
    return rows[0] if rows else None
