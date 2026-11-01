"""Run engine/simulator experiments and persist them (experiments + children) in MySQL."""
from __future__ import annotations

import json
import tempfile
from typing import Any

from backend.database import connect
from backend.services.engine import run_cli

BENCH_METRICS = {
    "throughput_tps": "txn/s", "elapsed_ms": "ms", "committed": "txn", "aborted_attempts": "attempts",
    "aborted_wait_die": "attempts", "aborted_no_wait": "attempts", "aborted_deadlock": "attempts",
    "lock_waits": "waits", "abort_rate": "ratio", "avg_latency_us": "us", "p50_latency_us": "us",
    "p95_latency_us": "us", "p99_latency_us": "us", "deadlock_cycles_detected": "cycles", "gave_up": "txn",
}


def _create(cur, name, category, source, algorithm, workload=None, workers=None, txn_count=None, params=None) -> int:
    cur.execute(
        "INSERT INTO experiments (name, category, source, algorithm, workload, workers, txn_count, parameters)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        (name, category, source, algorithm, workload, workers, txn_count, json.dumps(params or {})),
    )
    return cur.lastrowid


def _finish(cur, exp_id: int, ok: bool | None, error: str | None = None) -> None:
    cur.execute(
        "UPDATE experiments SET status=%s, invariant_ok=%s, error_message=%s WHERE experiment_id=%s",
        ("FAILED" if error else "COMPLETED", None if ok is None else int(ok), (error or None) and error[:500], exp_id),
    )


def _metrics(cur, exp_id: int, items: dict[str, tuple[float, str]]) -> None:
    for name, (value, unit) in items.items():
        cur.callproc("sp_record_metric", (exp_id, name, float(value), unit))


def _tx(fn):
    """Run fn(cursor) in one DB transaction; the experiment row is FAILED-marked on error."""
    conn = connect()
    cur = conn.cursor(dictionary=True)
    try:
        result = fn(cur)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def run_transaction_experiment(policy="wait_die", workload="ycsb", workers=4, txns=2000, records=1000,
                               ops=8, write_ratio=0.5, theta=0.6, ordered=0, seed=42, wal=False,
                               sync=False, think_us=0, name=None) -> dict[str, Any]:
    params = dict(policy=policy, workload=workload, workers=workers, txns=txns, records=records, ops=ops,
                  write_ratio=write_ratio, theta=theta, ordered=ordered, seed=seed, wal=wal, sync=int(sync),
                  think_us=think_us)
    with tempfile.TemporaryDirectory() as d:
        result = run_cli("bench", policy=policy, workload=workload, workers=workers, txns=txns, records=records,
                         ops=ops, write_ratio=write_ratio, theta=theta, ordered=ordered, seed=seed,
                         sync=int(sync), think_us=think_us, dir=d if wal else None)

    def save(cur):
        exp_id = _create(cur, name or f"{policy} / {workload} / {workers}w", "transaction", "measured_engine",
                         policy, workload, workers, txns, params)
        _metrics(cur, exp_id, {k: (result[k], u) for k, u in BENCH_METRICS.items()})
        for i, s in enumerate(result["txn_samples"]):
            cur.execute("INSERT INTO transactions (experiment_id, txn_ref, attempts, latency_us, reads_done, writes_done)"
                        " VALUES (%s,%s,%s,%s,%s,%s)",
                        (exp_id, s["txn_ref"], s["attempts"], s["latency_us"], s["reads"], s["writes"]))
        cur.execute("INSERT INTO transaction_logs (experiment_id, seq, event_type, detail, passed) VALUES (%s,1,'INVARIANT_CHECK',%s,%s)",
                    (exp_id, result["invariant_detail"][:1000], int(result["invariant_ok"])))
        _finish(cur, exp_id, result["invariant_ok"])
        return exp_id

    result["experiment_id"] = _tx(save)
    return result


def run_deadlock_experiment(policy="detect") -> dict[str, Any]:
    result = run_cli("deadlock", policy=policy)

    def save(cur):
        exp_id = _create(cur, f"deadlock demo / {policy}", "deadlock", "measured_engine", policy, params={"policy": policy})
        _metrics(cur, exp_id, {
            "cycle_detected": (result["cycle_detected"], "bool"), "aborted_wait_die": (result["aborted_wait_die"], "attempts"),
            "aborted_no_wait": (result["aborted_no_wait"], "attempts"), "aborted_deadlock": (result["aborted_deadlock"], "attempts"),
            "t1_attempts": (result["t1_attempts"], "attempts"), "t2_attempts": (result["t2_attempts"], "attempts"),
        })
        for e in result["events"]:
            cur.execute("INSERT INTO transaction_logs (experiment_id, seq, txn_ref, event_type, detail) VALUES (%s,%s,%s,%s,%s)",
                        (exp_id, e["seq"], e["txn"], e["event"], e["detail"][:1000]))
        _finish(cur, exp_id, result["consistent"])
        return exp_id

    result["experiment_id"] = _tx(save)
    return result


def run_recovery_experiment(scenario="crash") -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as d:
        result = run_cli("recovery", dir=d + "/db", scenario=scenario)

    def save(cur):
        exp_id = _create(cur, f"recovery / {scenario}", "recovery", "measured_engine", scenario, params={"scenario": scenario})
        r = result["report"]
        _metrics(cur, exp_id, {k: (r[k], "count") for k in ("records_scanned", "redo_applied", "undo_applied", "committed_txns", "losers")})
        for i, c in enumerate(result["checks"], 1):
            cur.execute("INSERT INTO transaction_logs (experiment_id, seq, event_type, detail, passed) VALUES (%s,%s,'RECOVERY_CHECK',%s,%s)",
                        (exp_id, i, f"{c['name']}: expected {c['expected']}, got {c['actual']}"[:1000], int(c["pass"])))
        _finish(cur, exp_id, result["all_pass"])
        return exp_id

    result["experiment_id"] = _tx(save)
    return result


def run_scheduling_experiment(algos="all", quantum=4, txn_jobs=12, seed=42, spread=20, jobs=None) -> dict[str, Any]:
    result = run_cli("sched", algos=algos, quantum=quantum, txn_jobs=None if jobs else txn_jobs, seed=seed, spread=spread, jobs=jobs)
    ids = []

    def save(cur):
        for r in result["results"]:
            exp_id = _create(cur, f"scheduling / {r['algorithm']}", "scheduling", "simulator", r["algorithm"],
                             "transaction_jobs" if not jobs else "explicit", txn_count=len(r["jobs"]),
                             params={"quantum": quantum, "seed": seed, "spread": spread, "job_source": result["job_source"]})
            _metrics(cur, exp_id, {k: (r[k], "time units") for k in ("avg_waiting", "avg_turnaround", "avg_response", "avg_completion", "makespan")}
                     | {"cpu_utilization": (r["cpu_utilization"], "ratio"), "context_switches": (r["context_switches"], "count")})
            for j in r["jobs"]:
                cur.execute("INSERT INTO job_results (experiment_id, job_id, arrival, burst, priority, completion, turnaround, waiting, response)"
                            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (exp_id, j["id"], j["arrival"], j["burst"], j["priority"], j["completion"], j["turnaround"], max(j["waiting"], 0), j["response"]))
            _finish(cur, exp_id, None)
            ids.append(exp_id)

    _tx(save)
    result["experiment_ids"] = ids
    return result


def _run_sim_batch(command: str, category: str, key: str, label_key: str, metrics: dict, **options) -> dict[str, Any]:
    result = run_cli(command, **options)
    ids = []

    def save(cur):
        for r in result["results"]:
            label = r[label_key]
            exp_id = _create(cur, f"{category} / {label}", category, "simulator", label, params=options)
            _metrics(cur, exp_id, {m: (r[m], u) for m, u in metrics.items()})
            _finish(cur, exp_id, None)
            ids.append(exp_id)

    _tx(save)
    result["experiment_ids"] = ids
    return result


def run_disk_experiment(requests="98,183,37,122,14,124,65,67", head=53, size=200, direction="up") -> dict[str, Any]:
    return _run_sim_batch("disk", "disk", "algorithm", "algorithm",
                          {"total_movement": "cylinders", "return_jump": "cylinders", "avg_movement_per_request": "cylinders"},
                          requests=requests, head=head, size=size, direction=direction)


def run_allocation_experiment(blocks=32, ops="create:A:5,create:B:3,create:C:6,remove:B,create:D:4,create:E:5") -> dict[str, Any]:
    return _run_sim_batch("alloc", "allocation", "method", "method",
                          {"used_blocks": "blocks", "free_blocks": "blocks", "overhead_blocks": "blocks", "failed_creates": "count",
                           "external_fragmentation_pct": "%", "avg_blocks_read_to_reach_last_block": "reads"},
                          blocks=blocks, ops=ops)


def run_os_deadlock_experiment(mode="safety", **options) -> dict[str, Any]:
    result = run_cli("osdeadlock", mode=mode, **options)

    def save(cur):
        exp_id = _create(cur, f"os deadlock / {mode}", "os_deadlock", "simulator", mode, params=options)
        flag = result.get("safe", result.get("granted", result.get("deadlocked")))
        _metrics(cur, exp_id, {"result_flag": (1 if flag else 0, "bool")})
        cur.execute("INSERT INTO transaction_logs (experiment_id, seq, event_type, detail) VALUES (%s,1,'OS_DEADLOCK',%s)",
                    (exp_id, json.dumps({k: v for k, v in result.items() if k != "_exit_code"})[:1000]))
        _finish(cur, exp_id, None)
        return exp_id

    result["experiment_id"] = _tx(save)
    return result


PIPELINE_METRICS = {
    "throughput_tps": "txn/s", "elapsed_ms": "ms", "committed": "txn", "aborted_attempts": "attempts",
    "aborted_wait_die": "attempts", "aborted_no_wait": "attempts", "aborted_deadlock": "attempts", "lock_waits": "waits",
    "deadlock_cycles_detected": "cycles", "gave_up": "txn", "transfers_committed": "txn", "audits_committed": "txn",
    "audit_violations": "count", "checkpoints": "count", "wal_bytes": "bytes", "recovery_redo": "count",
    "recovery_undo": "count", "recovery_losers": "count", "recovery_committed": "count",
    "avg_latency_us": "us", "p50_latency_us": "us", "p95_latency_us": "us", "p99_latency_us": "us",
    "sim_avg_waiting": "sim time units", "sim_avg_turnaround": "sim time units", "sim_avg_response": "sim time units",
}


def run_pipeline_experiment(policy="wait_die", admission="FCFS", txns=1000, workers=8, accounts=100, theta=0.8,
                            wal=True, sync=False, checkpoint_ms=20, crash_after=0, seed=42, queue=256, quantum=4,
                            name=None) -> dict[str, Any]:
    """All components together on one shared database (see engine/pipeline.h). Normal runs are saved as
    category 'transaction' (workload 'pipeline'); crash runs as category 'recovery' (workload 'pipeline-crash')."""
    params = dict(policy=policy, admission=admission, txns=txns, workers=workers, accounts=accounts, theta=theta, wal=wal,
                  sync=int(sync), checkpoint_ms=checkpoint_ms, crash_after=crash_after, seed=seed, queue=queue, quantum=quantum)
    with tempfile.TemporaryDirectory() as d:
        result = run_cli("pipeline", timeout=600, policy=policy, admission=admission, txns=txns, workers=workers,
                         accounts=accounts, theta=theta, wal=int(wal), sync=int(sync), checkpoint_ms=checkpoint_ms,
                         crash_after=crash_after, seed=seed, queue=queue, quantum=quantum, dir=(d + "/db") if wal else None)
    crashed = result["crash_injected"]

    def save(cur):
        exp_id = _create(cur, name or f"pipeline {policy}/{admission} {txns}tx {workers}w" + (" +crash" if crashed else ""),
                         "recovery" if crashed else "transaction", "measured_engine", policy,
                         "pipeline-crash" if crashed else "pipeline", workers, txns, params)
        skip = {"throughput_tps", "avg_latency_us", "p50_latency_us", "p95_latency_us", "p99_latency_us"} if crashed else set()
        _metrics(cur, exp_id, {k: (result[k], u) for k, u in PIPELINE_METRICS.items() if k not in skip})
        for s in result["txn_samples"]:
            cur.execute("INSERT INTO transactions (experiment_id, txn_ref, attempts, latency_us, reads_done, writes_done)"
                        " VALUES (%s,%s,%s,%s,%s,%s)", (exp_id, s["txn_ref"], s["attempts"], s["latency_us"], s["reads"], s["writes"]))
        seq = 0
        for s in result["stages"]:
            seq += 1
            cur.execute("INSERT INTO transaction_logs (experiment_id, seq, event_type, detail) VALUES (%s,%s,%s,%s)",
                        (exp_id, seq, s["stage"], f"[{s['t_ms']:.1f} ms] {s['detail']}"[:1000]))
        for c in result["checks"]:
            seq += 1
            cur.execute("INSERT INTO transaction_logs (experiment_id, seq, event_type, detail, passed) VALUES (%s,%s,'PIPELINE_CHECK',%s,%s)",
                        (exp_id, seq, f"{c['name']}: expected {c['expected']}, got {c['actual']}"[:1000], int(c["pass"])))
        _finish(cur, exp_id, result["all_pass"])
        return exp_id

    result["experiment_id"] = _tx(save)
    return result
