"""Isolation & serializability lab.

* Engine runs: the C++ engine replays the four anomaly scenarios (dirty read, non-repeatable read,
  lost update, phantom) at RU / RC / RR / SER with LOCKING isolation, and returns a step timeline,
  the executed history and its precedence-graph analysis.
* MySQL runs: the SAME scenarios on real InnoDB with two connections (two "users"). InnoDB uses
  MVCC snapshots for plain SELECTs at RC/RR, so its matrix differs from the textbook locking matrix
  (e.g. RR prevents phantoms for plain reads but allows a lost update) - that contrast is the point.
* Schedule checker / live runs: precedence graph, conflict + view serializability, recoverability.

Everything is saved to MySQL (experiments, anomaly_results, transaction_logs, experiment_metrics).
"""
from __future__ import annotations

import queue
import threading
import time
from typing import Any

from backend.database import connect
from backend.services.engine import run_cli
from backend.services.experiments import _create, _finish, _metrics, _tx

SCENARIOS = ["dirty_read", "non_repeatable_read", "lost_update", "phantom"]
SCENARIO_TITLES = {"dirty_read": "Dirty read", "non_repeatable_read": "Non-repeatable read",
                   "lost_update": "Lost update", "phantom": "Phantom"}
LEVELS = ["RU", "RC", "RR", "SER"]
LEVEL_NAMES = {"RU": "READ UNCOMMITTED", "RC": "READ COMMITTED", "RR": "REPEATABLE READ", "SER": "SERIALIZABLE"}
# The SQL-92 table for LOCKING implementations (what our engine must reproduce). True = anomaly possible.
TEXTBOOK = {"dirty_read": [True, False, False, False], "non_repeatable_read": [True, True, False, False],
            "lost_update": [True, True, False, False], "phantom": [True, True, True, False]}

EXAMPLE_SCHEDULES = {
    "Not serializable (classic cycle)": "R1(A) W2(A) R2(B) W1(B) C1 C2",
    "Lost update": "R1(A) R2(A) W1(A) W2(A) C1 C2",
    "Serializable (equivalent to T1, T2)": "R1(A) W1(A) R2(A) W2(A) R1(B) W1(B) R2(B) W2(B) C1 C2",
    "View- but not conflict-serializable (blind writes)": "R1(A) W2(A) W1(A) W3(A) C1 C2 C3",
    "Three transactions, serial order T2 → T1 → T3": "W2(A) R1(A) W1(B) R3(B) C2 C1 C3",
    "Dirty read, NOT recoverable": "W1(A) R2(A) C2 A1",
    "Recoverable but cascading abort possible": "W1(A) R2(A) C1 C2",
    "Strict": "W1(A) C1 R2(A) W2(A) C2",
}


def _check_level(level: str) -> None:
    if level not in LEVELS and level != "all":
        raise ValueError(f"isolation level must be one of {LEVELS} or 'all'")


def _check_scenario(scenario: str) -> None:
    if scenario not in SCENARIOS and scenario != "all":
        raise ValueError(f"scenario must be one of {SCENARIOS} or 'all'")


# ----------------------------------------------------------------------------- engine anomaly lab

def run_engine_anomalies(scenario: str = "all", level: str = "all", policy: str = "detect") -> dict[str, Any]:
    _check_scenario(scenario)
    _check_level(level)
    res = run_cli("isolation", scenario=scenario, level=level, policy=policy, timeout=120)
    return res


# ----------------------------------------------------------------------------- MySQL anomaly lab

MYSQL_TABLE = "lab_isolation_accounts"


def _mysql_steps(scenario: str) -> tuple[dict[str, int], list[tuple[int, str, str, Any]]]:
    """(initial rows, steps). A step is (txn, kind, label, arg)."""
    if scenario == "dirty_read":
        return {"A": 100}, [(1, "set", "W(A = 150)", ("A", 150)), (2, "read", "R(A)", "A"),
                            (1, "rollback", "ROLLBACK", None), (2, "commit", "COMMIT", None)]
    if scenario == "non_repeatable_read":
        return {"A": 100}, [(1, "read", "R(A)", "A"), (2, "set", "W(A = 150)", ("A", 150)), (2, "commit", "COMMIT", None),
                            (1, "read", "R(A)", "A"), (1, "commit", "COMMIT", None)]
    if scenario == "lost_update":
        return {"A": 100}, [(1, "read", "R(A)", "A"), (2, "read", "R(A)", "A"), (1, "add", "W(A = read + 10)", ("A", 10)),
                            (2, "add", "W(A = read + 20)", ("A", 20)), (1, "commit", "COMMIT", None), (2, "commit", "COMMIT", None)]
    if scenario == "phantom":
        return {"A": 100, "B": 100, "C": 100, "D": 100, "E": 50}, [
            (1, "count", "COUNT(balance >= 100)", 100), (2, "set", "W(E = 200)", ("E", 200)), (2, "commit", "COMMIT", None),
            (1, "count", "COUNT(balance >= 100)", 100), (1, "commit", "COMMIT", None)]
    raise ValueError(f"unknown scenario {scenario}")


class _Session(threading.Thread):
    """One MySQL connection = one user. Executes its steps in order on its own thread."""

    def __init__(self, txn: int, level: str, clock, lock_wait_timeout: int):
        super().__init__(daemon=True)
        self.txn, self.level, self.clock = txn, level, clock
        self.q: queue.Queue = queue.Queue()
        self.events: dict[int, dict] = {}
        self.done = {}
        self.last_read: dict[str, int] = {}
        self.state = "active"
        self.conn = connect()
        self.cur = self.conn.cursor()
        self.cur.execute(f"SET SESSION innodb_lock_wait_timeout = {int(lock_wait_timeout)}")
        self.cur.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {LEVEL_NAMES[level]}")
        self.cur.execute("START TRANSACTION")

    def run(self):
        while True:
            item = self.q.get()
            if item is None:
                break
            idx, kind, arg = item
            ev = self.events[idx]
            try:
                if self.state != "active":
                    ev.update(status="skipped", note="transaction was already rolled back")
                elif kind == "read":
                    self.cur.execute(f"SELECT balance FROM {MYSQL_TABLE} WHERE id = %s", (arg,))
                    v = int(self.cur.fetchone()[0])
                    self.last_read[arg] = v
                    ev.update(status="done", value=v, note=f"read {v}")
                elif kind == "set":
                    item_, v = arg
                    self.cur.execute(f"UPDATE {MYSQL_TABLE} SET balance = %s WHERE id = %s", (v, item_))
                    ev.update(status="done", value=v, note=f"wrote {v}")
                elif kind == "add":  # application computes the new value from ITS last read (read-modify-write)
                    item_, delta = arg
                    base = self.last_read.get(item_, 0)
                    v = base + delta
                    self.cur.execute(f"UPDATE {MYSQL_TABLE} SET balance = %s WHERE id = %s", (v, item_))
                    ev.update(status="done", value=v, note=f"wrote {base} + {delta} = {v}")
                elif kind == "count":
                    self.cur.execute(f"SELECT COUNT(*) FROM {MYSQL_TABLE} WHERE balance >= %s", (arg,))
                    v = int(self.cur.fetchone()[0])
                    ev.update(status="done", value=v, note=f"count = {v}")
                elif kind == "commit":
                    self.conn.commit()
                    self.state = "committed"
                    ev.update(status="done", note="committed")
                elif kind == "rollback":
                    self.conn.rollback()
                    self.state = "rolled_back"
                    ev.update(status="done", note="rolled back by the application")
            except Exception as exc:  # 1213 deadlock victim, 1205 lock-wait timeout
                errno = getattr(exc, "errno", None)
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                self.state = "rolled_back"
                reason = {1213: "deadlock victim: InnoDB rolled the transaction back",
                          1205: "lock wait timeout: transaction rolled back"}.get(errno, f"error: {exc}")
                ev.update(status="aborted", note=reason)
            ev["finished_tick"] = self.clock()
            self.done[idx] = True

    def close(self):
        try:
            if self.state == "active":
                self.conn.rollback()
        finally:
            self.cur.close()
            self.conn.close()


def run_mysql_anomaly(scenario: str, level: str, block_detect_s: float = 0.4, lock_wait_timeout: int = 5) -> dict[str, Any]:
    if scenario not in SCENARIOS:
        raise ValueError(f"scenario must be one of {SCENARIOS}")
    if level not in LEVELS:
        raise ValueError(f"isolation level must be one of {LEVELS}")
    initial, steps = _mysql_steps(scenario)
    conn = connect()  # setup (DDL commits implicitly, so it happens before the two sessions start)
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {MYSQL_TABLE}")
        cur.execute(f"CREATE TABLE {MYSQL_TABLE} (id CHAR(1) PRIMARY KEY, balance INT NOT NULL, INDEX idx_balance (balance)) ENGINE=InnoDB")
        cur.executemany(f"INSERT INTO {MYSQL_TABLE} VALUES (%s, %s)", list(initial.items()))
        conn.commit()
    finally:
        conn.close()

    tick = [0]
    tick_lock = threading.Lock()

    def clock():
        with tick_lock:
            tick[0] += 1
            return tick[0]

    sessions = {1: _Session(1, level, clock, lock_wait_timeout), 2: _Session(2, level, clock, lock_wait_timeout)}
    for s in sessions.values():
        s.start()
    events = []
    try:
        for i, (txn, kind, label, arg) in enumerate(steps):
            s = sessions[txn]
            behind = any(not s.done.get(j) for j in s.events)  # an earlier step of this user is still blocked
            ev = {"step": i + 1, "txn": txn, "label": label, "status": "pending", "note": "", "value": 0,
                  "waited": False, "issued_tick": clock(), "finished_tick": 0}
            s.events[i] = ev
            events.append(ev)
            s.q.put((i, kind, arg))
            if behind:
                ev["waited"] = True
                continue
            deadline = time.monotonic() + block_detect_s
            while time.monotonic() < deadline and not s.done.get(i):
                time.sleep(0.005)
            if not s.done.get(i):
                ev["waited"] = True  # still running after the grace period => blocked on a lock
        for s in sessions.values():
            s.q.put(None)
        for s in sessions.values():
            s.join(timeout=lock_wait_timeout * 3 + 5)
    finally:
        for s in sessions.values():
            s.close()

    for ev in events:
        if ev["waited"] and ev["status"] == "done":
            ev["status"] = "blocked_then_done"
            ev["note"] += " (had to wait)"

    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT id, balance FROM {MYSQL_TABLE} ORDER BY id")
        final = {r[0]: int(r[1]) for r in cur.fetchall()}
    finally:
        conn.close()

    ok = lambda i: events[i]["status"] in ("done", "blocked_then_done")  # noqa: E731
    t1c, t2c = sessions[1].state == "committed", sessions[2].state == "committed"
    if scenario == "dirty_read":
        anomaly = ok(1) and events[1]["value"] == 150
    elif scenario in ("non_repeatable_read", "phantom"):
        anomaly = ok(0) and ok(3) and events[0]["value"] != events[3]["value"]
    else:
        anomaly = t1c and t2c and final["A"] != 130
    victim = next((e for e in events if e["status"] == "aborted"), None)
    if anomaly:
        verdict = f"ANOMALY in MySQL at {LEVEL_NAMES[level]}."
    elif victim:
        verdict = f"Prevented in MySQL at {LEVEL_NAMES[level]}: T{victim['txn']} was rolled back ({victim['note']})."
    elif any(e["waited"] for e in events):
        verdict = f"Prevented in MySQL at {LEVEL_NAMES[level]} by LOCKING (a step had to wait)."
    else:
        verdict = (f"Prevented in MySQL at {LEVEL_NAMES[level]} WITHOUT waiting: InnoDB answered the read from an "
                   "MVCC snapshot (consistent read), not by locking.")
    return {"scenario": scenario, "title": SCENARIO_TITLES[scenario], "isolation": level,
            "isolation_long": LEVEL_NAMES[level], "engine": "mysql", "anomaly": bool(anomaly), "verdict": verdict,
            "t1_committed": t1c, "t2_committed": t2c, "events": events, "items": list(initial),
            "initial_values": list(initial.values()), "final_values": [final[k] for k in initial],
            "lock_waits": sum(1 for e in events if e["waited"])}


def run_mysql_anomalies(scenario: str = "all", level: str = "all") -> dict[str, Any]:
    _check_scenario(scenario)
    _check_level(level)
    scs = SCENARIOS if scenario == "all" else [scenario]
    lvs = LEVELS if level == "all" else [level]
    return {"kind": "measured_mysql", "runs": [run_mysql_anomaly(s, l) for s in scs for l in lvs]}


# ----------------------------------------------------------------------------- run + save

def run_isolation_experiment(scenario: str = "all", level: str = "all", policy: str = "detect",
                             include_mysql: bool = False) -> dict[str, Any]:
    """Run the anomaly lab on the engine (and optionally on MySQL) and save one experiment."""
    engine = run_engine_anomalies(scenario, level, policy)
    mysql_runs = run_mysql_anomalies(scenario, level)["runs"] if include_mysql else []

    def save(cur):
        exp_id = _create(cur, f"isolation lab / {scenario} / {level} / {policy}" + (" + mysql" if include_mysql else ""),
                         "isolation", "measured_engine", policy, scenario, 2, len(engine["runs"]),
                         dict(scenario=scenario, level=level, policy=policy, include_mysql=include_mysql))
        seq = 0
        for name, runs in (("intellitx", engine["runs"]), ("mysql", mysql_runs)):
            for r in runs:
                cs = r.get("analysis", {}).get("conflict_serializable")
                waits = r.get("lock_waits", 0) if name == "mysql" else sum(1 for e in r["events"] if e["waited"])
                cur.execute(
                    "INSERT INTO anomaly_results (experiment_id, engine, scenario, isolation_level, anomaly, t1_committed,"
                    " t2_committed, lock_waits, conflict_serializable, verdict) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (exp_id, name, r["scenario"], r["isolation"], int(r["anomaly"]), int(r["t1_committed"]),
                     int(r["t2_committed"]), waits, None if cs is None else int(cs), r["verdict"][:600]))
                for e in r["events"]:
                    seq += 1
                    cur.execute("INSERT INTO transaction_logs (experiment_id, seq, txn_ref, event_type, detail) VALUES (%s,%s,%s,%s,%s)",
                                (exp_id, seq, e["txn"], "ISOLATION_STEP",
                                 f"[{name} {r['scenario']} {r['isolation']}] {e['label']}: {e['status']} {e['note']}"[:1000]))
        all_runs = engine["runs"] + mysql_runs
        _metrics(cur, exp_id, {
            "anomaly_runs": (len(all_runs), "runs"),
            "anomalies_observed": (sum(r["anomaly"] for r in all_runs), "runs"),
            "engine_matches_textbook": (float(all(r["anomaly"] == TEXTBOOK[r["scenario"]][LEVELS.index(r["isolation"])]
                                                  for r in engine["runs"])) if policy == "detect" else -1, "bool"),
        })
        _finish(cur, exp_id, None)
        return exp_id

    exp_id = _tx(save)
    return {"experiment_id": exp_id, "engine": engine["runs"], "mysql": mysql_runs, "policy": policy}


def analyze_schedule(schedule: str, save: bool = False) -> dict[str, Any]:
    if not schedule or not schedule.strip():
        raise ValueError("type a schedule, e.g. R1(A) W2(A) C1 C2")
    res = run_cli("serial", schedule=schedule.strip())["analysis"]
    if save:
        def _save(cur):
            exp_id = _create(cur, f"schedule check / {res['schedule_text'][:80]}", "serializability", "simulator",
                             "precedence_graph", None, None, max(1, len(res["transactions"]) + len(res["aborted"])),
                             dict(schedule=res["schedule_text"]))
            _metrics(cur, exp_id, {"conflict_serializable": (int(res["conflict_serializable"]), "bool"),
                                   "edges": (len(res["edges"]), "edges")})
            cur.execute("INSERT INTO transaction_logs (experiment_id, seq, event_type, detail, passed) VALUES (%s,1,'SERIALIZABILITY',%s,%s)",
                        (exp_id, f"{res['schedule_text']} | cycle={res['cycle']} | order={res['serial_order']} | {res['recoverability_note']}"[:1000],
                         int(res["conflict_serializable"])))
            _finish(cur, exp_id, None)
            return exp_id
        res["experiment_id"] = _tx(_save)
    return res


def run_live_experiment(level: str = "SER", policy: str = "detect", txns: int = 6, workers: int = 3, records: int = 3,
                        ops: int = 3, write_ratio: float = 0.5, think_us: int = 300, seed: int = 7) -> dict[str, Any]:
    if level not in LEVELS:
        raise ValueError(f"isolation level must be one of {LEVELS}")
    r = run_cli("serial-run", level=level, policy=policy, txns=txns, workers=workers, records=records, ops=ops,
                write_ratio=write_ratio, think_us=think_us, seed=seed, timeout=120)

    def save(cur):
        exp_id = _create(cur, f"live schedule / {level} / {policy} / seed {seed}", "serializability", "measured_engine",
                         f"{level}+{policy}", "hot_rows", workers, txns,
                         dict(level=level, policy=policy, txns=txns, workers=workers, records=records, ops=ops,
                              write_ratio=write_ratio, think_us=think_us, seed=seed))
        a = r["analysis"]
        _metrics(cur, exp_id, {"committed": (r["committed"], "txn"), "aborted_attempts": (r["aborted_attempts"], "attempts"),
                               "lost_updates": (r["lost_updates"], "updates"),
                               "conflict_serializable": (int(a["conflict_serializable"]), "bool"),
                               "precedence_edges": (len(a["edges"]), "edges")})
        for i, h in enumerate(r["history"][:400], start=1):
            cur.execute("INSERT INTO transaction_logs (experiment_id, seq, txn_ref, event_type, detail) VALUES (%s,%s,%s,%s,%s)",
                        (exp_id, i, h["txn"], "HISTORY", f"{h['op']}{h['txn']}" + (f"({h['item']}) = {h['value']}" if "item" in h else "")))
        _finish(cur, exp_id, r["lost_updates"] == 0)
        return exp_id

    r["experiment_id"] = _tx(save)
    return r


def latest_anomaly_matrix() -> list[dict]:
    from backend.database import query
    return query("SELECT engine, scenario, read_uncommitted, read_committed, repeatable_read, serializable "
                 "FROM v_anomaly_matrix ORDER BY engine, FIELD(scenario,'dirty_read','non_repeatable_read','lost_update','phantom')")
