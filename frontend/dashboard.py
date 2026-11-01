from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from backend import database
from backend.services import experiments as ex
from backend.services import queries, sql_lab
from backend.services.engine import EngineError, cli_path, run_cli

st.set_page_config(page_title="IntelliTX", page_icon="🧪", layout="wide")
st.markdown("""<style>
.badge{display:inline-block;padding:2px 10px;border-radius:12px;font-size:.78rem;font-weight:600;margin-bottom:.4rem}
.measured{background:#dcfce7;color:#166534}.sim{background:#fef3c7;color:#92400e}
div[data-testid="stMetric"]{background:#f4f6fb;border-radius:10px;padding:10px 14px}
</style>""", unsafe_allow_html=True)

POLICIES = {"wait_die": "Strict 2PL + Wait-Die (prevention)", "no_wait": "Strict 2PL + NO-WAIT (prevention)",
            "detect": "Strict 2PL + wait-for-graph detection",
            "avoid": "Strict 2PL + Banker's avoidance (declared locks, safe-state check)"}
PALETTE = {"wait_die": "#2563eb", "no_wait": "#16a34a", "detect": "#dc2626", "avoid": "#9333ea"}


def badge(kind: str) -> None:
    if kind == "measured":
        st.markdown('<span class="badge measured">MEASURED · real engine / real MySQL</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="badge sim">SIMULATOR · deterministic model, not a measurement</span>', unsafe_allow_html=True)


def guarded(fn):
    """Run fn(); show a clear error instead of a stack trace."""
    try:
        return fn()
    except EngineError as exc:
        st.error(f"Engine error: {exc}")
    except ValueError as exc:
        st.error(f"Invalid input: {exc}")
    except Exception as exc:  # database / unexpected
        st.error(f"{type(exc).__name__}: {exc}")
    return None


def csv_button(df: pd.DataFrame, name: str, key: str) -> None:
    if not df.empty:
        st.download_button("Download CSV", df.to_csv(index=False).encode(), f"{name}.csv", "text/csv", key=key)


# ----------------------------------------------------------------------------- pages
def page_pipeline():
    st.header("Integrated Pipeline")
    badge("measured")
    st.caption("ALL components working together on ONE shared database: many transactions update the same accounts (and one hot counter row) "
               "at the same time, and the engine must keep the data correct.")
    st.markdown("`Transactions` → **CPU-scheduling policy** (admission order) → **bounded queue** (producer–consumer) → **worker pool** → "
                "**Strict 2PL locks** + **deadlock prevention / detection** → **undo log** + **write-ahead log** + **checkpoints** → "
                "(optional **crash**) → **recovery** → **correctness checks** → saved to **MySQL**")
    with st.form("pipeline_form"):
        c1, c2, c3 = st.columns(3)
        txns = c1.number_input("Concurrent transactions", 100, 100_000, 1000, step=100)
        workers = c1.select_slider("Worker threads", [1, 2, 4, 8, 16, 32], value=8)
        accounts = c1.number_input("Accounts (fewer = more conflicts)", 10, 5000, 100, step=10)
        theta = c2.slider("Hot-spot skew θ", 0.0, 1.2, 0.8, 0.05)
        policy = c2.selectbox("Concurrency control", list(POLICIES), format_func=POLICIES.get)
        admission = c2.selectbox("Admission scheduling policy", ["FCFS", "SJF", "PRIORITY"],
                                 help="REAL scheduler: from all waiting transactions, a free worker takes the one this policy picks. Non-preemptive only (a paused transaction would keep its locks).")
        queue = c3.number_input("Ready-queue size (scheduler chooses among these)", 1, 1000, 256)
        checkpoint_ms = c3.number_input("Checkpoint every (ms, 0 = off)", 0, 10_000, 20)
        wal = c3.checkbox("Write-ahead log", value=True)
        sync = c3.checkbox("fsync at commit", value=False, disabled=not wal)
        crash = st.checkbox("Inject a crash in the middle of the run, then recover", value=False, disabled=not wal)
        crash_after = st.number_input("Crash after this many commits", 1, int(txns) - 1, max(1, int(txns) * 2 // 5), disabled=not crash)
        run = st.form_submit_button("Run integrated pipeline", type="primary")
    if run:
        with st.spinner("Running 1000s of concurrent transactions…"):
            r = guarded(lambda: ex.run_pipeline_experiment(policy=policy, admission=admission, txns=int(txns), workers=workers, accounts=int(accounts),
                                                           theta=theta, wal=wal, sync=sync, checkpoint_ms=int(checkpoint_ms),
                                                           crash_after=int(crash_after) if crash and wal else 0, queue=int(queue)))
        if r:
            st.session_state["pipe"] = r
    r = st.session_state.get("pipe")
    if not r:
        return
    ok = r["all_pass"]
    (st.success if ok else st.error)(f"Experiment #{r['experiment_id']} saved. {sum(c['pass'] for c in r['checks'])}/{len(r['checks'])} correctness checks passed"
                                     + (" (after an injected crash and recovery)." if r["crash_injected"] else "."))
    a, b, c, d = st.columns(4)
    if r["crash_injected"]:
        a.metric("Transfers that survived the crash", f"{r['transfers_committed']:,}")
        b.metric("In-flight txns rolled back", r["recovery_losers"])
        c.metric("Redo applied (log tail)", r["recovery_redo"])
        d.metric("Undo applied", r["recovery_undo"])
    else:
        a.metric("Throughput", f"{r['throughput_tps']:,.0f} txn/s")
        b.metric("Committed", f"{r['committed']:,}", f"of {r['transactions']:,}", delta_color="off")
        c.metric("Aborted + retried", f"{r['aborted_attempts']:,}")
        d.metric("Deadlock cycles broken", r["deadlock_cycles_detected"])
    st.subheader("Correctness checks")
    chk = pd.DataFrame(r["checks"])
    chk["result"] = chk["pass"].map({True: "✅ PASS", False: "❌ FAIL"})
    st.dataframe(chk[["result", "name", "expected", "actual"]], use_container_width=True, hide_index=True)
    st.subheader("What each component did in this run")
    comp = [("CPU scheduling policy", f"{r['admission']} (REAL) chose which waiting transaction ran next. Measured avg queue wait {r['real_avg_wait_us']/1000:.1f} ms, turnaround {r['real_avg_turnaround_us']/1000:.1f} ms"),
            ("Bounded queue + worker pool", f"1 dispatcher → queue → {r['workers']} workers ran {r['transactions']:,} transactions ({r['audits']} were audits reading every account)"),
            ("Lock manager (Strict 2PL)", f"{r['lock_waits']:,} lock waits; aborts: {r['aborted_wait_die']} wait-die, {r['aborted_no_wait']} no-wait, {r['aborted_deadlock']} deadlock victims"),
            ("Deadlock handling", f"{r['deadlock_cycles_detected']} cycles found by the wait-for graph" if r["policy"] == "detect" else f"Banker's avoidance: {r['unsafe_denials']} free locks withheld because granting them was unsafe, 0 deadlocks formed" if r["policy"] == "avoid" else f"prevention ({r['policy']}): cycles cannot form, so 0 detected"),
            ("Undo log", f"{r['aborted_attempts']:,} aborted attempts were rolled back, each transaction retried until it committed"),
            ("Write-ahead log + checkpoints", f"{r['wal_bytes']:,} bytes of log, {r['checkpoints']} checkpoints taken during the load" if r["wal"] else "WAL disabled"),
            ("Recovery", f"redo {r['recovery_redo']}, undo {r['recovery_undo']}, {r['recovery_losers']} unfinished transaction(s) rolled back" if r["wal"] else "n/a"),
            ("Isolation", f"{r['audits_committed']} audits committed, {r['audit_violations']} saw an inconsistent total")]
    st.dataframe(pd.DataFrame(comp, columns=["Component", "What happened"]), use_container_width=True, hide_index=True)
    if r.get("classes"):
        st.subheader("Scheduler effect (MEASURED per class)")
        cl = pd.DataFrame(r["classes"]).rename(columns={"priority": "priority class (1=short, 3=audit)", "avg_wait_us": "avg queue wait (µs)", "avg_turnaround_us": "avg turnaround (µs)"})
        st.dataframe(cl, use_container_width=True, hide_index=True)
    if r.get("disk", {}).get("results"):
        st.subheader("Disk-scheduling algorithms on this run's committed writes (SIMULATED disk)")
        st.caption(f"Trace = {r['disk']['requests']:,} write requests (account/counter cylinders) from the committed transactions; head movement is computed, not measured.")
        st.dataframe(pd.DataFrame(r["disk"]["results"]), use_container_width=True, hide_index=True)
    if r["timeline"] and not r["crash_injected"]:
        tl = pd.DataFrame(r["timeline"]).melt("t_ms", ["committed", "aborted"], "series", "cumulative count")
        fig = px.line(tl, x="t_ms", y="cumulative count", color="series", title="Progress over time (dotted lines = checkpoints taken while the load ran)",
                      labels={"t_ms": "time (ms)"})
        for t in r["checkpoint_times_ms"]:
            fig.add_vline(x=t, line_dash="dot", line_color="#6b7280")
        st.plotly_chart(fig, use_container_width=True)
        causes = pd.DataFrame({"cause": ["wait-die", "no-wait", "deadlock victim"], "aborts": [r["aborted_wait_die"], r["aborted_no_wait"], r["aborted_deadlock"]]})
        c1, c2 = st.columns(2)
        c1.plotly_chart(px.bar(causes, x="cause", y="aborts", title="Why transactions were aborted (then retried)"), use_container_width=True)
        samples = queries.txn_samples(r["experiment_id"])
        if not samples.empty:
            c2.plotly_chart(px.histogram(samples, x="latency_us", nbins=30, title=f"Latency of {len(samples)} sampled commits (µs, incl. retries)"), use_container_width=True)
    st.subheader("Timeline of stages")
    stages = pd.DataFrame(r["stages"]).round({"t_ms": 1})
    st.dataframe(stages, use_container_width=True, hide_index=True)
    csv_button(stages, "pipeline_stages", "csv_pipe")


def page_transactions():
    st.header("Transaction Lab")
    badge("measured")
    st.caption("Real multithreaded transactions on the C++ engine: Strict 2PL, undo log, optional write-ahead log. "
               "Each run is saved to MySQL; correctness invariants are checked after every run.")
    with st.form("txn"):
        c1, c2, c3 = st.columns(3)
        policy = c1.selectbox("Lock policy", list(POLICIES), format_func=POLICIES.get)
        workload = c1.selectbox("Workload", ["ycsb", "transfer"], help="ycsb: Zipf point reads/writes; transfer: move money between 2 accounts")
        workers = c2.select_slider("Worker threads", [1, 2, 4, 8, 16], value=4)
        txns = c2.number_input("Transactions", 100, 500_000, 5000, step=500)
        records = c2.number_input("Records", 10, 1_000_000, 1000, step=100)
        theta = c3.slider("Zipf skew θ (contention)", 0.0, 1.2, 0.6, 0.05)
        ops = c3.number_input("Ops per txn (ycsb)", 1, 32, 8)
        write_ratio = c3.slider("Write ratio (ycsb)", 0.0, 1.0, 0.5, 0.05)
        wal = st.checkbox("Enable write-ahead log", value=False)
        sync = st.checkbox("fsync at commit (durable; slower)", value=False, disabled=not wal)
        go_run = st.form_submit_button("Run experiment", type="primary")
    if go_run:
        with st.spinner("Running on the real engine…"):
            r = guarded(lambda: ex.run_transaction_experiment(policy=policy, workload=workload, workers=workers, txns=int(txns),
                                                              records=int(records), ops=int(ops), write_ratio=write_ratio,
                                                              theta=theta, wal=wal, sync=sync))
        if r:
            (st.success if r["invariant_ok"] else st.error)(f"Experiment #{r['experiment_id']} saved. Invariant: {r['invariant_detail']}")
            a, b, c, d = st.columns(4)
            a.metric("Throughput", f"{r['throughput_tps']:,.0f} txn/s")
            b.metric("Elapsed", f"{r['elapsed_ms']:,.1f} ms")
            c.metric("Committed", f"{r['committed']:,}")
            d.metric("Aborted attempts", f"{r['aborted_attempts']:,}", f"{r['abort_rate']:.1%} of attempts", delta_color="off")
            e, f, g, h = st.columns(4)
            e.metric("Avg latency", f"{r['avg_latency_us']:,.0f} µs")
            f.metric("p95 latency", f"{r['p95_latency_us']:,.0f} µs")
            g.metric("Lock waits", f"{r['lock_waits']:,}")
            h.metric("Deadlock cycles found", r["deadlock_cycles_detected"])
            s = queries.txn_samples(r["experiment_id"])
            if not s.empty:
                st.plotly_chart(px.histogram(s, x="latency_us", nbins=40, title=f"Latency of {len(s)} sampled committed transactions (µs, includes retries)"),
                                use_container_width=True)
    st.subheader("Benchmark sweep")
    st.caption("Runs every selected policy at 1, 2, 4 and 8 workers with identical seeds, then stores each run.")
    sweep_policies = st.multiselect("Policies", list(POLICIES), default=list(POLICIES), format_func=POLICIES.get)
    sweep_txns = st.number_input("Transactions per run", 1000, 200_000, 10_000, step=1000, key="sweep_txns")
    if st.button("Run sweep"):
        bar = st.progress(0.0)
        todo = [(p, w) for p in sweep_policies for w in (1, 2, 4, 8)]
        for i, (p, w) in enumerate(todo, 1):
            guarded(lambda: ex.run_transaction_experiment(policy=p, workers=w, txns=int(sweep_txns), theta=0.6, name=f"sweep {p} {w}w"))
            bar.progress(i / len(todo))
        st.success(f"Saved {len(todo)} runs. See Performance Analysis.")


def page_deadlock():
    st.header("Concurrency and Deadlock Lab")
    t1, t2 = st.tabs(["Database lock deadlock (real engine)", "OS resource deadlock (simulator)"])
    with t1:
        badge("measured")
        st.markdown("T1 (older) locks **A then B**; T2 locks **B then A**. This creates a circular wait. "
                    "**Prevention** (Wait-Die, NO-WAIT) never lets the cycle form. **Detection** lets it form, finds it in the wait-for graph, aborts one victim, and retries.")
        policy = st.radio("Policy", list(POLICIES), format_func=POLICIES.get, horizontal=True)
        if st.button("Run deadlock scenario", type="primary"):
            r = guarded(lambda: ex.run_deadlock_experiment(policy))
            if r:
                (st.success if r["consistent"] else st.error)(
                    f"Experiment #{r['experiment_id']}: both transactions committed, final A={r['final_a']}, B={r['final_b']}, locks left={r['lock_entries_left']}")
                a, b, c = st.columns(3)
                a.metric("Mechanism", r["mechanism"])
                b.metric("Cycle detected", "yes" if r["cycle_detected"] else "no (none can form)")
                c.metric("Victim aborts", r["aborted_wait_die"] + r["aborted_no_wait"] + r["aborted_deadlock"])
                st.dataframe(pd.DataFrame(r["events"]), use_container_width=True, hide_index=True)
    with t2:
        badge("sim")
        st.markdown("Resource-allocation algorithms for **multiple instances per resource type** (OS syllabus). Matrices: rows = processes, columns = resources; rows separated by `;`.")
        mode = st.radio("Algorithm", ["safety", "request", "detect"], horizontal=True,
                        format_func={"safety": "Banker's safety (avoidance)", "request": "Banker's resource-request", "detect": "Deadlock detection"}.get)
        avail = st.text_input("Available", "3,3,2")
        alloc = st.text_area("Allocation", "0,1,0;2,0,0;3,0,2;2,1,1;0,0,2")
        opts = {"avail": avail, "alloc": alloc}
        if mode in ("safety", "request"):
            opts["max"] = st.text_area("Max demand", "7,5,3;3,2,2;9,0,2;2,2,2;4,3,3")
        if mode == "request":
            opts["process"] = st.number_input("Requesting process index", 0, 50, 1)
            opts["request"] = st.text_input("Request vector", "1,0,2")
        if mode == "detect":
            opts["request_matrix"] = st.text_area("Request matrix", "0,0,0;2,0,2;0,0,1;1,0,0;0,0,2")
            opts["avail"] = "0,0,0" if avail == "3,3,2" else avail
            opts["alloc"] = "0,1,0;2,0,0;3,0,3;2,1,1;0,0,2" if alloc == "0,1,0;2,0,0;3,0,2;2,1,1;0,0,2" else alloc
        if st.button("Run", key="osd"):
            r = guarded(lambda: ex.run_os_deadlock_experiment(mode, **opts))
            if r:
                st.json({k: v for k, v in r.items() if not k.startswith("_")})
        st.info("OS deadlocks involve processes holding resource instances; database deadlocks involve transactions waiting for record locks and are resolved by aborting and **rolling back** a transaction, which an OS cannot do to an arbitrary process.")


def gantt(result: dict) -> go.Figure:
    fig = go.Figure()
    for g in result["gantt"]:
        if g["job"] == -1:
            continue
        fig.add_trace(go.Bar(x=[g["end"] - g["start"]], y=["CPU"], base=g["start"], orientation="h", name=f"J{g['job']}",
                             text=f"J{g['job']}", textposition="inside", showlegend=False,
                             hovertemplate=f"J{g['job']}: {g['start']:.1f}–{g['end']:.1f}<extra></extra>"))
    fig.update_layout(barmode="overlay", height=160, margin=dict(l=10, r=10, t=30, b=10), title=f"Gantt chart · {result['algorithm']}",
                      xaxis_title="time units")
    return fig


def page_scheduling():
    st.header("CPU Scheduling Lab")
    badge("sim")
    st.caption("Discrete-event simulation of scheduling POLICIES on transaction-shaped jobs (burst = number of record operations). "
               "It does not change how Linux schedules the real engine threads. All algorithms receive the identical job list.")
    src = st.radio("Job source", ["Synthetic transaction jobs", "My own jobs"], horizontal=True)
    jobs_text, n, seed, spread = None, 12, 42, 20.0
    if src == "My own jobs":
        jobs_text = st.text_input("Jobs: id:arrival:burst:priority, …  (lower priority number = more important)", "1:0:24:1,2:0:3:2,3:0:3:3")
    else:
        c1, c2, c3 = st.columns(3)
        n = c1.number_input("Jobs", 2, 200, 12)
        seed = c2.number_input("Seed", 0, 10_000, 42)
        spread = c3.number_input("Arrival spread", 0.0, 500.0, 20.0)
    quantum = st.number_input("Round-robin quantum", 0.5, 100.0, 4.0)
    if st.button("Simulate all algorithms", type="primary"):
        r = guarded(lambda: ex.run_scheduling_experiment(quantum=quantum, txn_jobs=int(n), seed=int(seed), spread=spread, jobs=jobs_text))
        if r:
            st.session_state["sched"] = r
    r = st.session_state.get("sched")
    if r:
        m = queries.metrics_for(r["experiment_ids"])  # read back from MySQL
        piv = m[m.metric_name.isin(["avg_waiting", "avg_turnaround", "avg_response"])].pivot_table(index="algorithm", columns="metric_name", values="metric_value")
        st.plotly_chart(px.bar(piv.reset_index().melt("algorithm"), x="algorithm", y="value", color="variable", barmode="group",
                               title="Average metrics per algorithm (time units)"), use_container_width=True)
        full = m.pivot_table(index="algorithm", columns="metric_name", values="metric_value").round(3)
        st.dataframe(full, use_container_width=True)
        csv_button(full.reset_index(), "scheduling_comparison", "csv_sched")
        pick = st.selectbox("Gantt chart / per-job table", [x["algorithm"] for x in r["results"]])
        chosen = next(x for x in r["results"] if x["algorithm"] == pick)
        st.plotly_chart(gantt(chosen), use_container_width=True)
        st.dataframe(pd.DataFrame(chosen["jobs"]).round(3), use_container_width=True, hide_index=True)


def page_sql():
    st.header("SQL and Indexing Lab")
    badge("measured")
    st.caption("Real timings from this MySQL server on two identical tables; only one has secondary indexes. Median of several runs after a warm-up.")
    c1, c2 = st.columns(2)
    rows = c1.number_input("Rows", 1000, 1_000_000, 100_000, step=10_000)
    reps = c2.number_input("Repetitions per query", 1, 30, 7)
    if st.button("Build tables and run index experiment", type="primary"):
        with st.spinner("Loading data and timing queries…"):
            res = guarded(lambda: sql_lab.run_and_save(int(rows), int(reps)))
        if res:
            st.session_state["sqlres"] = res
    res = st.session_state.get("sqlres")
    if res:
        st.success(f"Experiment #{res['experiment_id']} saved ({res['rows']:,} rows).")
        df = pd.DataFrame([{"query": q["query"], "predicate": q["predicate"], "no index (ms)": q["plain"]["median_ms"],
                            "with index (ms)": q["indexed"]["median_ms"], "speedup ×": q["speedup"]} for q in res["queries"]]).round(2)
        st.dataframe(df, use_container_width=True, hide_index=True)
        melt = df.melt("query", ["no index (ms)", "with index (ms)"])
        st.plotly_chart(px.bar(melt, x="query", y="value", color="variable", barmode="group", log_y=True, title="Query time (ms, log scale)"), use_container_width=True)
        a, b, c = st.columns(3)
        a.metric("Insert 20k rows, no index", f"{res['insert_20k_ms']['plain']:.0f} ms")
        b.metric("Insert 20k rows, with indexes", f"{res['insert_20k_ms']['indexed']:.0f} ms",
                 f"{res['insert_20k_ms']['indexed'] / res['insert_20k_ms']['plain']:.2f}× slower", delta_color="inverse")
        c.metric("Index storage", f"{res['size_mb']['lab_orders_indexed']['index']:.1f} MB", f"data {res['size_mb']['lab_orders_indexed']['data']:.1f} MB", delta_color="off")
        st.caption(f"Index build time: {res['index_build_ms']:.0f} ms. Indexes speed up selective reads and cost write time and disk space. A full-table GROUP BY on a low-cardinality column gains little.")
        q = st.selectbox("Query plan", [x["query"] for x in res["queries"]])
        item = next(x for x in res["queries"] if x["query"] == q)
        l, r_ = st.columns(2)
        l.markdown("**Without index**"); l.code(item["plain"]["plan"], language="text"); l.code(item["plain"]["explain_analyze"], language="text")
        r_.markdown("**With index**"); r_.code(item["indexed"]["plan"], language="text"); r_.code(item["indexed"]["explain_analyze"], language="text")
    st.subheader("Run the demo queries (read-only)")
    text = (database.ROOT / "storage" / "queries.sql").read_text()
    stmts = [s for s in database.split_sql(text) if s.lstrip().upper().startswith("SELECT")]
    choice = st.selectbox("Statement", range(len(stmts)), format_func=lambda i: stmts[i].splitlines()[0][:90])
    st.code(stmts[choice], language="sql")
    if st.button("Execute statement"):
        out = guarded(lambda: database.query(stmts[choice]))
        if out is not None:
            st.dataframe(pd.DataFrame(out), use_container_width=True)


def page_recovery():
    st.header("Recovery Lab")
    badge("measured")
    st.caption("A child process runs transactions against the engine's write-ahead log and is killed with _exit() (no shutdown code). "
               "The engine then restarts and recovers. This tests PROCESS crashes; power-loss behaviour is not tested.")
    scenario = st.selectbox("Scenario", ["crash", "checkpoint", "torn_tail"], format_func={
        "crash": "Crash with committed, rolled-back and in-flight transactions",
        "checkpoint": "Crash after a checkpoint (snapshot + log tail)",
        "torn_tail": "Half-written log record at the tail"}.get)
    if st.button("Crash and recover", type="primary"):
        r = guarded(lambda: ex.run_recovery_experiment(scenario))
        if r:
            (st.success if r["all_pass"] else st.error)(f"Experiment #{r['experiment_id']}: {sum(c['pass'] for c in r['checks'])}/{len(r['checks'])} checks passed")
            rep = r["report"]
            cols = st.columns(5)
            for col, (label, key) in zip(cols, [("Log records", "records_scanned"), ("Redo applied", "redo_applied"), ("Undo applied", "undo_applied"),
                                                ("Committed txns", "committed_txns"), ("Loser txns undone", "losers")]):
                col.metric(label, rep[key])
            st.dataframe(pd.DataFrame(r["checks"]), use_container_width=True, hide_index=True)
    past = queries.history(category="recovery")
    if not past.empty:
        st.subheader("Past recovery runs")
        def _checks(i):
            lg = queries.logs_for(i)
            lg = lg[lg.passed.notna()]
            return f"{int(lg.passed.sum())}/{len(lg)} passed"
        past["checks"] = past["experiment_id"].map(_checks)
        st.dataframe(past[["experiment_id", "algorithm", "invariant_ok", "checks", "created_at"]], use_container_width=True, hide_index=True)


def page_fs_disk():
    st.header("File Allocation and Disk Scheduling Lab")
    badge("sim")
    t1, t2 = st.tabs(["Disk scheduling", "File allocation"])
    with t1:
        c1, c2, c3, c4 = st.columns(4)
        reqs = c1.text_input("Request queue", "98,183,37,122,14,124,65,67")
        head = c2.number_input("Head", 0, 100000, 53)
        size = c3.number_input("Cylinders", 2, 100000, 200)
        direction = c4.selectbox("Initial direction", ["up", "down"])
        if st.button("Compare disk algorithms", type="primary"):
            r = guarded(lambda: ex.run_disk_experiment(reqs, int(head), int(size), direction))
            if r:
                st.session_state["disk"] = r
        r = st.session_state.get("disk")
        if r:
            df = pd.DataFrame([{k: x[k] for k in ("algorithm", "total_movement", "return_jump", "avg_movement_per_request")} for x in r["results"]]).round(2)
            st.plotly_chart(px.bar(df, x="algorithm", y="total_movement", title="Total head movement (cylinders; circular variants include the return jump)"), use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True)
            csv_button(df, "disk_scheduling", "csv_disk")
            pick = st.selectbox("Head path", [x["algorithm"] for x in r["results"]])
            item = next(x for x in r["results"] if x["algorithm"] == pick)
            fig = px.line(x=list(range(len(item["head_path"]))), y=item["head_path"], markers=True, labels={"x": "step", "y": "cylinder"},
                          title=f"{pick}: service order {item['service_order']}")
            st.plotly_chart(fig, use_container_width=True)
    with t2:
        c1, c2 = st.columns([1, 3])
        blocks = c1.number_input("Disk blocks", 4, 5000, 32)
        ops = c2.text_input("Operations (create:NAME:BLOCKS | remove:NAME)", "create:A:5,create:B:3,create:C:6,remove:B,create:D:4,create:E:5")
        if st.button("Compare allocation methods", type="primary"):
            r = guarded(lambda: ex.run_allocation_experiment(int(blocks), ops))
            if r:
                st.session_state["alloc"] = r
        r = st.session_state.get("alloc")
        if r:
            df = pd.DataFrame([{k: x[k] for k in ("method", "used_blocks", "free_blocks", "overhead_blocks", "failed_creates",
                                                  "external_fragmentation_pct", "avg_blocks_read_to_reach_last_block")} for x in r["results"]]).round(2)
            st.dataframe(df, use_container_width=True, hide_index=True)
            csv_button(df, "file_allocation", "csv_alloc")
            pick = st.selectbox("Disk map", [x["method"] for x in r["results"]])
            item = next(x for x in r["results"] if x["method"] == pick)
            names = sorted({o for o in item["block_owner"] if o})
            cmap = {n: i + 1 for i, n in enumerate(names)}
            z = [cmap.get(o, 0) for o in item["block_owner"]]
            per_row = 16
            z += [None] * (-len(z) % per_row)
            grid = [z[i:i + per_row] for i in range(0, len(z), per_row)]
            text = [[(item["block_owner"][i * per_row + j] if i * per_row + j < len(item["block_owner"]) else "") or "·" for j in range(per_row)] for i in range(len(grid))]
            fig = go.Figure(go.Heatmap(z=grid, text=text, texttemplate="%{text}", colorscale="Tealgrn", showscale=False, xgap=2, ygap=2))
            fig.update_yaxes(autorange="reversed", visible=False); fig.update_xaxes(visible=False)
            fig.update_layout(height=60 + 40 * len(grid), margin=dict(l=10, r=10, t=30, b=10), title="Block map ('·' free, name* = index block)")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(pd.DataFrame(item["outcomes"])[["op", "name", "blocks_requested", "success", "message"]], use_container_width=True, hide_index=True)


def page_performance():
    st.header("Performance Analysis")
    badge("measured")
    runs = queries.transaction_runs()
    if runs.empty:
        st.info("No saved transaction experiments yet. Run one in the Transaction Lab (or the benchmark sweep).")
        return
    runs["workers"] = runs["workers"].astype(int)
    st.caption(f"{len(runs)} saved real-engine runs. Each point is the mean of the runs for that policy and worker count. "
               "Throughput does not always rise with workers: contention causes waits and aborts, and this engine has a single central lock table.")
    wl = st.selectbox("Workload", sorted(runs.workload.unique()))
    sel = runs[runs.workload == wl].copy()
    sel["abort_rate"] = sel.aborted_attempts / (sel.committed + sel.aborted_attempts).replace(0, pd.NA)
    agg = sel.groupby(["algorithm", "workers"], as_index=False).agg(throughput_tps=("throughput_tps", "mean"), abort_rate=("abort_rate", "mean"),
                                                                      elapsed_ms=("elapsed_ms", "mean"), runs=("experiment_id", "count"))
    c1, c2 = st.columns(2)
    c1.plotly_chart(px.line(agg, x="workers", y="throughput_tps", color="algorithm", markers=True, color_discrete_map=PALETTE,
                            title="Successful throughput (committed txn/s)"), use_container_width=True)
    c2.plotly_chart(px.line(agg, x="workers", y="abort_rate", color="algorithm", markers=True, color_discrete_map=PALETTE,
                            title="Abort rate (aborted attempts / all attempts)"), use_container_width=True)
    st.plotly_chart(px.bar(agg, x="workers", y="elapsed_ms", color="algorithm", barmode="group", color_discrete_map=PALETTE,
                           title="Elapsed wall time (ms)"), use_container_width=True)
    lat = queries.latency_percentiles(sel.experiment_id.tolist())
    if not lat.empty:
        lat = lat.groupby(["algorithm", "workers", "metric_name"], as_index=False).metric_value.mean()
        st.plotly_chart(px.bar(lat[lat.metric_name == "p95_latency_us"], x="workers", y="metric_value", color="algorithm", barmode="group",
                               color_discrete_map=PALETTE, title="p95 transaction latency (µs, includes retries)"), use_container_width=True)
    bad = sel[sel.invariant_ok != 1]
    if len(bad):
        st.error(f"{len(bad)} run(s) violated the correctness invariant: {bad.experiment_id.tolist()}")
    else:
        st.success("Every plotted run passed its correctness invariant.")
    st.dataframe(agg.round(3), use_container_width=True, hide_index=True)
    csv_button(agg, "performance_summary", "csv_perf")


def page_history():
    st.header("Experiment History")
    c1, c2, c3, c4 = st.columns(4)
    cat = c1.selectbox("Category", ["(all)", "transaction", "deadlock", "recovery", "scheduling", "disk", "allocation", "os_deadlock", "sql_index"])
    src = c2.selectbox("Source", ["(all)", "measured_engine", "measured_mysql", "simulator"])
    algo = c3.text_input("Algorithm contains", "")
    seeds = c4.checkbox("Include seed rows", False)
    df = queries.history(None if cat == "(all)" else cat, None if src == "(all)" else src, None, seeds)
    if algo:
        df = df[df.algorithm.str.contains(algo, case=False, na=False)]
    st.dataframe(df, use_container_width=True, hide_index=True)
    csv_button(df, "experiment_history", "csv_hist")
    if not df.empty:
        eid = st.selectbox("Inspect experiment", df.experiment_id.tolist())
        row = queries.experiment_row(int(eid))
        st.json({k: str(v) for k, v in row.items()})
        m = queries.metrics_for([int(eid)])
        st.dataframe(m[["metric_name", "metric_value", "unit"]], use_container_width=True, hide_index=True)
        logs = queries.logs_for(int(eid))
        if not logs.empty:
            st.markdown("**Event log**"); st.dataframe(logs, use_container_width=True, hide_index=True)
        jobs = queries.jobs_for(int(eid))
        if not jobs.empty:
            st.markdown("**Per-job results (simulated)**"); st.dataframe(jobs, use_container_width=True, hide_index=True)


PAGES = {"Integrated Pipeline": page_pipeline, "Transaction Lab": page_transactions, "Concurrency & Deadlock Lab": page_deadlock, "CPU Scheduling Lab": page_scheduling,
         "SQL & Indexing Lab": page_sql, "Recovery Lab": page_recovery, "File Allocation & Disk Lab": page_fs_disk,
         "Performance Analysis": page_performance, "Experiment History": page_history}


def main():
    st.sidebar.title("IntelliTX")
    st.sidebar.caption("Multithreaded Transaction Management and OS Resource Scheduling Analysis")
    ok, info = database.ping()
    if not ok:
        st.sidebar.error("MySQL not reachable")
        st.error(f"Cannot reach MySQL: {info}\n\nConfigure `.env` (see README) and run `python -m scripts.init_db`.")
        st.stop()
    st.sidebar.success(f"MySQL {info.split('-')[0]}")
    if cli_path().exists():
        st.sidebar.success("C++ engine found")
    else:
        st.sidebar.error("C++ engine not built")
    page = st.sidebar.radio("Module", list(PAGES))
    try:
        database.query("SELECT 1 FROM experiments LIMIT 1")
    except Exception:
        st.warning("Schema missing. Run `python -m scripts.init_db` first.")
        st.stop()
    PAGES[page]()


main()
