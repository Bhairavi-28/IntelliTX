"""Isolation levels, anomaly lab (engine + MySQL) and the serializability checker."""
import pytest

from backend.services.engine import EngineError, run_cli

TEXTBOOK = {"dirty_read": [True, False, False, False], "non_repeatable_read": [True, True, False, False],
            "lost_update": [True, True, False, False], "phantom": [True, True, True, False]}
LEVELS = ["RU", "RC", "RR", "SER"]


# ------------------------------------------------------------------ engine only (no MySQL needed)

@pytest.mark.usefixtures("engine_cli")
def test_engine_anomaly_matrix_is_the_textbook_table():
    runs = run_cli("isolation", policy="detect")["runs"]
    assert len(runs) == 16
    for r in runs:
        assert r["anomaly"] == TEXTBOOK[r["scenario"]][LEVELS.index(r["isolation"])], r["verdict"]
        assert r["events"] and r["verdict"]


@pytest.mark.usefixtures("engine_cli")
def test_engine_anomaly_shows_up_as_precedence_cycle():
    for r in run_cli("isolation", scenario="lost_update")["runs"]:
        assert r["analysis"]["conflict_serializable"] == (not r["anomaly"])
    ser = run_cli("isolation", scenario="phantom", level="SER")["runs"][0]
    assert any(e["waited"] for e in ser["events"])  # T2's write waited for the scan's locks


@pytest.mark.usefixtures("engine_cli")
def test_schedule_checker_cli():
    a = run_cli("serial", schedule="R1(A) W2(A) R2(B) W1(B) C1 C2")["analysis"]
    assert not a["conflict_serializable"] and sorted(a["cycle"]) == [1, 2]
    b = run_cli("serial", schedule="R1(A) W2(A) W1(A) W3(A)")["analysis"]
    assert b["view_serializable"] == "yes" and not b["conflict_serializable"]
    c = run_cli("serial", schedule="W1(A) R2(A) C2 A1")["analysis"]
    assert c["recoverable"] == "no"
    with pytest.raises(EngineError):
        run_cli("serial", schedule="X1(A)")


@pytest.mark.usefixtures("engine_cli")
def test_live_run_serializable_vs_read_uncommitted():
    ser = run_cli("serial-run", level="SER", txns=8, seed=3)
    assert ser["analysis"]["conflict_serializable"] and ser["lost_updates"] == 0
    lost = sum(run_cli("serial-run", level="RU", txns=8, think_us=800, seed=s)["lost_updates"] for s in range(1, 5))
    assert lost > 0


# ------------------------------------------------------------------ saved to MySQL



@pytest.mark.db
@pytest.mark.usefixtures("db", "engine_cli")
def test_isolation_experiment_is_saved_and_matrix_view_pivots():
    from backend.database import query
    from backend.services import isolation as iso
    r = iso.run_isolation_experiment("all", "all", "detect", include_mysql=False)
    rows = query("SELECT COUNT(*) AS n FROM anomaly_results WHERE experiment_id=%s", (r["experiment_id"],))
    assert rows[0]["n"] == 16
    mat = {m["scenario"]: m for m in iso.latest_anomaly_matrix() if m["engine"] == "intellitx"}
    assert mat["phantom"]["repeatable_read"] == "X" and mat["phantom"]["serializable"] == "-"
    assert mat["dirty_read"]["read_uncommitted"] == "X" and mat["dirty_read"]["read_committed"] == "-"
    m = query("SELECT metric_value FROM experiment_metrics WHERE experiment_id=%s AND metric_name='engine_matches_textbook'",
              (r["experiment_id"],))
    assert m[0]["metric_value"] == 1


@pytest.mark.db
@pytest.mark.usefixtures("db")
def test_anomaly_results_constraints():
    from mysql.connector import Error
    from backend.database import cursor
    with cursor() as cur:
        cur.execute("INSERT INTO experiments (name, category, source, algorithm) VALUES ('c', 'isolation', 'measured_engine', 'detect')")
        eid = cur.lastrowid
    with pytest.raises(Error):  # an anomaly needs at least one committed transaction (CHECK)
        with cursor() as cur:
            cur.execute("INSERT INTO anomaly_results (experiment_id, engine, scenario, isolation_level, anomaly, t1_committed, t2_committed, verdict)"
                        " VALUES (%s,'intellitx','phantom','RR',1,0,0,'x')", (eid,))
    with pytest.raises(Error):  # bad isolation level (ENUM)
        with cursor() as cur:
            cur.execute("INSERT INTO anomaly_results (experiment_id, engine, scenario, isolation_level, anomaly, t1_committed, t2_committed, verdict)"
                        " VALUES (%s,'intellitx','phantom','SNAPSHOT',0,1,1,'x')", (eid,))


@pytest.mark.db
@pytest.mark.usefixtures("db")
def test_mysql_innodb_isolation_behaviour():
    """Documented InnoDB behaviour (MVCC for plain SELECT at RC/RR, locking reads at SERIALIZABLE)."""
    from backend.services import isolation as iso
    cell = lambda s, l: iso.run_mysql_anomaly(s, l)  # noqa: E731
    assert cell("dirty_read", "RU")["anomaly"]               # RU reads the uncommitted row version
    rc_dirty = cell("dirty_read", "RC")
    assert not rc_dirty["anomaly"] and not any(e["waited"] for e in rc_dirty["events"])  # snapshot, no waiting
    assert cell("non_repeatable_read", "RC")["anomaly"]
    assert not cell("non_repeatable_read", "RR")["anomaly"]  # consistent snapshot for the whole transaction
    assert cell("lost_update", "RR")["anomaly"]              # InnoDB RR still loses the update without FOR UPDATE
    assert not cell("phantom", "RR")["anomaly"]              # snapshot read hides the new matching row
    for s in iso.SCENARIOS:
        assert not cell(s, "SER")["anomaly"], s
