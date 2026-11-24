import mysql.connector
import pytest

from backend.database import split_sql

pytestmark = pytest.mark.db


def test_split_sql_handles_delimiters():
    stmts = split_sql("SELECT 1;\nDELIMITER $$\nCREATE TRIGGER t BEFORE INSERT ON x FOR EACH ROW BEGIN\n SET @a=1;\nEND$$\nDELIMITER ;\nSELECT 2;")
    assert stmts[0] == "SELECT 1" and stmts[-1] == "SELECT 2" and len(stmts) == 3 and "SET @a=1;" in stmts[1]


def test_at_least_four_related_tables_exist(db):
    rows = db.query("SELECT table_name AS t FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type='BASE TABLE'")
    names = {r["t"] for r in rows}
    assert {"experiments", "transactions", "transaction_logs", "experiment_metrics"} <= names
    fks = db.query("SELECT COUNT(*) AS n FROM information_schema.referential_constraints WHERE constraint_schema = DATABASE()")
    assert fks[0]["n"] >= 4


def test_foreign_key_rejects_orphans(db):
    with pytest.raises(mysql.connector.Error):
        with db.cursor() as cur:
            cur.execute("INSERT INTO experiment_metrics (experiment_id, metric_name, metric_value) VALUES (999999,'x',1)")


def test_check_and_not_null_and_unique_constraints(db):
    with pytest.raises(mysql.connector.Error):  # CHECK: workers range
        with db.cursor() as cur:
            cur.execute("INSERT INTO experiments (name,category,source,algorithm,workers) VALUES ('x','transaction','measured_engine','a',0)")
    with pytest.raises(mysql.connector.Error):  # CHECK: blank name
        with db.cursor() as cur:
            cur.execute("INSERT INTO experiments (name,category,source,algorithm) VALUES ('  ','transaction','measured_engine','a')")
    with pytest.raises(mysql.connector.Error):  # ENUM / NOT NULL
        with db.cursor() as cur:
            cur.execute("INSERT INTO experiments (name,category,source,algorithm) VALUES ('x','bogus','measured_engine','a')")
    with db.cursor() as cur:
        cur.execute("INSERT INTO experiments (name,category,source,algorithm) VALUES ('uq','transaction','measured_engine','a')")
        eid = cur.lastrowid
        cur.execute("INSERT INTO experiment_metrics (experiment_id,metric_name,metric_value) VALUES (%s,'m',1)", (eid,))
    with pytest.raises(mysql.connector.Error):  # UNIQUE (experiment, metric)
        with db.cursor() as cur:
            cur.execute("INSERT INTO experiment_metrics (experiment_id,metric_name,metric_value) VALUES (%s,'m',2)", (eid,))


def test_cascade_delete_trigger_audit_and_procedures(db):
    with db.cursor() as cur:
        cur.execute("INSERT INTO experiments (name,category,source,algorithm) VALUES ('casc','transaction','measured_engine','a')")
        eid = cur.lastrowid
        cur.callproc("sp_record_metric", (eid, "throughput_tps", 10.0, "txn/s"))
        cur.callproc("sp_record_metric", (eid, "throughput_tps", 20.0, "txn/s"))  # upsert
        cur.execute("SELECT COUNT(*) AS n, MAX(metric_value) AS v FROM experiment_metrics WHERE experiment_id=%s", (eid,))
        row = cur.fetchone()
        assert row["n"] == 1 and row["v"] == 20.0
        cur.execute("INSERT INTO transaction_logs (experiment_id,seq,event_type,detail) VALUES (%s,1,'X','d')", (eid,))
        cur.execute("DELETE FROM experiments WHERE experiment_id=%s", (eid,))
        cur.execute("SELECT COUNT(*) AS n FROM experiment_metrics WHERE experiment_id=%s", (eid,))
        assert cur.fetchone()["n"] == 0                                   # cascade
        cur.execute("SELECT COUNT(*) AS n FROM transaction_logs WHERE experiment_id=%s", (eid,))
        assert cur.fetchone()["n"] == 0
        cur.execute("SELECT COUNT(*) AS n FROM experiment_audit WHERE experiment_id=%s AND action='DELETE'", (eid,))
        assert cur.fetchone()["n"] == 1                                   # trigger


def test_status_guard_and_latency_triggers(db):
    with db.cursor() as cur:
        cur.execute("INSERT INTO experiments (name,category,source,algorithm,status) VALUES ('g','transaction','measured_engine','a','COMPLETED')")
        eid = cur.lastrowid
    with pytest.raises(mysql.connector.Error, match="cannot be reopened"):
        with db.cursor() as cur:
            cur.execute("UPDATE experiments SET status='RUNNING' WHERE experiment_id=%s", (eid,))
    with pytest.raises(mysql.connector.Error, match="negative"):
        with db.cursor() as cur:
            cur.execute("INSERT INTO transactions (experiment_id,txn_ref,latency_us) VALUES (%s,1,-5)", (eid,))


def test_views_and_purge_procedure(db):
    rows = db.query("SELECT * FROM v_experiment_summary WHERE is_seed = 1")
    assert any(r["throughput_tps"] == 1000 for r in rows)
    with db.cursor() as cur:
        cur.execute("INSERT INTO experiments (name,category,source,algorithm,created_at) VALUES ('old','transaction','measured_engine','a', NOW() - INTERVAL 40 DAY)")
        cur.execute("SET @n = 0")
        cur.execute("CALL sp_purge_experiments(30, @n)")
        cur.execute("SELECT @n AS n")
        assert cur.fetchone()["n"] >= 1
    assert db.query("SELECT COUNT(*) AS n FROM experiments WHERE is_seed=1")[0]["n"] == 2  # seeds survive purge
    with pytest.raises(mysql.connector.Error):
        with db.cursor() as cur:
            cur.execute("CALL sp_purge_experiments(-1, @n)")


def test_queries_sql_file_runs(db):
    from backend.database import ROOT, run_script
    run_script(ROOT / "storage" / "queries.sql")
