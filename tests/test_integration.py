import pytest

from backend.services import experiments as ex
from backend.services import queries, sql_lab

pytestmark = [pytest.mark.db, pytest.mark.engine, pytest.mark.usefixtures("db", "engine_cli")]


def test_transaction_run_is_persisted_with_samples_and_metrics():
    r = ex.run_transaction_experiment(workers=2, txns=1500, records=200, ops=4, name="pytest run")
    assert r["invariant_ok"]
    row = queries.experiment_row(r["experiment_id"])
    assert row["status"] == "COMPLETED" and row["source"] == "measured_engine" and row["invariant_ok"] == 1
    m = queries.metrics_for([r["experiment_id"]]).set_index("metric_name")["metric_value"]
    assert m["committed"] == 1500 and m["throughput_tps"] > 0
    assert 0 < len(queries.txn_samples(r["experiment_id"])) <= 200
    assert not queries.logs_for(r["experiment_id"]).empty


def test_scheduling_results_use_identical_jobs_and_are_saved():
    r = ex.run_scheduling_experiment(txn_jobs=10, seed=1)
    assert len(r["experiment_ids"]) == 6
    jobs = [queries.jobs_for(i)[["job_id", "arrival", "burst"]].to_dict("records") for i in r["experiment_ids"]]
    assert all(j == jobs[0] for j in jobs)
    assert all(queries.experiment_row(i)["source"] == "simulator" for i in r["experiment_ids"])


def test_deadlock_recovery_disk_alloc_osdeadlock_persist():
    assert ex.run_deadlock_experiment("detect")["cycle_detected"]
    rec = ex.run_recovery_experiment("crash")
    assert rec["all_pass"] and len(queries.logs_for(rec["experiment_id"])) == len(rec["checks"])
    assert len(ex.run_disk_experiment()["experiment_ids"]) == 6
    assert len(ex.run_allocation_experiment()["experiment_ids"]) == 4
    assert ex.run_os_deadlock_experiment("detect", request_matrix="0,0,0;2,0,2;0,0,1;1,0,0;0,0,2",
                                         alloc="0,1,0;2,0,0;3,0,3;2,1,1;0,0,2", avail="0,0,0")["deadlocked"]


def test_history_filters_and_hides_seed_rows():
    assert queries.history(include_seed=False)["is_seed"].eq(0).all()
    assert queries.history(include_seed=True)["is_seed"].eq(1).any()
    assert queries.history(category="disk")["category"].eq("disk").all()


def test_index_lab_shows_real_speedup_and_write_cost():
    r = sql_lab.run_and_save(rows=20000, reps=3)
    point = next(q for q in r["queries"] if q["query"] == "point_lookup")
    assert "idx_customer" in point["indexed"]["plan"]            # the optimiser really used the index
    assert "idx_customer" not in point["plain"]["plan"]
    assert point["speedup"] > 1
    assert r["size_mb"]["lab_orders_indexed"]["index"] > r["size_mb"]["lab_orders_plain"]["index"]
    with pytest.raises(ValueError):
        sql_lab.setup_lab(10)


def test_pipeline_normal_and_crash_runs_are_saved_with_checks():
    r = ex.run_pipeline_experiment(txns=600, workers=4, accounts=40, policy="detect", admission="SJF")
    assert r["all_pass"] and r["committed"] == 600 and r["audit_violations"] == 0
    row = queries.experiment_row(r["experiment_id"])
    assert row["category"] == "transaction" and row["invariant_ok"] == 1
    logs = queries.logs_for(r["experiment_id"])
    assert logs[logs.event_type == "PIPELINE_CHECK"].passed.eq(1).all() and "ADMISSION_SCHEDULING" in set(logs.event_type)
    c = ex.run_pipeline_experiment(txns=600, workers=4, accounts=40, crash_after=250, sync=True)
    assert c["crash_injected"] and c["all_pass"]
    assert queries.experiment_row(c["experiment_id"])["category"] == "recovery"
