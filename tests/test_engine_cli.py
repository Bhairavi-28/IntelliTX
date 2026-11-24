import pytest

from backend.services.engine import EngineError, run_cli

pytestmark = pytest.mark.usefixtures("engine_cli")


def test_cpu_scheduling_matches_textbook():
    r = run_cli("sched", algos="FCFS,SJF,RR", jobs="1:0:24:1,2:0:3:2,3:0:3:3", quantum=4)
    by = {x["algorithm"]: x for x in r["results"]}
    assert by["FCFS"]["avg_waiting"] == pytest.approx(17)
    assert by["SJF"]["avg_waiting"] == pytest.approx(3)
    assert by["RR"]["avg_waiting"] == pytest.approx(17 / 3, rel=1e-5)


def test_same_workload_for_all_algorithms():
    r = run_cli("sched", txn_jobs=15, seed=3)
    assert len(r["results"]) == 6
    assert {len(x["jobs"]) for x in r["results"]} == {15}


def test_disk_textbook_totals():
    r = run_cli("disk")
    tot = {x["algorithm"]: x["total_movement"] for x in r["results"]}
    assert tot["FCFS"] == 640 and tot["SSTF"] == 236 and tot["LOOK"] == 299 and tot["SCAN"] == 331


def test_allocation_fragmentation_story():
    r = run_cli("alloc", blocks=12, ops="create:A:4,create:B:2,create:C:4,remove:B,create:D:3", methods="contiguous_first_fit,linked")
    by = {x["method"]: x for x in r["results"]}
    assert by["contiguous_first_fit"]["failed_creates"] == 1
    assert by["linked"]["failed_creates"] == 0


@pytest.mark.parametrize("policy", ["wait_die", "no_wait", "detect"])
def test_bench_invariants_hold_and_nothing_gave_up(policy):
    r = run_cli("bench", policy=policy, workers=4, txns=3000, records=200, theta=0.9, samples=5)
    assert r["invariant_ok"] and r["gave_up"] == 0 and r["lock_entries_left"] == 0
    assert r["committed"] == 3000


def test_deadlock_detection_vs_prevention():
    det = run_cli("deadlock", policy="detect")
    prev = run_cli("deadlock", policy="wait_die")
    assert det["cycle_detected"] and det["aborted_deadlock"] == 1 and det["consistent"]
    assert not prev["cycle_detected"] and prev["aborted_wait_die"] >= 1 and prev["consistent"]


@pytest.mark.parametrize("scenario", ["crash", "checkpoint", "torn_tail"])
def test_recovery_scenarios(scenario, tmp_path):
    r = run_cli("recovery", dir=str(tmp_path / "d"), scenario=scenario)
    assert r["all_pass"] and all(c["pass"] for c in r["checks"])


def test_os_deadlock_modes():
    assert run_cli("osdeadlock", mode="safety")["safe"]
    assert not run_cli("osdeadlock", mode="request", process=4, request="3,3,0")["granted"]


@pytest.mark.parametrize("cmd,opts", [("bench", {"workers": 0}), ("bench", {"policy": "bogus"}), ("sched", {"algos": "XYZ"}),
                                      ("disk", {"requests": "999", "size": 10}), ("nope", {})])
def test_invalid_input_is_rejected_with_message(cmd, opts):
    with pytest.raises(EngineError):
        run_cli(cmd, **opts)


@pytest.mark.parametrize("policy", ["wait_die", "no_wait", "detect"])
def test_pipeline_cli_all_checks_pass(policy, tmp_path):
    r = run_cli("pipeline", policy=policy, txns=1000, workers=8, accounts=50, checkpoint_ms=20, dir=str(tmp_path / "db"))
    assert r["all_pass"] and r["committed"] == 1000 and r["audit_violations"] == 0
    assert all(c["pass"] for c in r["checks"])


def test_pipeline_cli_crash_and_recover(tmp_path):
    r = run_cli("pipeline", txns=1000, workers=8, accounts=50, crash_after=400, sync=1, dir=str(tmp_path / "db"))
    assert r["crash_injected"] and r["all_pass"]


def test_pipeline_cli_rejects_bad_input(tmp_path):
    with pytest.raises(EngineError):
        run_cli("pipeline", txns=1000, crash_after=1000, dir=str(tmp_path / "db"))
