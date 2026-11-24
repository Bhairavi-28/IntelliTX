#include "pipeline.h"
#include "scenarios.h"
#include "test_framework.h"

using namespace itx;

static PipelineConfig base(const TempDir& d, LockPolicy p) {
    PipelineConfig c;
    c.transactions = 1000;
    c.workers = 8;
    c.accounts = 50;
    c.theta = 0.8;
    c.policy = p;
    c.dir = d.sub("db");
    c.checkpoint_every_ms = 20;
    return c;
}

static void expect_pass(const PipelineResult& r) {
    for (const auto& c : r.checks)
        if (!c.pass) throw TestFailure("check '" + c.name + "' expected " + c.expected + " got " + c.actual);
    CHECK(r.all_pass);
}

TEST(pipeline_1000_concurrent_transactions_all_policies_all_checks_pass) {
    for (LockPolicy p : {LockPolicy::WAIT_DIE, LockPolicy::NO_WAIT, LockPolicy::DEADLOCK_DETECT, LockPolicy::AVOID}) {
        TempDir d;
        PipelineResult r = run_pipeline(base(d, p));
        expect_pass(r);
        CHECK_EQ(r.committed, 1000u);
        CHECK(r.audits > 0 && r.audits_committed == r.audits);   // isolation was really exercised
        CHECK_EQ(r.audit_violations, 0u);
        CHECK(r.timeline.size() > 1);
        CHECK(!r.stages.empty());
        std::cout << "         (" << to_string(p) << ": " << static_cast<long>(r.throughput_tps) << " tps, aborts " << r.aborted_attempts
                  << ", deadlock cycles " << r.deadlock_cycles << ", checkpoints " << r.checkpoints << ")\n";
    }
}

TEST(pipeline_detection_policy_finds_and_breaks_real_deadlocks_or_none_formed) {
    TempDir d;
    PipelineConfig c = base(d, LockPolicy::DEADLOCK_DETECT);
    c.theta = 0.0;  // uniform keys, transfers lock 2 random accounts => cycles are possible
    PipelineResult r = run_pipeline(c);
    expect_pass(r);
    CHECK_EQ(r.aborted_deadlock, r.deadlock_cycles);  // every detected cycle aborted exactly one victim
}

TEST(pipeline_without_wal_runs_and_checks_in_memory_state) {
    TempDir d;
    PipelineConfig c = base(d, LockPolicy::WAIT_DIE);
    c.wal = false;
    c.checkpoint_every_ms = 0;
    PipelineResult r = run_pipeline(c);
    expect_pass(r);
    CHECK_EQ(r.wal_bytes, 0u);
}

TEST(pipeline_admission_policy_changes_simulated_metrics_not_correctness) {
    TempDir d1, d2;
    PipelineConfig a = base(d1, LockPolicy::WAIT_DIE), b = base(d2, LockPolicy::WAIT_DIE);
    a.admission = SchedAlgo::FCFS;
    b.admission = SchedAlgo::SJF;
    PipelineResult ra = run_pipeline(a), rb = run_pipeline(b);
    expect_pass(ra);
    expect_pass(rb);
    CHECK(rb.sim_avg_waiting < ra.sim_avg_waiting);  // SJF minimises average waiting on the same jobs
}

TEST(pipeline_crash_mid_load_then_recover_keeps_acknowledged_commits) {
    for (LockPolicy p : {LockPolicy::WAIT_DIE, LockPolicy::DEADLOCK_DETECT}) {
        TempDir d;
        PipelineConfig c = base(d, p);
        c.crash_after_commits = 400;
        c.sync_commit = true;
        PipelineResult r = run_pipeline(c);
        CHECK(r.crash_injected);
        expect_pass(r);
        std::cout << "         (" << to_string(p) << ": crash after 400 commits; recovery redo " << r.recovery_redo << " undo " << r.recovery_undo
                  << " losers " << r.recovery_losers << ")\n";
    }
}

TEST(pipeline_crash_with_checkpoints_recovers_from_snapshot_plus_log) {
    TempDir d;
    PipelineConfig c = base(d, LockPolicy::WAIT_DIE);
    c.crash_after_commits = 700;
    c.checkpoint_every_ms = 10;
    PipelineResult r = run_pipeline(c);
    expect_pass(r);
}

TEST(pipeline_validates_configuration) {
    TempDir d;
    PipelineConfig c = base(d, LockPolicy::WAIT_DIE);
    c.transactions = 5;
    CHECK_THROWS(run_pipeline(c));
    c = base(d, LockPolicy::NONE);
    CHECK_THROWS(run_pipeline(c));
    c = base(d, LockPolicy::WAIT_DIE);
    c.dir = "";
    CHECK_THROWS(run_pipeline(c));
    c = base(d, LockPolicy::WAIT_DIE);
    c.crash_after_commits = 1000;  // must be below the number of transactions
    CHECK_THROWS(run_pipeline(c));
    c = base(d, LockPolicy::WAIT_DIE);
    c.wal = false;
    c.crash_after_commits = 10;
    CHECK_THROWS(run_pipeline(c));
}

namespace itx {
TEST(real_scheduler_policy_changes_measured_waiting_of_short_transactions) {
    double fcfs_wait = 0, sjf_wait = 0;
    for (SchedAlgo a : {SchedAlgo::FCFS, SchedAlgo::SJF}) {
        TempDir d;
        PipelineConfig c = base(d, LockPolicy::DEADLOCK_DETECT);
        c.admission = a; c.workers = 2; c.work_us = 100;
        PipelineResult r = run_pipeline(c);
        expect_pass(r);
        for (const auto& k : r.classes) if (k.priority == 1) (a == SchedAlgo::FCFS ? fcfs_wait : sjf_wait) = k.avg_wait_us;
    }
    CHECK(sjf_wait < fcfs_wait);  // short (priority-1) transactions really are served first under SJF
}
TEST(preemptive_admission_policy_is_rejected_and_disk_trace_is_produced) {
    TempDir d;
    PipelineConfig c = base(d, LockPolicy::WAIT_DIE);
    c.admission = SchedAlgo::SRTF;
    CHECK_THROWS(run_pipeline(c));
    TempDir d2;
    PipelineResult r = run_pipeline(base(d2, LockPolicy::WAIT_DIE));
    CHECK(r.disk_requests > 0 && r.disk.size() == 6);
}
TEST(avoid_policy_never_forms_a_deadlock_cycle_in_the_classic_scenario) {
    DeadlockScenarioResult r = run_deadlock_scenario(LockPolicy::AVOID);
    CHECK(!r.deadlock_cycle_detected);
    CHECK(r.consistent);
    CHECK_EQ(r.aborted_deadlock + r.aborted_wait_die + r.aborted_no_wait, 0u);
    CHECK(r.unsafe_denials >= 1);
    CHECK_EQ(r.lock_entries_left, 0u);
}
}  // namespace itx
