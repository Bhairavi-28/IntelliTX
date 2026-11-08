// Integrated pipeline: ALL engine components working together on ONE shared database.
//
//   N transaction jobs ──► CPU-scheduling policy decides the ADMISSION ORDER ──► dispatcher thread
//   ──► bounded queue (producer–consumer, back-pressure) ──► worker pool (W threads)
//   ──► transaction manager: Strict 2PL + {Wait-Die | NO-WAIT | wait-for-graph detection}
//   ──► undo log + write-ahead log (+ periodic checkpoints taken while the load runs)
//   ──► [optional] process crash injected mid-run ──► restart recovery
//   ──► invariants verified ──► (Python) saved to MySQL.
//
// ADMISSION: a REAL non-preemptive scheduler (FCFS/SJF/PRIORITY) picks which waiting transaction a free
// worker runs next; queue wait and turnaround are MEASURED per class. Policy `avoid` = Banker's deadlock
// avoidance on the real transactions. The committed writes form a disk-request trace for the disk algorithms.
// Workload: bank-style. Records 0..accounts-1 are accounts; record `accounts` is a HOT counter that
// EVERY transfer increments (maximum contention on one row). Jobs of priority 3 are AUDITS: they
// read every account under shared locks and must always see the same total (isolation check).
// The admission policy only changes the ORDER in which transactions are handed to the pool; the
// threads themselves are still scheduled by the Linux kernel.
#pragma once
#include <string>
#include <vector>

#include "scheduler.h"
#include "transaction_manager.h"

namespace itx {

struct PipelineConfig {
    size_t transactions = 1000;
    size_t workers = 8;
    size_t queue_capacity = 256;        // ready-queue size = how many waiting transactions the scheduler can choose from
    uint32_t accounts = 100;
    double theta = 0.8;
    Value initial_balance = 1000;
    LockPolicy policy = LockPolicy::WAIT_DIE;
    SchedAlgo admission = SchedAlgo::FCFS;
    double quantum = 4.0;
    double work_us = 10.0;              // computation per burst unit, spent while holding locks
    bool wal = true;
    bool sync_commit = false;
    int checkpoint_every_ms = 0;        // 0 = no periodic checkpoint
    uint64_t crash_after_commits = 0;   // 0 = no crash; else kill the process after this many commits
    unsigned seed = 42;
    std::string dir;                    // WAL directory (required when wal = true)
    int sample_limit = 200;
};

struct StageEvent { double t_ms; std::string stage; std::string detail; };
struct PipelineCheck { std::string name, expected, actual; bool pass; };
struct TimelinePoint { double t_ms; uint64_t committed; uint64_t aborted; };
struct ClassStat { int priority; uint64_t count; double avg_wait_us, avg_turnaround_us; };
struct DiskStat { std::string algorithm; long total_movement; };
struct PipelineSample { uint64_t txn_ref; int attempts; double latency_us; int reads, writes; bool audit; };

struct PipelineResult {
    std::vector<StageEvent> stages;
    std::vector<PipelineCheck> checks;
    std::vector<TimelinePoint> timeline;
    std::vector<double> checkpoint_times_ms, checkpoint_durations_ms;
    std::vector<PipelineSample> samples;
    std::vector<ClassStat> classes;   // REAL measured queue wait / turnaround per transaction class
    std::vector<DiskStat> disk;       // disk algorithms run on this run's committed-write trace (simulated disk)
    uint64_t disk_requests = 0, unsafe_denials = 0;
    double real_avg_wait_us = 0, real_avg_turnaround_us = 0;
    bool crash_injected = false;
    bool all_pass = false;
    double elapsed_ms = 0, throughput_tps = 0;
    double avg_latency_us = 0, p50_latency_us = 0, p95_latency_us = 0, p99_latency_us = 0;
    uint64_t transactions = 0, audits = 0, committed = 0, transfers_committed = 0, audits_committed = 0;
    uint64_t aborted_attempts = 0, aborted_wait_die = 0, aborted_no_wait = 0, aborted_deadlock = 0;
    uint64_t lock_waits = 0, deadlock_cycles = 0, checkpoints = 0, audit_violations = 0, gave_up = 0;
    uint64_t wal_bytes = 0;
    uint64_t recovery_redo = 0, recovery_undo = 0, recovery_losers = 0, recovery_committed = 0;
    double sim_avg_waiting = 0, sim_avg_turnaround = 0, sim_avg_response = 0;  // admission policy (SIMULATED)
};

// Throws std::invalid_argument for bad configuration, std::runtime_error for OS failures.
PipelineResult run_pipeline(const PipelineConfig& cfg);

}  // namespace itx
