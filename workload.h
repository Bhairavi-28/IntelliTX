// Reproducible transactional workloads run on the REAL engine (real locks, real WAL, real threads).
//
//   transfer : each transaction moves an amount between two accounts (read-modify-write on two
//              records, acquired in random order => deadlocks are possible).
//              Invariant: the sum of all balances never changes.
//   ycsb     : each transaction performs `ops` point operations on Zipf-distributed keys; a
//              write is read_for_update + 1. Invariant: final sum - initial sum == number of
//              write operations of COMMITTED transactions (detects lost updates).
//
// Both invariants are checked after every run, so a benchmark that "wins" by corrupting data is
// reported as invalid.
#pragma once
#include <string>
#include <vector>

#include "transaction_manager.h"

namespace itx {

enum class WorkloadType { TRANSFER, YCSB };

struct WorkloadConfig {
    WorkloadType type = WorkloadType::YCSB;
    size_t workers = 4;
    size_t transactions = 2000;
    uint32_t records = 1000;
    int ops_per_txn = 8;        // ycsb only
    double write_ratio = 0.5;   // ycsb only
    double theta = 0.6;         // Zipf skew: 0 = uniform, ~0.99 = very hot keys
    bool ordered_access = false;  // sort keys inside a transaction (avoids deadlocks by design)
    unsigned seed = 42;
    int sample_limit = 200;       // number of per-transaction samples to export
    uint64_t think_micros = 0;    // artificial delay after each op (0 = none): lengthens lock hold time
};

struct TxnSample {
    uint64_t txn_ref;
    int attempts;
    double latency_us;  // first begin -> successful commit, includes retries
    int reads, writes;
};

struct WorkloadResult {
    double elapsed_ms = 0;
    uint64_t committed = 0;
    uint64_t gave_up = 0;           // exceeded the retry cap (should be 0)
    uint64_t aborted_attempts = 0;  // total aborted attempts (each retried)
    uint64_t aborted_wait_die = 0, aborted_no_wait = 0, aborted_deadlock = 0;
    uint64_t lock_waits = 0;
    uint64_t write_ops_committed = 0;
    double throughput_tps = 0;      // committed / elapsed seconds
    double avg_latency_us = 0, p50_latency_us = 0, p95_latency_us = 0, p99_latency_us = 0;
    bool invariant_ok = false;
    std::string invariant_detail;
    std::vector<TxnSample> samples;
};

const char* to_string(WorkloadType t);
bool parse_workload(const std::string& text, WorkloadType& out);

// Runs the workload on an existing engine (fresh or recovered; invariants use sums measured
// before the run). Throws std::invalid_argument on bad configuration.
WorkloadResult run_workload(TransactionManager& engine, const WorkloadConfig& cfg);

}  // namespace itx
