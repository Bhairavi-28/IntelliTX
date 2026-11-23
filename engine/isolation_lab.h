// Isolation-level ANOMALY LAB (measured on the real engine: real threads, real locks).
//
// Each scenario is a fixed interleaving of two transactions, T1 (older) and T2. A coordinator issues
// the steps in order; every transaction runs in its own thread. If a step blocks on a lock the
// coordinator records it as BLOCKED and moves on (the transaction's later steps queue behind it),
// exactly like two users typing into two SQL sessions. The executed history is analysed with the
// precedence-graph checker, so every anomaly is ALSO visible as a cycle (or a non-recoverable read).
//
//   dirty_read          T1: W(A=150) ......... ABORT        T2: R(A)  COMMIT
//   non_repeatable_read T1: R(A) ......... R(A) COMMIT      T2: W(A=150) COMMIT
//   lost_update         T1: R(A) .. W(A=r+10) COMMIT        T2: R(A) .. W(A=r+20) COMMIT
//   phantom             T1: COUNT(bal>=100) .. COUNT(...)   T2: W(E=200) COMMIT   (E starts at 50)
//
// Live run: a small concurrent workload (few hot rows, read-then-write) at a chosen level whose full
// history is returned for the precedence graph and the lost-update invariant.
#pragma once
#include <string>
#include <vector>

#include "serializability.h"
#include "transaction_manager.h"

namespace itx {

struct LabStep {
    enum Kind { READ, WRITE_SET, WRITE_ADD_LAST_READ, SCAN_COUNT, COMMIT, ABORT };
    int txn = 1;     // 1 or 2
    Kind kind = READ;
    Key key = 0;
    Value arg = 0;   // value to set / delta to add / scan threshold
};

struct LabEvent {
    int step = 0;              // 1-based position in the script
    int txn = 1;
    std::string label;         // e.g. "R(A)", "W(A = 100+10)", "COUNT(balance>=100)"
    std::string status;        // done | blocked_then_done | aborted | skipped
    std::string note;          // e.g. "read 150", "deadlock victim", "count = 4"
    long issued_tick = 0;      // logical clock when the coordinator issued the step
    long finished_tick = 0;    // logical clock when the step finished
    bool waited = false;       // the step was blocked on a lock
    Value value = 0;           // value read / written / counted
};

struct AnomalyResult {
    std::string scenario, title, anomaly_name, question;
    std::string isolation;     // RU / RC / RR / SER
    std::string isolation_long;
    std::string policy;
    std::vector<std::string> item_names;
    std::vector<Value> initial_values, final_values;
    std::vector<LabEvent> events;
    bool t1_committed = false, t2_committed = false;
    bool anomaly = false;      // the anomaly HAPPENED in this run
    std::string verdict;       // human explanation
    std::vector<HistoryOp> history;
    SerializabilityResult analysis;
    long lock_waits = 0, deadlocks = 0;
};

std::vector<std::string> anomaly_scenarios();  // ids in display order
// Throws std::invalid_argument for an unknown scenario or policy NONE/AVOID.
AnomalyResult run_anomaly(const std::string& scenario, IsolationLevel level,
                          LockPolicy policy = LockPolicy::DEADLOCK_DETECT);

struct LiveRunConfig {
    IsolationLevel isolation = IsolationLevel::SERIALIZABLE;
    LockPolicy policy = LockPolicy::DEADLOCK_DETECT;
    int transactions = 6;   // committed transactions to produce (graph stays readable: <= 12)
    int workers = 3;
    uint32_t records = 3;   // few rows => many conflicts
    int ops_per_txn = 3;
    double write_ratio = 0.5;
    int think_us = 300;     // pause between operations to force interleaving
    unsigned seed = 7;
};

struct LiveRunResult {
    std::string isolation, policy;
    std::vector<HistoryOp> history;
    SerializabilityResult analysis;
    uint64_t committed = 0, aborted_attempts = 0;
    long expected_increment = 0, actual_increment = 0;  // committed +1 writes vs observed growth
    long lost_updates = 0;
};

LiveRunResult run_live_schedule(const LiveRunConfig& cfg);

}  // namespace itx
