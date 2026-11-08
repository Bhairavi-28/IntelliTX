// Deterministic, self-checking demonstrations used by the CLI, the dashboard and the tests:
//   * a two-transaction deadlock under each lock policy,
//   * crash-recovery scenarios (a child process is killed with _exit, then the engine restarts).
#pragma once
#include <string>
#include <vector>

#include "transaction_manager.h"

namespace itx {

struct ScenarioEvent {
    int seq;
    std::string event;
    TxnId txn;
    std::string detail;
};

struct DeadlockScenarioResult {
    std::string policy;
    std::string mechanism;  // "prevention" / "detection" / "none"
    bool deadlock_cycle_detected = false;
    std::vector<ScenarioEvent> events;
    int t1_attempts = 0, t2_attempts = 0;
    bool t1_committed = false, t2_committed = false;
    uint64_t aborted_wait_die = 0, aborted_no_wait = 0, aborted_deadlock = 0;
    Value final_a = 0, final_b = 0;
    bool consistent = false;  // both transactions committed exactly once: A == B == 2
    uint64_t unsafe_denials = 0;   // AVOID: lock requests delayed by the Banker's check
    size_t lock_entries_left = 0;  // must be 0 (no lock leak)
};

// T1 (older) locks A then B; T2 locks B then A: a classic circular wait.
// Throws std::invalid_argument for policy NONE.
DeadlockScenarioResult run_deadlock_scenario(LockPolicy policy);

struct ScenarioCheck {
    std::string name;
    std::string expected;
    std::string actual;
    bool pass;
};

struct RecoveryScenarioResult {
    std::string scenario;
    std::vector<ScenarioCheck> checks;
    RecoveryReport report;  // report of the first restart after the crash
    bool all_pass = false;
};

// scenario: "crash" | "checkpoint" | "torn_tail". `dir` must be empty or non-existent.
// Throws std::invalid_argument for an unknown scenario, std::runtime_error if fork fails.
RecoveryScenarioResult run_recovery_scenario(const std::string& dir, const std::string& scenario);

}  // namespace itx
