// Record-level lock manager: shared/exclusive locks for Strict 2PL.
//
// All state is guarded by one mutex + one condition variable. Every state change that can
// unblock a waiter (release, victim selection) does notify_all under the same mutex and every
// waiter re-evaluates its predicate in a loop, so wake-ups cannot be lost.
// NOTE: a single central lock table is exactly the kind of shared structure the reference paper
// identifies as a many-core bottleneck; it is kept because it is simple and verifiably correct.
#pragma once
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "deadlock_detector.h"
#include "types.h"

namespace itx {

struct LockStats {
    uint64_t grants = 0;
    uint64_t upgrades = 0;
    uint64_t waits = 0;
    uint64_t wait_die_aborts = 0;
    uint64_t no_wait_aborts = 0;
    uint64_t deadlock_cycles = 0;
    uint64_t deadlock_victims = 0;
    uint64_t unsafe_denials = 0;  // AVOID: free lock withheld because granting it would be unsafe
};

// Called with the lock-manager mutex held: must not call back into the LockManager.
using TraceFn = std::function<void(const std::string& event, TxnId txn, const std::string& detail)>;

class LockManager {
public:
    explicit LockManager(LockPolicy policy);

    LockPolicy policy() const { return policy_; }

    // Blocks (WAIT_DIE older txn / DEADLOCK_DETECT) until granted, or returns an ABORT_* status.
    // `timestamp` is the transaction's age (smaller = older) and is kept across restarts.
    // On an ABORT_* result the caller must rollback and call release_all().
    Status acquire(TxnId txn, uint64_t timestamp, Key key, LockMode mode);

    // AVOID policy only: declare every key the transaction may lock (its "maximum claim").
    void declare(TxnId txn, uint64_t timestamp, const std::vector<Key>& claim);

    // Strict 2PL: the ONLY way locks are released is here, at commit/abort time.
    void release_all(TxnId txn);

    bool holds(TxnId txn, Key key, LockMode at_least) const;
    size_t locks_held(TxnId txn) const;
    size_t total_lock_entries() const;  // number of keys with at least one holder
    size_t waiting_count() const;
    WaitForGraph wait_for_graph() const;  // freshly computed from current lock table
    LockStats stats() const;
    void set_trace(TraceFn fn);

private:
    struct Entry {
        std::unordered_map<TxnId, LockMode> holders;
    };
    struct TxnInfo {
        uint64_t timestamp = 0;
        std::unordered_set<Key> held;
        bool waiting = false;
        bool declared = false;
        std::unordered_set<Key> claim;
        Key wait_key = 0;
        LockMode wait_mode = LockMode::SHARED;
    };

    void collect_blockers_locked(TxnId txn, Key key, LockMode mode, std::vector<TxnId>& out) const;
    WaitForGraph build_graph_locked() const;
    void run_detection_locked();
    bool safe_after_grant_locked(TxnId txn, Key key) const;  // Banker's safety check (single-instance resources)
    bool older_locked(TxnId a, TxnId b) const;  // is a older than b ?
    void trace_locked(const std::string& event, TxnId txn, const std::string& detail);

    const LockPolicy policy_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::unordered_map<Key, Entry> table_;
    std::unordered_map<TxnId, TxnInfo> txns_;
    std::unordered_set<TxnId> victims_;
    LockStats stats_;
    TraceFn trace_;
};

}  // namespace itx
