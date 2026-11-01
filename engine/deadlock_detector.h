// Deadlock handling building blocks.
//
//  * WaitForGraph + victim selection   : DATABASE deadlock DETECTION (used by LockManager).
//  * Banker's algorithm / multi-instance detection : OS resource-allocation concepts
//    (avoidance and detection). These are pure algorithms, separate from the DB lock manager.
//
// Prevention (Wait-Die / NO_WAIT) lives in lock_manager.cpp: it never builds a graph at all.
#pragma once
#include <functional>
#include <map>
#include <set>
#include <utility>
#include <vector>

#include "types.h"

namespace itx {

class WaitForGraph {
public:
    void add_edge(TxnId waiter, TxnId holder);  // waiter -> holder ("waiter waits for holder")
    void remove_node(TxnId txn);
    bool has_edge(TxnId waiter, TxnId holder) const;
    bool empty() const { return adj_.empty(); }
    std::vector<std::pair<TxnId, TxnId>> edges() const;
    // Returns the nodes of one cycle (in order), or an empty vector if the graph is acyclic.
    std::vector<TxnId> find_cycle() const;

private:
    std::map<TxnId, std::set<TxnId>> adj_;
};

enum class VictimPolicy { YOUNGEST, FEWEST_LOCKS };

// Picks the transaction to abort. FEWEST_LOCKS minimises lost work (ties -> youngest).
TxnId choose_victim(const std::vector<TxnId>& cycle, VictimPolicy policy,
                    const std::function<uint64_t(TxnId)>& timestamp_of,
                    const std::function<size_t(TxnId)>& locks_held);

// ---------- OS resource deadlocks (multiple instances per resource type) ----------
using Matrix = std::vector<std::vector<int>>;

struct SafetyResult {
    bool safe = false;
    std::vector<int> safe_sequence;  // process indices
};
// Deadlock AVOIDANCE: Banker's safety algorithm. need = max - allocation.
SafetyResult bankers_safety(const std::vector<int>& available, const Matrix& allocation,
                            const Matrix& max_demand);
// Banker's resource-request algorithm: grants only if the resulting state is safe.
struct RequestResult {
    bool granted = false;
    std::string reason;
};
RequestResult bankers_request(const std::vector<int>& available, const Matrix& allocation,
                              const Matrix& max_demand, int process,
                              const std::vector<int>& request);

struct DetectionResult {
    bool deadlocked = false;
    std::vector<int> deadlocked_processes;
};
// Deadlock DETECTION for multi-instance resources (current outstanding requests matrix).
DetectionResult detect_deadlock(const std::vector<int>& available, const Matrix& allocation,
                                const Matrix& request);

}  // namespace itx
