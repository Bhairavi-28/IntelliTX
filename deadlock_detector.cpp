#include "deadlock_detector.h"

#include <algorithm>
#include <stdexcept>

namespace itx {

void WaitForGraph::add_edge(TxnId waiter, TxnId holder) {
    if (waiter == holder) return;
    adj_[waiter].insert(holder);
}

void WaitForGraph::remove_node(TxnId txn) {
    adj_.erase(txn);
    for (auto it = adj_.begin(); it != adj_.end();) {
        it->second.erase(txn);
        if (it->second.empty()) it = adj_.erase(it);
        else ++it;
    }
}

bool WaitForGraph::has_edge(TxnId waiter, TxnId holder) const {
    auto it = adj_.find(waiter);
    return it != adj_.end() && it->second.count(holder) > 0;
}

std::vector<std::pair<TxnId, TxnId>> WaitForGraph::edges() const {
    std::vector<std::pair<TxnId, TxnId>> out;
    for (const auto& [from, tos] : adj_)
        for (TxnId to : tos) out.emplace_back(from, to);
    return out;
}

namespace {
enum class Color { WHITE, GREY, BLACK };

bool dfs(TxnId node, const std::map<TxnId, std::set<TxnId>>& adj,
         std::map<TxnId, Color>& color, std::vector<TxnId>& path, std::vector<TxnId>& cycle) {
    color[node] = Color::GREY;
    path.push_back(node);
    auto it = adj.find(node);
    if (it != adj.end()) {
        for (TxnId next : it->second) {
            Color c = color.count(next) ? color[next] : Color::WHITE;
            if (c == Color::GREY) {
                auto start = std::find(path.begin(), path.end(), next);
                cycle.assign(start, path.end());
                return true;
            }
            if (c == Color::WHITE && dfs(next, adj, color, path, cycle)) return true;
        }
    }
    path.pop_back();
    color[node] = Color::BLACK;
    return false;
}
}  // namespace

std::vector<TxnId> WaitForGraph::find_cycle() const {
    std::map<TxnId, Color> color;
    std::vector<TxnId> path, cycle;
    for (const auto& [node, _] : adj_) {
        if (!color.count(node) || color[node] == Color::WHITE) {
            path.clear();
            if (dfs(node, adj_, color, path, cycle)) return cycle;
        }
    }
    return {};
}

TxnId choose_victim(const std::vector<TxnId>& cycle, VictimPolicy policy,
                    const std::function<uint64_t(TxnId)>& timestamp_of,
                    const std::function<size_t(TxnId)>& locks_held) {
    if (cycle.empty()) throw std::invalid_argument("choose_victim: empty cycle");
    TxnId best = cycle.front();
    for (TxnId t : cycle) {
        bool better = false;
        if (policy == VictimPolicy::FEWEST_LOCKS) {
            size_t a = locks_held(t), b = locks_held(best);
            better = a < b || (a == b && (timestamp_of(t) > timestamp_of(best) ||
                                          (timestamp_of(t) == timestamp_of(best) && t > best)));
        } else {
            better = timestamp_of(t) > timestamp_of(best) ||
                     (timestamp_of(t) == timestamp_of(best) && t > best);
        }
        if (better) best = t;
    }
    return best;
}

// ------------------------------------------------------------------------------------------

namespace {
bool leq(const std::vector<int>& a, const std::vector<int>& b) {
    for (size_t i = 0; i < a.size(); ++i)
        if (a[i] > b[i]) return false;
    return true;
}
void check_shapes(const std::vector<int>& avail, const Matrix& m1, const Matrix& m2) {
    if (m1.size() != m2.size()) throw std::invalid_argument("matrix row mismatch");
    for (const auto& r : m1)
        if (r.size() != avail.size()) throw std::invalid_argument("matrix column mismatch");
    for (const auto& r : m2)
        if (r.size() != avail.size()) throw std::invalid_argument("matrix column mismatch");
}
}  // namespace

SafetyResult bankers_safety(const std::vector<int>& available, const Matrix& allocation,
                            const Matrix& max_demand) {
    check_shapes(available, allocation, max_demand);
    const size_t n = allocation.size(), m = available.size();
    Matrix need(n, std::vector<int>(m));
    for (size_t i = 0; i < n; ++i)
        for (size_t j = 0; j < m; ++j) {
            need[i][j] = max_demand[i][j] - allocation[i][j];
            if (need[i][j] < 0) throw std::invalid_argument("allocation exceeds max demand");
        }
    std::vector<int> work = available;
    std::vector<bool> finished(n, false);
    SafetyResult result;
    for (size_t round = 0; round < n; ++round) {
        bool progressed = false;
        for (size_t i = 0; i < n; ++i) {
            if (!finished[i] && leq(need[i], work)) {
                for (size_t j = 0; j < m; ++j) work[j] += allocation[i][j];
                finished[i] = true;
                result.safe_sequence.push_back(static_cast<int>(i));
                progressed = true;
                break;  // restart scan from process 0 (deterministic order)
            }
        }
        if (!progressed) break;
    }
    result.safe = result.safe_sequence.size() == n;
    if (!result.safe) result.safe_sequence.clear();
    return result;
}

RequestResult bankers_request(const std::vector<int>& available, const Matrix& allocation,
                              const Matrix& max_demand, int process,
                              const std::vector<int>& request) {
    check_shapes(available, allocation, max_demand);
    RequestResult r;
    if (process < 0 || static_cast<size_t>(process) >= allocation.size())
        throw std::invalid_argument("bad process index");
    const size_t m = available.size();
    if (request.size() != m) throw std::invalid_argument("bad request length");
    for (size_t j = 0; j < m; ++j)
        if (request[j] > max_demand[process][j] - allocation[process][j]) {
            r.reason = "request exceeds the process's declared maximum claim";
            return r;
        }
    if (!leq(request, available)) {
        r.reason = "resources not available: process must wait";
        return r;
    }
    std::vector<int> avail2 = available;
    Matrix alloc2 = allocation;
    for (size_t j = 0; j < m; ++j) {
        avail2[j] -= request[j];
        alloc2[process][j] += request[j];
    }
    if (!bankers_safety(avail2, alloc2, max_demand).safe) {
        r.reason = "granting would lead to an UNSAFE state: request denied";
        return r;
    }
    r.granted = true;
    r.reason = "resulting state is safe: request granted";
    return r;
}

DetectionResult detect_deadlock(const std::vector<int>& available, const Matrix& allocation,
                                const Matrix& request) {
    check_shapes(available, allocation, request);
    const size_t n = allocation.size(), m = available.size();
    std::vector<int> work = available;
    std::vector<bool> finish(n, false);
    for (size_t i = 0; i < n; ++i) {  // a process holding nothing cannot be part of a deadlock
        bool holds_any = false;
        for (size_t j = 0; j < m; ++j) holds_any |= allocation[i][j] != 0;
        if (!holds_any) finish[i] = true;
    }
    bool progressed = true;
    while (progressed) {
        progressed = false;
        for (size_t i = 0; i < n; ++i) {
            if (!finish[i] && leq(request[i], work)) {
                for (size_t j = 0; j < m; ++j) work[j] += allocation[i][j];
                finish[i] = true;
                progressed = true;
            }
        }
    }
    DetectionResult d;
    for (size_t i = 0; i < n; ++i)
        if (!finish[i]) d.deadlocked_processes.push_back(static_cast<int>(i));
    d.deadlocked = !d.deadlocked_processes.empty();
    return d;
}

}  // namespace itx
