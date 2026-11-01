#include "lock_manager.h"

#include <sstream>

namespace itx {

LockManager::LockManager(LockPolicy policy) : policy_(policy) {}

void LockManager::set_trace(TraceFn fn) {
    std::lock_guard<std::mutex> g(mu_);
    trace_ = std::move(fn);
}

void LockManager::trace_locked(const std::string& event, TxnId txn, const std::string& detail) {
    if (trace_) trace_(event, txn, detail);
}

bool LockManager::older_locked(TxnId a, TxnId b) const {
    uint64_t ta = txns_.at(a).timestamp, tb = txns_.at(b).timestamp;
    return ta < tb || (ta == tb && a < b);
}

void LockManager::collect_blockers_locked(TxnId txn, Key key, LockMode mode,
                                          std::vector<TxnId>& out) const {
    out.clear();
    auto it = table_.find(key);
    if (it == table_.end()) return;
    for (const auto& [holder, held_mode] : it->second.holders) {
        if (holder == txn) continue;
        if (mode == LockMode::EXCLUSIVE || held_mode == LockMode::EXCLUSIVE) out.push_back(holder);
    }
}

WaitForGraph LockManager::build_graph_locked() const {
    WaitForGraph g;
    std::vector<TxnId> blockers;
    for (const auto& [id, info] : txns_) {
        if (!info.waiting || victims_.count(id)) continue;  // a chosen victim no longer waits
        collect_blockers_locked(id, info.wait_key, info.wait_mode, blockers);
        for (TxnId b : blockers) g.add_edge(id, b);
    }
    return g;
}

void LockManager::run_detection_locked() {
    for (;;) {
        WaitForGraph g = build_graph_locked();
        std::vector<TxnId> cycle = g.find_cycle();
        if (cycle.empty()) return;
        TxnId victim = choose_victim(
            cycle, VictimPolicy::FEWEST_LOCKS,
            [this](TxnId t) { return txns_.at(t).timestamp; },
            [this](TxnId t) { return txns_.at(t).held.size(); });
        victims_.insert(victim);
        ++stats_.deadlock_cycles;
        ++stats_.deadlock_victims;
        std::ostringstream os;
        os << "cycle:";
        for (TxnId t : cycle) os << " T" << t << " ->";
        os << " T" << cycle.front() << "; wait-for edges:";
        for (const auto& [from, to] : g.edges()) os << " T" << from << "->T" << to;
        os << "; victim=T" << victim << " (fewest locks, then youngest)";
        trace_locked("DEADLOCK_DETECTED", victim, os.str());
        cv_.notify_all();
    }
}

void LockManager::declare(TxnId txn, uint64_t timestamp, const std::vector<Key>& claim) {
    if (policy_ != LockPolicy::AVOID) return;
    std::lock_guard<std::mutex> g(mu_);
    TxnInfo& info = txns_[txn];
    info.timestamp = timestamp;
    info.declared = true;
    info.claim.insert(claim.begin(), claim.end());
}

// Banker's safety algorithm. Resources = keys (one instance each). A state is SAFE if the declared
// transactions can finish in some order: a transaction can finish when every key in its claim is
// free (or already its own); when it finishes it frees what it holds.
bool LockManager::safe_after_grant_locked(TxnId txn, Key key) const {
    std::unordered_map<Key, TxnId> owner;
    for (const auto& [k, e] : table_)
        for (const auto& [h, m] : e.holders) owner[k] = h;
    owner[key] = txn;  // hypothetical grant
    std::vector<TxnId> pending;
    for (const auto& [id, info] : txns_)
        if (info.declared) pending.push_back(id);
    std::unordered_set<Key> freed;
    std::unordered_set<TxnId> finished;
    for (bool progress = true; progress;) {
        progress = false;
        for (TxnId id : pending) {
            if (finished.count(id)) continue;
            bool can_finish = true;
            for (Key k : txns_.at(id).claim) {
                auto o = owner.find(k);
                if (o != owner.end() && o->second != id && !freed.count(k)) { can_finish = false; break; }
            }
            if (!can_finish) continue;
            finished.insert(id);
            for (const auto& [k, h] : owner)
                if (h == id) freed.insert(k);
            progress = true;
        }
    }
    return finished.size() == pending.size();
}

Status LockManager::acquire(TxnId txn, uint64_t timestamp, Key key, LockMode mode) {
    if (policy_ == LockPolicy::NONE) return Status::OK;
    if (policy_ == LockPolicy::AVOID) mode = LockMode::EXCLUSIVE;  // conservative: avoidance reasons about exclusive use
    std::unique_lock<std::mutex> lk(mu_);
    TxnInfo& info = txns_[txn];
    info.timestamp = timestamp;
    bool counted_wait = false;

    for (;;) {
        TxnInfo& me = txns_[txn];  // re-fetch: rehashing may have moved the node
        if (victims_.erase(txn)) {
            me.waiting = false;
            trace_locked("ABORT_DEADLOCK", txn, "chosen as deadlock victim");
            return Status::ABORT_DEADLOCK;
        }
        Entry& entry = table_[key];
        auto self = entry.holders.find(txn);
        if (self != entry.holders.end() &&
            (self->second == LockMode::EXCLUSIVE || mode == LockMode::SHARED)) {
            me.waiting = false;
            return Status::OK;  // already holds a sufficient lock
        }

        std::vector<TxnId> blockers;
        collect_blockers_locked(txn, key, mode, blockers);
        if (policy_ == LockPolicy::AVOID) {
            if (!me.declared || !me.claim.count(key)) {  // avoidance needs the key in the declared claim
                me.waiting = false;
                if (entry.holders.empty()) table_.erase(key);
                return Status::ERR_BAD_STATE;
            }
            if (blockers.empty() && safe_after_grant_locked(txn, key)) {
                entry.holders[txn] = LockMode::EXCLUSIVE;
                me.held.insert(key);
                me.waiting = false;
                ++stats_.grants;
                return Status::OK;
            }
            if (blockers.empty() && !counted_wait) {
                ++stats_.unsafe_denials;
                trace_locked("AVOID_DENIED", txn, "key " + std::to_string(key) + " is free but granting it would leave NO safe sequence (Banker's check): waiting instead of risking deadlock");
            }
            if (blockers.empty() && entry.holders.empty()) table_.erase(key);
            me.waiting = true;
            me.wait_key = key;
            me.wait_mode = mode;
            if (!counted_wait) {
                counted_wait = true;
                ++stats_.waits;
                trace_locked("LOCK_WAIT", txn, "waiting for key " + std::to_string(key));
            }
            cv_.wait(lk);
            continue;
        }
        if (blockers.empty()) {
            if (self != entry.holders.end()) {
                self->second = LockMode::EXCLUSIVE;  // upgrade S -> X (sole holder)
                ++stats_.upgrades;
            } else {
                entry.holders[txn] = mode;
            }
            me.held.insert(key);
            me.waiting = false;
            ++stats_.grants;
            return Status::OK;
        }

        // ---- conflict ----
        if (policy_ == LockPolicy::NO_WAIT) {
            me.waiting = false;
            ++stats_.no_wait_aborts;
            trace_locked("ABORT_NO_WAIT", txn, "conflict on key " + std::to_string(key));
            return Status::ABORT_NO_WAIT;
        }
        if (policy_ == LockPolicy::WAIT_DIE) {
            for (TxnId b : blockers) {
                if (older_locked(b, txn)) {  // requester is younger than a holder -> dies
                    me.waiting = false;
                    ++stats_.wait_die_aborts;
                    trace_locked("ABORT_WAIT_DIE", txn,
                                 "younger than holder T" + std::to_string(b) + " on key " +
                                     std::to_string(key));
                    return Status::ABORT_WAIT_DIE;
                }
            }
        }
        me.waiting = true;
        me.wait_key = key;
        me.wait_mode = mode;
        if (!counted_wait) {
            counted_wait = true;
            ++stats_.waits;
            trace_locked("LOCK_WAIT", txn,
                         std::string("waiting for ") + to_string(mode) + " lock on key " +
                             std::to_string(key));
        }
        if (policy_ == LockPolicy::DEADLOCK_DETECT) {
            run_detection_locked();
            if (victims_.count(txn)) continue;  // we are the victim: handled at loop top
        }
        cv_.wait(lk);
    }
}

void LockManager::release_all(TxnId txn) {
    if (policy_ == LockPolicy::NONE) return;
    std::lock_guard<std::mutex> g(mu_);
    auto it = txns_.find(txn);
    if (it != txns_.end()) {
        for (Key k : it->second.held) {
            auto e = table_.find(k);
            if (e == table_.end()) continue;
            e->second.holders.erase(txn);
            if (e->second.holders.empty()) table_.erase(e);
        }
        txns_.erase(it);
    }
    victims_.erase(txn);
    cv_.notify_all();
}

bool LockManager::holds(TxnId txn, Key key, LockMode at_least) const {
    std::lock_guard<std::mutex> g(mu_);
    auto e = table_.find(key);
    if (e == table_.end()) return false;
    auto h = e->second.holders.find(txn);
    if (h == e->second.holders.end()) return false;
    return at_least == LockMode::SHARED || h->second == LockMode::EXCLUSIVE;
}

size_t LockManager::locks_held(TxnId txn) const {
    std::lock_guard<std::mutex> g(mu_);
    auto it = txns_.find(txn);
    return it == txns_.end() ? 0 : it->second.held.size();
}

size_t LockManager::total_lock_entries() const {
    std::lock_guard<std::mutex> g(mu_);
    return table_.size();
}

size_t LockManager::waiting_count() const {
    std::lock_guard<std::mutex> g(mu_);
    size_t n = 0;
    for (const auto& [id, info] : txns_) n += info.waiting ? 1 : 0;
    return n;
}

WaitForGraph LockManager::wait_for_graph() const {
    std::lock_guard<std::mutex> g(mu_);
    return build_graph_locked();
}

LockStats LockManager::stats() const {
    std::lock_guard<std::mutex> g(mu_);
    return stats_;
}

}  // namespace itx
