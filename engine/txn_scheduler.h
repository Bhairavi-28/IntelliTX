// REAL (non-preemptive) CPU-scheduling policy applied to transactions: a bounded ready queue from
// which a free worker takes the NEXT transaction according to the policy.
//   FCFS     : arrival order            SJF : smallest cost first        PRIORITY : lowest priority number first
// Preemptive policies (SRTF, RR, preemptive priority) are rejected on purpose: a descheduled
// transaction would keep holding its locks (Strict 2PL) and could stall every other worker.
#pragma once
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <vector>

#include "scheduler.h"

namespace itx {

struct ReadyJob {
    size_t index = 0;
    uint64_t seq = 0;   // arrival order
    double cost = 0;    // estimated work (SJF)
    int priority = 1;   // lower = more important
    std::chrono::steady_clock::time_point arrived;  // stamped by push()
};

class TxnScheduler {
public:
    TxnScheduler(SchedAlgo policy, size_t capacity) : policy_(policy), capacity_(capacity) {
        if (capacity == 0) throw std::invalid_argument("ready-queue capacity must be > 0");
        if (policy != SchedAlgo::FCFS && policy != SchedAlgo::SJF && policy != SchedAlgo::PRIORITY)
            throw std::invalid_argument("only FCFS, SJF and PRIORITY can schedule transactions (a preempted transaction would keep its locks)");
    }
    bool push(ReadyJob job) {  // blocks while the ready queue is full
        std::unique_lock<std::mutex> lk(m_);
        not_full_.wait(lk, [this] { return closed_ || ready_.size() < capacity_; });
        if (closed_) return false;
        job.arrived = std::chrono::steady_clock::now();
        ready_.push_back(job);
        lk.unlock();
        not_empty_.notify_one();
        return true;
    }
    bool pop(ReadyJob& out) {  // the policy chooses among ALL currently ready transactions
        std::unique_lock<std::mutex> lk(m_);
        not_empty_.wait(lk, [this] { return closed_ || !ready_.empty(); });
        if (ready_.empty()) return false;
        auto best = ready_.begin();
        for (auto it = ready_.begin(); it != ready_.end(); ++it)
            if (better(*it, *best)) best = it;
        out = *best;
        ready_.erase(best);
        lk.unlock();
        not_full_.notify_one();
        return true;
    }
    void close() {
        { std::lock_guard<std::mutex> g(m_); closed_ = true; }
        not_full_.notify_all();
        not_empty_.notify_all();
    }

private:
    bool better(const ReadyJob& a, const ReadyJob& b) const {
        switch (policy_) {
            case SchedAlgo::SJF: if (a.cost != b.cost) return a.cost < b.cost; break;
            case SchedAlgo::PRIORITY: if (a.priority != b.priority) return a.priority < b.priority; break;
            default: break;
        }
        return a.seq < b.seq;
    }
    SchedAlgo policy_;
    size_t capacity_;
    std::mutex m_;
    std::condition_variable not_full_, not_empty_;
    std::vector<ReadyJob> ready_;
    bool closed_ = false;
};

}  // namespace itx
