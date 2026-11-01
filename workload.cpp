#include "workload.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <mutex>
#include <random>
#include <set>
#include <stdexcept>
#include <thread>

#include "worker_pool.h"
#include "zipf.h"

namespace itx {

const char* to_string(WorkloadType t) { return t == WorkloadType::TRANSFER ? "transfer" : "ycsb"; }

bool parse_workload(const std::string& text, WorkloadType& out) {
    if (text == "transfer") out = WorkloadType::TRANSFER;
    else if (text == "ycsb") out = WorkloadType::YCSB;
    else return false;
    return true;
}

namespace {

struct Op {
    Key key;
    bool write;
};
struct TxnSpec {
    std::vector<Op> ops;
    Value amount = 0;  // transfer only
};

struct ThreadStats {
    std::vector<double> latencies;
    std::vector<TxnSample> samples;
    uint64_t committed = 0, gave_up = 0, aborted_attempts = 0;
    uint64_t write_ops = 0;
};

double percentile(std::vector<double>& v, double p) {
    if (v.empty()) return 0;
    size_t idx = static_cast<size_t>(std::ceil(p * v.size())) - 1;
    return v[std::min(idx, v.size() - 1)];
}
}  // namespace

WorkloadResult run_workload(TransactionManager& engine, const WorkloadConfig& cfg) {
    if (cfg.workers == 0) throw std::invalid_argument("workers must be > 0");
    if (cfg.transactions == 0) throw std::invalid_argument("transactions must be > 0");
    if (cfg.records < 2 || cfg.records > engine.config().num_records)
        throw std::invalid_argument("records must be in [2, engine num_records]");
    if (cfg.theta < 0 || cfg.theta > 1.5) throw std::invalid_argument("theta must be in [0, 1.5]");
    if (cfg.write_ratio < 0 || cfg.write_ratio > 1) throw std::invalid_argument("write_ratio in [0,1]");
    const int ops = cfg.type == WorkloadType::TRANSFER ? 2 : cfg.ops_per_txn;
    if (ops < 1 || static_cast<uint32_t>(ops) > cfg.records)
        throw std::invalid_argument("ops_per_txn must be in [1, records]");

    // ---- deterministic workload generation (same seed => same transactions) ----
    std::mt19937_64 rng(cfg.seed);
    ZipfGenerator zipf(cfg.records, cfg.theta);
    std::uniform_real_distribution<double> coin(0.0, 1.0);
    std::vector<TxnSpec> specs(cfg.transactions);
    for (auto& spec : specs) {
        std::set<Key> chosen;
        std::vector<Key> keys;
        while (static_cast<int>(keys.size()) < ops) {
            Key k = zipf.next(rng);
            if (chosen.insert(k).second) keys.push_back(k);
        }
        if (cfg.ordered_access) std::sort(keys.begin(), keys.end());
        for (Key k : keys)
            spec.ops.push_back({k, cfg.type == WorkloadType::TRANSFER ? true : coin(rng) < cfg.write_ratio});
        spec.amount = static_cast<Value>(1 + rng() % 50);
    }

    const Value sum_before = engine.sum_all();
    const EngineStats stats_before = engine.stats();
    const LockStats lock_before = engine.locks().stats();

    std::vector<ThreadStats> per_worker(cfg.workers);
    std::mutex slot_mu;
    std::vector<size_t> free_slots;
    for (size_t i = 0; i < cfg.workers; ++i) free_slots.push_back(i);
    std::atomic<size_t> sample_budget{static_cast<size_t>(std::max(0, cfg.sample_limit))};
    const uint64_t max_attempts = 100000;

    auto run_one = [&](const TxnSpec& spec, ThreadStats& ts, std::mt19937& backoff_rng) {
        auto t0 = std::chrono::steady_clock::now();
        uint64_t timestamp = 0;
        int attempts = 0;
        for (;;) {
            ++attempts;
            auto txn = engine.begin(timestamp);
            timestamp = txn->timestamp;  // keep our age across restarts (prevents starvation)
            if (engine.config().policy == LockPolicy::AVOID) {
                std::vector<Key> claim;
                for (const Op& op : spec.ops) claim.push_back(op.key);
                engine.declare(*txn, claim);
            }
            Status st = Status::OK;
            int reads = 0, writes = 0;
            if (cfg.type == WorkloadType::TRANSFER) {
                Value a = 0, b = 0;
                st = engine.read_for_update(*txn, spec.ops[0].key, a);
                if (st == Status::OK && cfg.think_micros) std::this_thread::sleep_for(std::chrono::microseconds(cfg.think_micros));
                if (st == Status::OK) st = engine.read_for_update(*txn, spec.ops[1].key, b);
                if (st == Status::OK) st = engine.write(*txn, spec.ops[0].key, a - spec.amount);
                if (st == Status::OK) st = engine.write(*txn, spec.ops[1].key, b + spec.amount);
                reads = 2; writes = 2;
            } else {
                for (const Op& op : spec.ops) {
                    Value v = 0;
                    if (op.write) {
                        st = engine.read_for_update(*txn, op.key, v);
                        if (st == Status::OK) st = engine.write(*txn, op.key, v + 1);
                        ++writes;
                    } else {
                        st = engine.read(*txn, op.key, v);
                        ++reads;
                    }
                    if (st != Status::OK) break;
                    if (cfg.think_micros) std::this_thread::sleep_for(std::chrono::microseconds(cfg.think_micros));
                }
            }
            if (st == Status::OK) st = engine.commit(*txn);
            if (st == Status::OK) {
                double us = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - t0).count();
                ++ts.committed;
                ts.write_ops += static_cast<uint64_t>(cfg.type == WorkloadType::YCSB ? writes : 0);
                ts.latencies.push_back(us);
                size_t budget = sample_budget.load();
                if (budget > 0 && sample_budget.compare_exchange_strong(budget, budget - 1))
                    ts.samples.push_back({txn->id, attempts, us, reads, writes});
                return;
            }
            engine.rollback(*txn, st);  // no-op if the engine already rolled back
            ++ts.aborted_attempts;
            if (static_cast<uint64_t>(attempts) >= max_attempts || !is_abort(st)) {
                ++ts.gave_up;
                return;
            }
            // randomised exponential backoff, capped
            int cap = 1 << std::min(attempts, 10);
            std::this_thread::sleep_for(std::chrono::microseconds(1 + backoff_rng() % cap));
        }
    };

    auto start = std::chrono::steady_clock::now();
    {
        WorkerPool pool(cfg.workers, cfg.workers * 4);
        for (size_t i = 0; i < specs.size(); ++i) {
            pool.submit([&, i] {
                static thread_local std::mt19937 backoff_rng(std::random_device{}());
                size_t slot;
                {
                    std::lock_guard<std::mutex> g(slot_mu);
                    slot = free_slots.back();
                    free_slots.pop_back();
                }
                run_one(specs[i], per_worker[slot], backoff_rng);
                std::lock_guard<std::mutex> g(slot_mu);
                free_slots.push_back(slot);
            });
        }
        pool.wait_idle();
        pool.shutdown();
    }
    auto end = std::chrono::steady_clock::now();

    WorkloadResult res;
    res.elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();
    std::vector<double> all;
    for (auto& w : per_worker) {
        res.committed += w.committed;
        res.gave_up += w.gave_up;
        res.aborted_attempts += w.aborted_attempts;
        res.write_ops_committed += w.write_ops;
        all.insert(all.end(), w.latencies.begin(), w.latencies.end());
        res.samples.insert(res.samples.end(), w.samples.begin(), w.samples.end());
    }
    std::sort(all.begin(), all.end());
    if (!all.empty()) {
        double sum = 0;
        for (double d : all) sum += d;
        res.avg_latency_us = sum / all.size();
        res.p50_latency_us = percentile(all, 0.50);
        res.p95_latency_us = percentile(all, 0.95);
        res.p99_latency_us = percentile(all, 0.99);
    }
    res.throughput_tps = res.elapsed_ms > 0 ? res.committed / (res.elapsed_ms / 1000.0) : 0;
    EngineStats sa = engine.stats();
    res.aborted_wait_die = sa.aborted_wait_die - stats_before.aborted_wait_die;
    res.aborted_no_wait = sa.aborted_no_wait - stats_before.aborted_no_wait;
    res.aborted_deadlock = sa.aborted_deadlock - stats_before.aborted_deadlock;
    res.lock_waits = engine.locks().stats().waits - lock_before.waits;

    const Value sum_after = engine.sum_all();
    if (cfg.type == WorkloadType::TRANSFER) {
        res.invariant_ok = sum_after == sum_before && res.gave_up == 0;
        res.invariant_detail = "sum of balances before=" + std::to_string(sum_before) +
                               " after=" + std::to_string(sum_after) + " (must be equal)";
    } else {
        Value expected = sum_before + static_cast<Value>(res.write_ops_committed);
        res.invariant_ok = sum_after == expected && res.gave_up == 0;
        res.invariant_detail = "final sum=" + std::to_string(sum_after) + " expected=" +
                               std::to_string(expected) + " (initial + committed write ops)";
    }
    if (engine.config().policy == LockPolicy::NONE)
        res.invariant_detail += " [policy NONE is unsafe by design]";
    return res;
}

}  // namespace itx
