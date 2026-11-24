#include <algorithm>
#include <random>
#include <thread>

#include "test_framework.h"
#include "transaction_manager.h"
#include "workload.h"

using namespace itx;

static EngineConfig small(LockPolicy p = LockPolicy::WAIT_DIE, uint32_t n = 10) {
    EngineConfig c;
    c.num_records = n;
    c.policy = p;
    return c;
}

TEST(txn_commit_makes_changes_visible) {
    TransactionManager tm(small());
    auto t = tm.begin();
    CHECK_EQ(tm.write(*t, 1, 11), Status::OK);
    CHECK_EQ(tm.write(*t, 2, 22), Status::OK);
    CHECK_EQ(tm.commit(*t), Status::OK);
    CHECK(t->state == TxnState::COMMITTED);
    CHECK_EQ(tm.peek(1), 11);
    CHECK_EQ(tm.peek(2), 22);
    CHECK_EQ(tm.stats().committed, 1u);
}

TEST(txn_rollback_restores_previous_state_atomicity) {
    TransactionManager tm(small());
    auto setup = tm.begin();
    tm.write(*setup, 1, 5);
    tm.commit(*setup);
    auto t = tm.begin();
    tm.write(*t, 1, 100);
    tm.write(*t, 1, 200);  // same key twice: undo must restore the ORIGINAL value (5)
    tm.write(*t, 2, 300);
    CHECK_EQ(tm.peek(1), 200);
    tm.rollback(*t);
    CHECK_EQ(tm.peek(1), 5);
    CHECK_EQ(tm.peek(2), 0);
    CHECK(t->state == TxnState::ABORTED);
    tm.rollback(*t);  // idempotent
    CHECK_EQ(tm.stats().aborted, 1u);
    CHECK_EQ(tm.locks().total_lock_entries(), 0u);
}

TEST(txn_rollback_does_not_undo_other_transactions) {
    TransactionManager tm(small());
    auto a = tm.begin();
    auto b = tm.begin();
    tm.write(*a, 1, 10);
    tm.write(*b, 2, 20);
    tm.commit(*b);
    tm.rollback(*a);
    CHECK_EQ(tm.peek(1), 0);
    CHECK_EQ(tm.peek(2), 20);  // b's committed work untouched
}

TEST(txn_operations_after_end_are_rejected) {
    TransactionManager tm(small());
    auto t = tm.begin();
    tm.commit(*t);
    Value v;
    CHECK_EQ(tm.read(*t, 1, v), Status::ERR_BAD_STATE);
    CHECK_EQ(tm.write(*t, 1, 1), Status::ERR_BAD_STATE);
    CHECK_EQ(tm.commit(*t), Status::ERR_BAD_STATE);
    auto u = tm.begin();
    CHECK_EQ(tm.read(*u, 999, v), Status::ERR_BAD_KEY);
    tm.rollback(*u);
}

TEST(txn_abort_by_engine_releases_everything) {
    TransactionManager tm(small(LockPolicy::NO_WAIT));
    auto a = tm.begin();
    auto b = tm.begin();
    CHECK_EQ(tm.write(*a, 1, 1), Status::OK);
    CHECK_EQ(tm.write(*b, 2, 2), Status::OK);
    CHECK_EQ(tm.write(*b, 1, 9), Status::ABORT_NO_WAIT);  // engine rolls b back itself
    CHECK(b->state == TxnState::ABORTED);
    CHECK_EQ(tm.peek(2), 0);                               // b's write on key 2 was undone
    CHECK_EQ(tm.locks().locks_held(b->id), 0u);
    CHECK_EQ(tm.commit(*a), Status::OK);
    CHECK_EQ(tm.locks().total_lock_entries(), 0u);
}

TEST(consistency_invariant_transfer_money) {
    EngineConfig c = small(LockPolicy::WAIT_DIE, 50);
    c.initial_value = 1000;
    TransactionManager tm(c);
    WorkloadConfig w;
    w.type = WorkloadType::TRANSFER;
    w.workers = 4;
    w.transactions = 3000;
    w.records = 50;
    w.theta = 0.8;
    WorkloadResult r = run_workload(tm, w);
    CHECK(r.invariant_ok);
    CHECK_EQ(r.committed, 3000u);
    CHECK_EQ(tm.sum_all(), 50 * 1000);
    CHECK_EQ(tm.locks().total_lock_entries(), 0u);
}

TEST(isolation_no_lost_updates_under_every_safe_policy) {
    for (LockPolicy p : {LockPolicy::WAIT_DIE, LockPolicy::NO_WAIT, LockPolicy::DEADLOCK_DETECT}) {
        EngineConfig c = small(p, 20);
        TransactionManager tm(c);
        WorkloadConfig w;
        w.type = WorkloadType::YCSB;
        w.workers = 8;
        w.transactions = 2000;
        w.records = 20;
        w.ops_per_txn = 4;
        w.write_ratio = 1.0;
        w.theta = 0.9;
        WorkloadResult r = run_workload(tm, w);
        CHECK(r.invariant_ok);
        CHECK_EQ(r.committed, 2000u);
        CHECK_EQ(tm.locks().total_lock_entries(), 0u);
    }
}

// Demonstrates WHY locking is needed: with policy NONE concurrent increments are lost.
// (Probabilistic by nature, so the test only asserts the safe case is exact and reports the unsafe one.)
TEST(isolation_unsafe_baseline_can_lose_updates) {
    EngineConfig c = small(LockPolicy::NONE, 1 + 1);
    TransactionManager tm(c);
    const int kThreads = 4, kIters = 20000;
    std::vector<std::thread> th;
    for (int i = 0; i < kThreads; ++i)
        th.emplace_back([&] {
            for (int k = 0; k < kIters; ++k) {
                auto t = tm.begin();
                Value v;
                tm.read(*t, 0, v);
                tm.write(*t, 0, v + 1);
                tm.commit(*t);
            }
        });
    for (auto& t : th) t.join();
    CHECK(tm.peek(0) <= kThreads * kIters);  // never MORE than expected; usually fewer (lost updates)
    std::cout << "         (policy none: counter=" << tm.peek(0) << " of " << kThreads * kIters << ")\n";
}

TEST(isolation_conflicting_readers_and_writers_see_committed_states_only) {
    // Writer keeps keys 0 and 1 equal; readers must never observe them different (dirty/non-repeatable read).
    TransactionManager tm(small(LockPolicy::DEADLOCK_DETECT, 2));
    std::atomic<bool> stop{false};
    std::atomic<int> violations{0};
    std::thread writer([&] {
        for (int i = 1; i <= 3000; ++i) {
            for (;;) {
                auto t = tm.begin();
                Status s = tm.write(*t, 0, i);
                if (s == Status::OK) s = tm.write(*t, 1, i);
                if (s == Status::OK && tm.commit(*t) == Status::OK) break;
                tm.rollback(*t, s);
            }
        }
        stop = true;
    });
    std::vector<std::thread> readers;
    for (int r = 0; r < 3; ++r)
        readers.emplace_back([&] {
            while (!stop) {
                auto t = tm.begin();
                Value a = 0, b = 0;
                Status s = tm.read(*t, 0, a);
                if (s == Status::OK) s = tm.read(*t, 1, b);
                if (s == Status::OK) {
                    Value a2 = 0;
                    if (tm.read(*t, 0, a2) == Status::OK && a2 != a) ++violations;  // repeatable read
                    if (a != b) ++violations;
                    tm.commit(*t);
                } else {
                    tm.rollback(*t, s);
                }
            }
        });
    writer.join();
    for (auto& r : readers) r.join();
    CHECK_EQ(violations.load(), 0);
}

TEST(workload_is_reproducible_for_a_seed) {
    WorkloadConfig w;
    w.workers = 1;
    w.transactions = 500;
    w.records = 100;
    w.seed = 7;
    EngineConfig c = small(LockPolicy::WAIT_DIE, 100);
    TransactionManager a(c), b(c);
    run_workload(a, w);
    run_workload(b, w);
    for (Key k = 0; k < 100; ++k) CHECK_EQ(a.peek(k), b.peek(k));  // same seed => same final state
}

TEST(workload_validates_configuration) {
    TransactionManager tm(small(LockPolicy::WAIT_DIE, 10));
    WorkloadConfig w;
    w.workers = 0;
    CHECK_THROWS(run_workload(tm, w));
    w = WorkloadConfig{};
    w.records = 20;  // engine has only 10
    CHECK_THROWS(run_workload(tm, w));
    w = WorkloadConfig{};
    w.records = 10;
    w.ops_per_txn = 11;
    CHECK_THROWS(run_workload(tm, w));
}

// Serializability validation against a SERIAL execution.
// Strict 2PL makes the commit order a valid serial order. Each transaction applies a
// NON-commutative, value-dependent function, so any non-serializable interleaving changes some
// observed read value or the final state. We replay the committed transactions serially in commit
// order on a fresh store and require: (a) identical values read by every transaction, (b) identical final state.
namespace {
// Unsigned arithmetic: wrap-around is well defined (signed overflow would be undefined behaviour).
Value mix1(Value a, Value b, int tag) { return static_cast<Value>(static_cast<uint64_t>(a) * 31u + static_cast<uint64_t>(b) + static_cast<uint64_t>(tag)); }
Value mix2(Value a, Value b, int tag) { return static_cast<Value>(static_cast<uint64_t>(b) * 17u + static_cast<uint64_t>(a) + static_cast<uint64_t>(tag)); }
struct Executed {
    uint64_t commit_seq;
    Key k1, k2;
    Value read1, read2;
    int tag;
};
}  // namespace

TEST(serializability_concurrent_execution_equals_serial_replay_in_commit_order) {
    for (LockPolicy p : {LockPolicy::WAIT_DIE, LockPolicy::NO_WAIT, LockPolicy::DEADLOCK_DETECT}) {
        const uint32_t kRecords = 6;  // tiny => heavy conflicts
        const int kThreads = 4, kPerThread = 400;
        EngineConfig c = small(p, kRecords);
        for (Key k = 0; k < kRecords; ++k) { (void)k; }
        TransactionManager tm(c);
        std::vector<std::vector<Executed>> done(kThreads);
        std::vector<std::thread> th;
        for (int t = 0; t < kThreads; ++t)
            th.emplace_back([&, t] {
                std::mt19937 rng(100 + t);
                for (int i = 0; i < kPerThread; ++i) {
                    Key k1 = rng() % kRecords, k2 = rng() % kRecords;
                    int tag = t * kPerThread + i + 1;
                    uint64_t ts = 0;
                    for (;;) {
                        auto txn = tm.begin(ts);
                        ts = txn->timestamp;
                        Value a = 0, b = 0;
                        Status s = tm.read_for_update(*txn, k1, a);
                        if (s == Status::OK) s = tm.read_for_update(*txn, k2, b);  // k1==k2 is fine (re-entrant)
                        if (s == Status::OK) s = tm.write(*txn, k1, mix1(a, b, tag));
                        if (s == Status::OK && k2 != k1) s = tm.write(*txn, k2, mix2(a, b, tag));
                        if (s == Status::OK) s = tm.commit(*txn);
                        if (s == Status::OK) { done[t].push_back({txn->commit_sequence, k1, k2, a, b, tag}); break; }
                        tm.rollback(*txn, s);
                    }
                }
            });
        for (auto& x : th) x.join();

        std::vector<Executed> all;
        for (auto& v : done) all.insert(all.end(), v.begin(), v.end());
        CHECK_EQ(all.size(), static_cast<size_t>(kThreads * kPerThread));
        std::sort(all.begin(), all.end(), [](const Executed& x, const Executed& y) { return x.commit_seq < y.commit_seq; });
        for (size_t i = 1; i < all.size(); ++i) CHECK(all[i].commit_seq > all[i - 1].commit_seq);  // unique, total order

        std::vector<Value> serial(kRecords, 0);
        for (const Executed& e : all) {
            Value a = serial[e.k1], b = serial[e.k2];
            CHECK_EQ(a, e.read1);  // the concurrent run saw exactly what the serial run sees
            CHECK_EQ(b, e.read2);
            serial[e.k1] = mix1(a, b, e.tag);
            if (e.k2 != e.k1) serial[e.k2] = mix2(a, b, e.tag);
        }
        for (Key k = 0; k < kRecords; ++k) CHECK_EQ(tm.peek(k), serial[k]);
        CHECK_EQ(tm.locks().total_lock_entries(), 0u);
    }
}
