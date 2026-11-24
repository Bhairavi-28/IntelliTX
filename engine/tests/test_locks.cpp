#include <atomic>
#include <chrono>
#include <thread>

#include "lock_manager.h"
#include "test_framework.h"
#include "transaction_manager.h"

using namespace itx;

static void wait_until_waiting(const LockManager& lm, size_t n) {
    for (int i = 0; i < 20000 && lm.waiting_count() < n; ++i)
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    CHECK(lm.waiting_count() >= n);
}

TEST(lock_shared_shared_compatible) {
    LockManager lm(LockPolicy::NO_WAIT);
    CHECK_EQ(lm.acquire(1, 1, 7, LockMode::SHARED), Status::OK);
    CHECK_EQ(lm.acquire(2, 2, 7, LockMode::SHARED), Status::OK);
    CHECK(lm.holds(1, 7, LockMode::SHARED));
    CHECK(lm.holds(2, 7, LockMode::SHARED));
    CHECK(!lm.holds(1, 7, LockMode::EXCLUSIVE));
    lm.release_all(1);
    lm.release_all(2);
    CHECK_EQ(lm.total_lock_entries(), 0u);
}

TEST(lock_conflicts_and_release) {
    LockManager lm(LockPolicy::NO_WAIT);
    CHECK_EQ(lm.acquire(1, 1, 7, LockMode::SHARED), Status::OK);
    CHECK_EQ(lm.acquire(2, 2, 7, LockMode::EXCLUSIVE), Status::ABORT_NO_WAIT);  // S vs X
    lm.release_all(2);
    lm.release_all(1);
    CHECK_EQ(lm.acquire(1, 1, 7, LockMode::EXCLUSIVE), Status::OK);
    CHECK_EQ(lm.acquire(2, 2, 7, LockMode::SHARED), Status::ABORT_NO_WAIT);     // X vs S
    CHECK_EQ(lm.acquire(3, 3, 7, LockMode::EXCLUSIVE), Status::ABORT_NO_WAIT);  // X vs X
    lm.release_all(2);
    lm.release_all(3);
    lm.release_all(1);  // releasing frees the key for everybody else
    CHECK_EQ(lm.acquire(4, 4, 7, LockMode::EXCLUSIVE), Status::OK);
    lm.release_all(4);
    CHECK_EQ(lm.total_lock_entries(), 0u);
}

TEST(lock_upgrade_rules) {
    LockManager lm(LockPolicy::NO_WAIT);
    CHECK_EQ(lm.acquire(1, 1, 3, LockMode::SHARED), Status::OK);
    CHECK_EQ(lm.acquire(1, 1, 3, LockMode::EXCLUSIVE), Status::OK);  // sole holder: upgrade
    CHECK(lm.holds(1, 3, LockMode::EXCLUSIVE));
    CHECK_EQ(lm.acquire(1, 1, 3, LockMode::SHARED), Status::OK);     // X already covers S
    CHECK_EQ(lm.locks_held(1), 1u);                                  // still ONE lock entry
    lm.release_all(1);
    // upgrade is refused (not silently granted) while another reader exists
    CHECK_EQ(lm.acquire(1, 1, 3, LockMode::SHARED), Status::OK);
    CHECK_EQ(lm.acquire(2, 2, 3, LockMode::SHARED), Status::OK);
    CHECK_EQ(lm.acquire(1, 1, 3, LockMode::EXCLUSIVE), Status::ABORT_NO_WAIT);
    CHECK(!lm.holds(1, 3, LockMode::EXCLUSIVE));
    lm.release_all(1);
    lm.release_all(2);
}

TEST(wait_die_younger_dies_immediately) {
    LockManager lm(LockPolicy::WAIT_DIE);
    CHECK_EQ(lm.acquire(1, 10, 5, LockMode::EXCLUSIVE), Status::OK);  // older holder (ts 10)
    CHECK_EQ(lm.acquire(2, 20, 5, LockMode::EXCLUSIVE), Status::ABORT_WAIT_DIE);  // younger dies
    CHECK_EQ(lm.stats().wait_die_aborts, 1u);
    CHECK_EQ(lm.waiting_count(), 0u);
    lm.release_all(2);
    lm.release_all(1);
}

TEST(wait_die_older_waits_then_is_granted) {
    LockManager lm(LockPolicy::WAIT_DIE);
    CHECK_EQ(lm.acquire(2, 20, 5, LockMode::EXCLUSIVE), Status::OK);  // younger holds
    std::atomic<int> result{-1};
    std::thread older([&] { result = static_cast<int>(lm.acquire(1, 10, 5, LockMode::EXCLUSIVE)); });
    wait_until_waiting(lm, 1);
    CHECK_EQ(result.load(), -1);  // really blocked, not busy-returning WAIT
    CHECK(lm.wait_for_graph().has_edge(1, 2));
    lm.release_all(2);            // wake-up must not be lost
    older.join();
    CHECK_EQ(result.load(), static_cast<int>(Status::OK));
    CHECK(lm.holds(1, 5, LockMode::EXCLUSIVE));
    lm.release_all(1);
    CHECK_EQ(lm.stats().waits, 1u);
}

TEST(wait_die_keeps_age_across_restart) {
    TransactionManager tm(EngineConfig{});
    auto first = tm.begin();
    uint64_t ts = first->timestamp;
    tm.rollback(*first);
    auto again = tm.begin(ts);  // restart keeps the old timestamp
    CHECK_EQ(again->timestamp, ts);
    CHECK(again->id > first->id);
    tm.rollback(*again);
}

TEST(detect_policy_waits_without_cycle) {
    LockManager lm(LockPolicy::DEADLOCK_DETECT);
    CHECK_EQ(lm.acquire(1, 1, 5, LockMode::EXCLUSIVE), Status::OK);
    std::atomic<int> result{-1};
    std::thread t([&] { result = static_cast<int>(lm.acquire(2, 2, 5, LockMode::EXCLUSIVE)); });
    wait_until_waiting(lm, 1);
    CHECK_EQ(lm.stats().deadlock_cycles, 0u);
    lm.release_all(1);
    t.join();
    CHECK_EQ(result.load(), static_cast<int>(Status::OK));
    lm.release_all(2);
}

TEST(strict_2pl_locks_held_until_commit) {
    EngineConfig cfg;
    cfg.num_records = 10;
    TransactionManager tm(cfg);
    auto t = tm.begin();
    Value v;
    CHECK_EQ(tm.read(*t, 1, v), Status::OK);
    CHECK_EQ(tm.write(*t, 2, 42), Status::OK);
    // growing phase finished (no more operations) but NOTHING has been released yet:
    CHECK(tm.locks().holds(t->id, 1, LockMode::SHARED));
    CHECK(tm.locks().holds(t->id, 2, LockMode::EXCLUSIVE));
    CHECK_EQ(tm.locks().locks_held(t->id), 2u);
    CHECK_EQ(tm.commit(*t), Status::OK);
    CHECK_EQ(tm.locks().total_lock_entries(), 0u);  // all released at commit, together
    auto t2 = tm.begin();
    CHECK_EQ(tm.write(*t2, 3, 1), Status::OK);
    tm.rollback(*t2);                               // ... and at abort
    CHECK_EQ(tm.locks().total_lock_entries(), 0u);
}
