#include "deadlock_detector.h"
#include "scenarios.h"
#include "test_framework.h"

using namespace itx;

TEST(wait_for_graph_detects_cycle_and_ignores_dag) {
    WaitForGraph g;
    g.add_edge(1, 2); g.add_edge(2, 3);
    CHECK(g.find_cycle().empty());
    g.add_edge(3, 1);
    auto cyc = g.find_cycle();
    CHECK_EQ(cyc.size(), 3u);
    g.remove_node(2);
    CHECK(g.find_cycle().empty());
    WaitForGraph self;
    self.add_edge(4, 4);  // a transaction never waits for itself
    CHECK(self.empty());
}

TEST(victim_selection_policies) {
    std::vector<TxnId> cycle = {1, 2, 3};
    auto ts = [](TxnId t) { return static_cast<uint64_t>(t * 10); };
    auto locks = [](TxnId t) { return t == 2 ? size_t(1) : size_t(5); };
    CHECK_EQ(choose_victim(cycle, VictimPolicy::YOUNGEST, ts, locks), 3u);
    CHECK_EQ(choose_victim(cycle, VictimPolicy::FEWEST_LOCKS, ts, locks), 2u);
    CHECK_THROWS(choose_victim({}, VictimPolicy::YOUNGEST, ts, locks));
}

static void expect_clean(const DeadlockScenarioResult& r) {
    CHECK(r.t1_committed);
    CHECK(r.t2_committed);
    CHECK(r.consistent);                  // both increments applied exactly once: A == B == 2
    CHECK_EQ(r.lock_entries_left, 0u);    // no lock leaks
}

TEST(deadlock_scenario_wait_die_prevention_no_graph_needed) {
    auto r = run_deadlock_scenario(LockPolicy::WAIT_DIE);
    expect_clean(r);
    CHECK(!r.deadlock_cycle_detected);    // prevention: a cycle can never form
    CHECK(r.aborted_wait_die >= 1u);      // the younger transaction died and restarted
    CHECK_EQ(r.aborted_deadlock, 0u);
}

TEST(deadlock_scenario_no_wait_prevention) {
    auto r = run_deadlock_scenario(LockPolicy::NO_WAIT);
    expect_clean(r);
    CHECK(!r.deadlock_cycle_detected);
    CHECK(r.aborted_no_wait >= 1u);
}

TEST(deadlock_scenario_detection_finds_cycle_and_recovers) {
    auto r = run_deadlock_scenario(LockPolicy::DEADLOCK_DETECT);
    expect_clean(r);
    CHECK(r.deadlock_cycle_detected);     // detection: the cycle really formed and was found
    CHECK_EQ(r.aborted_deadlock, 1u);     // exactly one victim
    bool saw_victim = false;
    for (auto& e : r.events) if (e.event == "ROLLBACK" && e.detail.find("ABORT_DEADLOCK") != std::string::npos) saw_victim = true;
    CHECK(saw_victim);
}

TEST(deadlock_scenario_rejects_policy_none) {
    CHECK_THROWS(run_deadlock_scenario(LockPolicy::NONE));
}

TEST(os_bankers_safe_state_textbook_example) {
    Matrix alloc = {{0,1,0},{2,0,0},{3,0,2},{2,1,1},{0,0,2}};
    Matrix mx = {{7,5,3},{3,2,2},{9,0,2},{2,2,2},{4,3,3}};
    SafetyResult s = bankers_safety({3,3,2}, alloc, mx);
    CHECK(s.safe);
    CHECK_EQ(s.safe_sequence.size(), 5u);
    // Several safe sequences exist; ours is deterministic (always the lowest runnable index).
    std::vector<int> expected = {1, 3, 0, 2, 4};
    CHECK(s.safe_sequence == expected);
    // Property check: replaying the sequence never needs more than is available.
    std::vector<int> work = {3, 3, 2};
    for (int p : s.safe_sequence)
        for (int j = 0; j < 3; ++j) { CHECK(mx[p][j] - alloc[p][j] <= work[j]); work[j] += alloc[p][j]; }
}

TEST(os_bankers_request_grant_and_deny) {
    Matrix alloc = {{0,1,0},{2,0,0},{3,0,2},{2,1,1},{0,0,2}};
    Matrix mx = {{7,5,3},{3,2,2},{9,0,2},{2,2,2},{4,3,3}};
    CHECK(bankers_request({3,3,2}, alloc, mx, 1, {1,0,2}).granted);       // safe => granted
    auto r = bankers_request({3,3,2}, alloc, mx, 4, {3,3,0});             // would make state unsafe
    CHECK(!r.granted);
    CHECK(r.reason.find("UNSAFE") != std::string::npos);
    Matrix after_p1 = alloc;                                              // textbook: after P1 got (1,0,2)
    after_p1[1] = {3,0,2};
    auto u = bankers_request({2,3,0}, after_p1, mx, 0, {0,2,0});
    CHECK(!u.granted);                                                    // resources exist but state unsafe
    CHECK(u.reason.find("UNSAFE") != std::string::npos);
    CHECK(!bankers_request({3,3,2}, alloc, mx, 1, {9,0,0}).granted);      // exceeds declared max
    CHECK_THROWS(bankers_request({3,3,2}, alloc, mx, 9, {0,0,0}));
}

TEST(os_bankers_unsafe_state) {
    Matrix alloc = {{1,0},{0,1}};
    Matrix mx = {{2,2},{2,2}};
    CHECK(!bankers_safety({0,0}, alloc, mx).safe);
}

TEST(os_deadlock_detection_multi_instance) {
    Matrix alloc = {{0,1,0},{2,0,0},{3,0,3},{2,1,1},{0,0,2}};
    Matrix rq_ok = {{0,0,0},{2,0,2},{0,0,0},{1,0,0},{0,0,2}};
    CHECK(!detect_deadlock({0,0,0}, alloc, rq_ok).deadlocked);            // textbook: no deadlock
    Matrix rq_dead = {{0,0,0},{2,0,2},{0,0,1},{1,0,0},{0,0,2}};
    auto d = detect_deadlock({0,0,0}, alloc, rq_dead);
    CHECK(d.deadlocked);
    std::vector<int> procs = {1, 2, 3, 4};
    CHECK(d.deadlocked_processes == procs);                               // P0 finishes, P1..P4 deadlocked
    CHECK_THROWS(detect_deadlock({0,0}, alloc, rq_dead));                 // shape mismatch
}
