// Isolation levels, the anomaly lab and the precedence-graph serializability checker.
#include <atomic>
#include <chrono>
#include <thread>

#include "isolation_lab.h"
#include "serializability.h"
#include "test_framework.h"
#include "transaction_manager.h"

using namespace itx;

namespace {
const IsolationLevel kLevels[] = {IsolationLevel::READ_UNCOMMITTED, IsolationLevel::READ_COMMITTED,
                                  IsolationLevel::REPEATABLE_READ, IsolationLevel::SERIALIZABLE};
}  // namespace

// ------------------------------------------------------------------ isolation mechanics

TEST(isolation_parse_and_names) {
    IsolationLevel l;
    CHECK(parse_isolation("RC", l) && l == IsolationLevel::READ_COMMITTED);
    CHECK(parse_isolation("REPEATABLE READ", l) && l == IsolationLevel::REPEATABLE_READ);
    CHECK(parse_isolation("serializable", l) && l == IsolationLevel::SERIALIZABLE);
    CHECK(!parse_isolation("snapshot", l));
    CHECK_EQ(std::string(to_string(IsolationLevel::READ_UNCOMMITTED)), std::string("RU"));
}

TEST(isolation_default_is_serializable_and_history_off) {
    EngineConfig c;
    c.num_records = 4;
    TransactionManager tm(c);
    auto t = tm.begin();
    CHECK(t->isolation == IsolationLevel::SERIALIZABLE);
    Value v;
    CHECK_EQ(tm.read(*t, 1, v), Status::OK);
    CHECK(tm.locks().holds(t->id, 1, LockMode::SHARED));  // long read lock
    CHECK_EQ(tm.commit(*t), Status::OK);
    CHECK(tm.history().empty());
}

TEST(isolation_read_committed_uses_short_read_locks_but_keeps_write_locks) {
    EngineConfig c;
    c.num_records = 4;
    TransactionManager tm(c);
    auto t = tm.begin_at(IsolationLevel::READ_COMMITTED);
    Value v;
    CHECK_EQ(tm.read(*t, 0, v), Status::OK);
    CHECK(!tm.locks().holds(t->id, 0, LockMode::SHARED));  // released right after the read
    CHECK_EQ(tm.write(*t, 1, 5), Status::OK);
    CHECK_EQ(tm.read(*t, 1, v), Status::OK);  // reading its own write must NOT drop the X lock
    CHECK_EQ(v, 5);
    CHECK(tm.locks().holds(t->id, 1, LockMode::EXCLUSIVE));
    CHECK_EQ(tm.commit(*t), Status::OK);
    CHECK_EQ(tm.locks().total_lock_entries(), 0u);
}

TEST(isolation_read_uncommitted_takes_no_read_locks_and_sees_dirty_data) {
    EngineConfig c;
    c.num_records = 2;
    TransactionManager tm(c);
    auto writer = tm.begin();
    CHECK_EQ(tm.write(*writer, 0, 42), Status::OK);
    auto reader = tm.begin_at(IsolationLevel::READ_UNCOMMITTED);
    Value v = 0;
    CHECK_EQ(tm.read(*reader, 0, v), Status::OK);  // does not block on the X lock
    CHECK_EQ(v, 42);                               // dirty value
    CHECK_EQ(tm.locks().locks_held(reader->id), 0u);
    tm.rollback(*writer);
    CHECK_EQ(tm.commit(*reader), Status::OK);
    CHECK_EQ(tm.peek(0), 0);
}

TEST(isolation_repeatable_read_blocks_writer_on_rows_read) {
    EngineConfig c;
    c.num_records = 2;
    c.policy = LockPolicy::DEADLOCK_DETECT;
    TransactionManager tm(c);
    auto r = tm.begin_at(IsolationLevel::REPEATABLE_READ);
    Value v;
    CHECK_EQ(tm.read(*r, 0, v), Status::OK);
    std::atomic<bool> wrote{false};
    std::thread w([&] {
        auto t = tm.begin();
        CHECK_EQ(tm.write(*t, 0, 9), Status::OK);
        wrote = true;
        tm.commit(*t);
    });
    for (int i = 0; i < 20000 && tm.locks().waiting_count() == 0; ++i) std::this_thread::sleep_for(std::chrono::microseconds(100));
    CHECK(!wrote.load());
    CHECK_EQ(tm.read(*r, 0, v), Status::OK);
    CHECK_EQ(v, 0);  // repeatable
    CHECK_EQ(tm.commit(*r), Status::OK);
    w.join();
    CHECK(wrote.load());
    CHECK_EQ(tm.peek(0), 9);
}

TEST(isolation_scan_count_locks_all_rows_only_at_serializable) {
    for (IsolationLevel lvl : {IsolationLevel::REPEATABLE_READ, IsolationLevel::SERIALIZABLE}) {
        EngineConfig c;
        c.num_records = 5;
        c.initial_value = 100;
        TransactionManager tm(c);
        { auto t = tm.begin(); tm.write(*t, 4, 50); tm.commit(*t); }
        auto s = tm.begin_at(lvl);
        Value n = 0;
        std::vector<Key> matched;
        CHECK_EQ(tm.scan_count(*s, 100, n, &matched), Status::OK);
        CHECK_EQ(n, 4);
        CHECK_EQ(matched.size(), 4u);
        bool row_e_locked = tm.locks().holds(s->id, 4, LockMode::SHARED);
        CHECK_EQ(row_e_locked, lvl == IsolationLevel::SERIALIZABLE);  // RR leaves the non-matching row unlocked
        CHECK(tm.locks().holds(s->id, 0, LockMode::SHARED));
        CHECK_EQ(tm.commit(*s), Status::OK);
    }
}

// ------------------------------------------------------------------ anomaly lab

TEST(anomaly_matrix_matches_the_sql_standard_table) {
    // rows: scenario; columns: RU RC RR SER; true = anomaly is possible and HAPPENED
    struct Row { const char* id; bool expect[4]; };
    const Row rows[] = {{"dirty_read", {true, false, false, false}},
                        {"non_repeatable_read", {true, true, false, false}},
                        {"lost_update", {true, true, false, false}},
                        {"phantom", {true, true, true, false}}};
    for (const Row& row : rows)
        for (int i = 0; i < 4; ++i) {
            AnomalyResult r = run_anomaly(row.id, kLevels[i], LockPolicy::DEADLOCK_DETECT);
            if (r.anomaly != row.expect[i])
                throw TestFailure(std::string(row.id) + " at " + to_string(kLevels[i]) + ": expected anomaly=" +
                                  (row.expect[i] ? "true" : "false") + " - " + r.verdict);
            CHECK(!r.verdict.empty());
        }
}

TEST(anomaly_is_visible_in_the_precedence_graph_or_recoverability) {
    for (const std::string id : {"non_repeatable_read", "lost_update", "phantom"})
        for (IsolationLevel l : kLevels) {
            AnomalyResult r = run_anomaly(id, l);
            // Every anomaly between two COMMITTED transactions is a precedence-graph cycle; every prevented run is serializable.
            CHECK_EQ(r.analysis.conflict_serializable, !r.anomaly);
        }
    AnomalyResult dirty = run_anomaly("dirty_read", IsolationLevel::READ_UNCOMMITTED);
    CHECK(dirty.anomaly);
    CHECK(dirty.analysis.recoverable == Tristate::NO);  // T2 committed after reading from aborted T1
    AnomalyResult clean = run_anomaly("dirty_read", IsolationLevel::READ_COMMITTED);
    CHECK(clean.analysis.recoverable == Tristate::YES);
    CHECK(clean.analysis.strict == Tristate::YES);
}

TEST(anomaly_lab_events_show_blocking_and_deadlock_victims) {
    AnomalyResult r = run_anomaly("non_repeatable_read", IsolationLevel::REPEATABLE_READ);
    CHECK(r.events[1].waited);  // T2's write waited for T1's shared lock
    CHECK(r.events[1].finished_tick > r.events[3].finished_tick);  // ... until after T1's second read
    CHECK(r.t1_committed && r.t2_committed);
    AnomalyResult lu = run_anomaly("lost_update", IsolationLevel::SERIALIZABLE);
    CHECK_EQ(lu.deadlocks, 1);
    CHECK(lu.t1_committed != lu.t2_committed);  // exactly one survives
    CHECK_EQ(lu.final_values[0], lu.t1_committed ? 110 : 120);
    for (const auto& e : lu.events) CHECK(e.finished_tick >= e.issued_tick);
}

TEST(anomaly_lab_other_policies_prevent_by_aborting) {
    AnomalyResult r = run_anomaly("lost_update", IsolationLevel::READ_COMMITTED, LockPolicy::WAIT_DIE);
    CHECK(!r.anomaly);  // T2 (younger) dies instead of waiting for T1's X lock
    CHECK(!r.t2_committed);
    CHECK_THROWS(run_anomaly("lost_update", IsolationLevel::READ_COMMITTED, LockPolicy::NONE));
    CHECK_THROWS(run_anomaly("no_such", IsolationLevel::READ_COMMITTED));
}

// ------------------------------------------------------------------ serializability checker

TEST(schedule_parser_accepts_textbook_notation_and_rejects_garbage) {
    auto ops = parse_schedule("r1(x), w2[x]; C1 a2");
    CHECK_EQ(ops.size(), 4u);
    CHECK_EQ(ops[1].op, 'W');
    CHECK_EQ(ops[1].item, std::string("x"));
    CHECK_EQ(ops[3].op, 'A');
    CHECK_EQ(format_schedule(ops), std::string("R1(x) W2(x) C1 A2"));
    CHECK_THROWS(parse_schedule(""));
    CHECK_THROWS(parse_schedule("X1(A)"));
    CHECK_THROWS(parse_schedule("R1"));
    CHECK_THROWS(parse_schedule("R0(A)"));
    CHECK_THROWS(analyze_schedule(parse_schedule("C1 R1(A)")));  // operation after commit
}

TEST(precedence_graph_textbook_cases) {
    // Non-serializable: classic cycle on A and B.
    auto r = analyze_schedule(parse_schedule("R1(A) W2(A) R2(B) W1(B) C1 C2"));
    CHECK(!r.conflict_serializable);
    CHECK_EQ(r.cycle.size(), 2u);
    CHECK_EQ(r.edges.size(), 2u);
    // Serializable, equivalent to T1 then T2.
    auto s = analyze_schedule(parse_schedule("R1(A) W1(A) R2(A) W2(A) R1(B) W1(B) R2(B) W2(B)"));
    CHECK(s.conflict_serializable);
    CHECK_EQ(s.serial_order.size(), 2u);
    CHECK_EQ(s.serial_order[0], 1u);
    CHECK(s.recoverable == Tristate::UNKNOWN);  // no commit points given
    // Reads alone never conflict.
    auto reads = analyze_schedule(parse_schedule("R1(A) R2(A) R3(A)"));
    CHECK(reads.edges.empty() && reads.conflict_serializable);
    // Three-transaction order: T2 -> T1 -> T3.
    auto t3 = analyze_schedule(parse_schedule("W2(A) R1(A) W1(B) R3(B)"));
    CHECK(t3.conflict_serializable);
    CHECK(t3.serial_order == std::vector<TxnId>({2, 1, 3}));
}

TEST(view_serializable_but_not_conflict_serializable_blind_writes) {
    auto r = analyze_schedule(parse_schedule("R1(A) W2(A) W1(A) W3(A)"));
    CHECK(!r.conflict_serializable);
    CHECK(r.view_serializable == Tristate::YES);
    CHECK(r.view_serial_order == std::vector<TxnId>({1, 2, 3}));
    auto lost = analyze_schedule(parse_schedule("R1(A) R2(A) W1(A) W2(A)"));
    CHECK(lost.view_serializable == Tristate::NO);
}

TEST(recoverability_classes) {
    auto nonrec = analyze_schedule(parse_schedule("W1(A) R2(A) C2 A1"));
    CHECK(nonrec.recoverable == Tristate::NO);
    auto cascading = analyze_schedule(parse_schedule("W1(A) R2(A) C1 C2"));
    CHECK(cascading.recoverable == Tristate::YES);
    CHECK(cascading.cascadeless == Tristate::NO);
    auto aca_not_strict = analyze_schedule(parse_schedule("W1(A) W2(A) C1 C2"));
    CHECK(aca_not_strict.cascadeless == Tristate::YES);
    CHECK(aca_not_strict.strict == Tristate::NO);
    auto strict = analyze_schedule(parse_schedule("W1(A) C1 R2(A) W2(A) C2"));
    CHECK(strict.strict == Tristate::YES);
    // Aborted transactions are removed before the graph is built.
    auto ab = analyze_schedule(parse_schedule("R1(A) W2(A) W1(A) A2 C1"));
    CHECK(ab.conflict_serializable);
    CHECK(ab.aborted == std::vector<TxnId>({2}));
}

TEST(engine_history_records_operations_in_effect_order) {
    EngineConfig c;
    c.num_records = 3;
    c.record_history = true;
    TransactionManager tm(c);
    auto t = tm.begin();
    Value v;
    tm.read(*t, 0, v);
    tm.write(*t, 1, 7);
    tm.commit(*t);
    auto u = tm.begin();
    tm.write(*u, 2, 1);
    tm.rollback(*u);
    auto h = tm.history();
    CHECK_EQ(h.size(), 5u);
    CHECK_EQ(h[0].op, 'R');
    CHECK_EQ(h[1].op, 'W');
    CHECK_EQ(h[1].value, 7);
    CHECK_EQ(h[2].op, 'C');
    CHECK_EQ(h[4].op, 'A');
    for (size_t i = 0; i < h.size(); ++i) CHECK_EQ(h[i].seq, i + 1);
    auto sched = schedule_from_history(h, {"A", "B"});
    CHECK_EQ(sched[0].item, std::string("A"));
    CHECK_EQ(sched[3].item, std::string("k2"));  // no name given for key 2
}

TEST(live_run_serializable_is_always_conflict_serializable_and_loses_nothing) {
    for (unsigned seed = 1; seed <= 4; ++seed) {
        LiveRunConfig c;
        c.isolation = IsolationLevel::SERIALIZABLE;
        c.seed = seed;
        c.transactions = 8;
        LiveRunResult r = run_live_schedule(c);
        CHECK_EQ(r.committed, 8u);
        CHECK(r.analysis.conflict_serializable);  // the 2PL theorem, observed on real threads
        CHECK_EQ(r.lost_updates, 0);
        CHECK(r.analysis.recoverable == Tristate::YES);
    }
}

TEST(live_run_read_uncommitted_produces_non_serializable_histories) {
    int cycles = 0;
    long lost = 0;
    for (unsigned seed = 1; seed <= 6; ++seed) {
        LiveRunConfig c;
        c.isolation = IsolationLevel::READ_UNCOMMITTED;
        c.seed = seed;
        c.transactions = 8;
        c.think_us = 800;
        LiveRunResult r = run_live_schedule(c);
        cycles += r.analysis.conflict_serializable ? 0 : 1;
        lost += r.lost_updates;
    }
    CHECK(cycles > 0);
    CHECK(lost > 0);
    CHECK_THROWS(run_live_schedule([] { LiveRunConfig c; c.transactions = 99; return c; }()));
}
