#include <fstream>

#include "scenarios.h"
#include "test_framework.h"
#include "transaction_manager.h"
#include "wal.h"

using namespace itx;

static void expect_all_pass(const RecoveryScenarioResult& r) {
    for (const auto& c : r.checks)
        if (!c.pass) throw TestFailure("check '" + c.name + "' expected " + c.expected + " got " + c.actual);
    CHECK(r.all_pass);
}

TEST(wal_record_roundtrip_and_checksum) {
    LogRecord r;
    r.lsn = 9; r.type = LogType::UPDATE; r.txn = 4; r.key = 3; r.old_value = -5; r.new_value = 77;
    std::string line = serialize_record(r);
    CHECK(line.back() == '\n');
    line.pop_back();
    LogRecord back;
    CHECK(parse_record(line, back));
    CHECK_EQ(back.lsn, 9u); CHECK_EQ(back.old_value, -5); CHECK_EQ(back.new_value, 77);
    line[5] = line[5] == '1' ? '2' : '1';  // corrupt one byte
    CHECK(!parse_record(line, back));
}

TEST(wal_write_ahead_ordering_update_logged_before_data_change) {
    TempDir d;
    EngineConfig c;
    c.num_records = 4;
    c.data_dir = d.path;
    TransactionManager tm(c);
    auto t = tm.begin();
    tm.write(*t, 2, 99);
    // The UPDATE record (with before/after image) is already on disk although we have not committed.
    LogScan s = scan_log_file(RecoveryManager::log_path(d.path));
    bool found = false;
    for (auto& r : s.records)
        if (r.type == LogType::UPDATE && r.key == 2 && r.old_value == 0 && r.new_value == 99) found = true;
    CHECK(found);
    tm.commit(*t);
    s = scan_log_file(RecoveryManager::log_path(d.path));
    CHECK(s.records.back().type == LogType::COMMIT);  // commit record is the last durable record
}

TEST(recovery_committed_survive_incomplete_undone_process_crash) {
    TempDir d;
    expect_all_pass(run_recovery_scenario(d.sub("crash"), "crash"));
}

TEST(recovery_with_checkpoint) {
    TempDir d;
    expect_all_pass(run_recovery_scenario(d.sub("cp"), "checkpoint"));
}

TEST(recovery_torn_log_tail) {
    TempDir d;
    expect_all_pass(run_recovery_scenario(d.sub("torn"), "torn_tail"));
}

TEST(recovery_rejects_unknown_scenario) {
    TempDir d;
    CHECK_THROWS(run_recovery_scenario(d.sub("x"), "bogus"));
}

TEST(recovery_clean_restart_keeps_committed_data) {
    TempDir d;
    EngineConfig c;
    c.num_records = 10;
    c.data_dir = d.path;
    {
        TransactionManager tm(c);
        auto t = tm.begin();
        tm.write(*t, 5, 55);
        tm.commit(*t);
    }  // graceful shutdown
    TransactionManager tm2(c);
    CHECK_EQ(tm2.peek(5), 55);
    CHECK_EQ(tm2.recovery_report().committed.size(), 1u);
    CHECK_EQ(tm2.recovery_report().losers.size(), 0u);
    auto t = tm2.begin();  // transaction ids continue after the old ones
    CHECK(t->id > 1);
    tm2.rollback(*t);
}

TEST(recovery_rollback_before_crash_is_replayed_correctly) {
    // History: T1 commits k=1; T2 writes k=1 then ROLLS BACK (logs CLR); crash. Recovery must end at T1's value.
    TempDir d;
    EngineConfig c;
    c.num_records = 4;
    c.data_dir = d.path;
    {
        TransactionManager tm(c);
        auto t1 = tm.begin(); tm.write(*t1, 1, 10); tm.commit(*t1);
        auto t2 = tm.begin(); tm.write(*t2, 1, 20); tm.rollback(*t2);
    }
    TransactionManager tm2(c);
    CHECK_EQ(tm2.peek(1), 10);
    CHECK_EQ(tm2.recovery_report().rolled_back_before_crash.size(), 1u);
}

TEST(recovery_corrupt_snapshot_is_reported_not_ignored) {
    TempDir d;
    EngineConfig c;
    c.num_records = 4;
    c.data_dir = d.path;
    {
        TransactionManager tm(c);
        auto t = tm.begin(); tm.write(*t, 0, 1); tm.commit(*t);
        CHECK_EQ(tm.checkpoint(), Status::OK);
    }
    {
        std::fstream f(RecoveryManager::snapshot_path(d.path), std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(10);
        f.put('Z');
    }
    CHECK_THROWS(TransactionManager tm(c));  // refuse to start on a corrupt snapshot
}

TEST(checkpoint_truncates_log) {
    TempDir d;
    EngineConfig c;
    c.num_records = 4;
    c.data_dir = d.path;
    TransactionManager tm(c);
    for (int i = 0; i < 20; ++i) { auto t = tm.begin(); tm.write(*t, 0, i); tm.commit(*t); }
    uint64_t before = tm.log_bytes();
    CHECK_EQ(tm.checkpoint(), Status::OK);
    CHECK(tm.log_bytes() < before / 4);
    auto t = tm.begin(); tm.write(*t, 1, 5); tm.commit(*t);  // engine keeps logging after truncation
    LogScan s = scan_log_file(RecoveryManager::log_path(d.path));
    CHECK(s.records.size() >= 4);  // checkpoint + begin + update + commit
    CHECK(!s.torn_tail);
}

TEST(volatile_engine_has_no_checkpoint) {
    TransactionManager tm(EngineConfig{});
    CHECK_EQ(tm.checkpoint(), Status::ERR_BAD_STATE);
    CHECK(!tm.durable());
}
