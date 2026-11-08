#include "scenarios.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>

#include "sync_primitives.h"

namespace itx {

namespace {
void sleep_us(int us) { std::this_thread::sleep_for(std::chrono::microseconds(us)); }
}  // namespace

DeadlockScenarioResult run_deadlock_scenario(LockPolicy policy) {
    if (policy == LockPolicy::NONE) throw std::invalid_argument("policy none cannot deadlock");
    EngineConfig cfg;
    cfg.num_records = 2;
    cfg.policy = policy;
    TransactionManager tm(cfg);
    const Key A = 0, B = 1;

    DeadlockScenarioResult res;
    res.policy = to_string(policy);
    res.mechanism = policy == LockPolicy::DEADLOCK_DETECT ? "detection" : policy == LockPolicy::AVOID ? "avoidance" : "prevention";

    std::mutex ev_mu;
    std::atomic<int> seq{0};
    auto add_event = [&](const std::string& e, TxnId t, const std::string& d) {
        std::lock_guard<std::mutex> g(ev_mu);
        res.events.push_back({++seq, e, t, d});
        if (e == "DEADLOCK_DETECTED") res.deadlock_cycle_detected = true;
    };
    tm.locks().set_trace(add_event);

    Latch both_hold_first(2);
    std::atomic<bool> t1_first_result_ready{false}, t1_has_first{false};
    std::atomic<uint64_t> t1_ts{0}, t2_ts{0};

    // One attempt of a two-record increment in the given order. Returns final status.
    auto attempt = [&](TxnId /*label*/, uint64_t& ts, Key first, Key second, bool is_t1,
                       bool first_attempt, int& attempts) -> bool {
        auto txn = tm.begin(ts);
        ts = txn->timestamp;
        ++attempts;
        add_event("BEGIN", txn->id, std::string("timestamp ") + std::to_string(ts));
        if (policy == LockPolicy::AVOID) {
            tm.declare(*txn, {A, B});  // avoidance needs the maximum claim up front
            if (first_attempt && !is_t1)  // T2 tries to take B only after T1 holds A: the dangerous interleaving
                for (int i = 0; i < 100000 && !t1_has_first.load(); ++i) sleep_us(100);
        }
        Value v = 0;
        Status st = tm.read_for_update(*txn, first, v);
        if (st == Status::OK) st = tm.write(*txn, first, v + 1);
        if (st == Status::OK && first_attempt && policy != LockPolicy::AVOID) {
            both_hold_first.count_down();
            both_hold_first.wait();  // both transactions now hold their first lock
            if (is_t1) {
                // nothing: T1 goes first for the second lock
            } else {
                // T2 waits until T1 is blocked (or has already been aborted) before requesting
                while (!t1_first_result_ready.load() && tm.locks().waiting_count() < 1) sleep_us(100);
            }
        }
        if (st == Status::OK && policy == LockPolicy::AVOID && first_attempt && is_t1) {
            t1_has_first = true;
            for (int i = 0; i < 100000 && tm.locks().waiting_count() < 1; ++i) sleep_us(100);  // let T2 try B first
        }
        if (st == Status::OK) st = tm.read_for_update(*txn, second, v);
        if (is_t1 && first_attempt) t1_first_result_ready.store(true);
        if (st == Status::OK) st = tm.write(*txn, second, v + 1);
        if (st == Status::OK) st = tm.commit(*txn);
        if (st == Status::OK) {
            add_event("COMMIT", txn->id, "committed");
            return true;
        }
        tm.rollback(*txn, st);
        add_event("ROLLBACK", txn->id, std::string("reason ") + to_string(st));
        return false;
    };

    auto worker = [&](bool is_t1) {
        uint64_t ts = is_t1 ? 1 : 2;  // explicit ages: T1 is older than T2
        (void)t1_ts; (void)t2_ts;
        int& attempts = is_t1 ? res.t1_attempts : res.t2_attempts;
        bool done = false;
        for (int i = 0; i < 1000 && !done; ++i) {
            done = attempt(is_t1 ? 1 : 2, ts, is_t1 ? A : B, is_t1 ? B : A, is_t1, i == 0, attempts);
            if (!done) sleep_us(200 * (i + 1));
        }
        (is_t1 ? res.t1_committed : res.t2_committed) = done;
    };

    // Make sure T1 begins first so that its id/timestamp is the smaller one.
    std::thread t1(worker, true);
    std::thread t2(worker, false);
    t1.join();
    t2.join();

    EngineStats es = tm.stats();
    res.aborted_wait_die = es.aborted_wait_die;
    res.aborted_no_wait = es.aborted_no_wait;
    res.aborted_deadlock = es.aborted_deadlock;
    res.final_a = tm.peek(A);
    res.final_b = tm.peek(B);
    res.consistent = res.t1_committed && res.t2_committed && res.final_a == 2 && res.final_b == 2;
    res.unsafe_denials = tm.locks().stats().unsafe_denials;
    res.lock_entries_left = tm.locks().total_lock_entries();
    return res;
}

// ------------------------------------------------------------------------------------------

namespace {
std::string str(Value v) { return std::to_string(v); }

void check(RecoveryScenarioResult& r, const std::string& name, const std::string& expected,
           const std::string& actual) {
    r.checks.push_back({name, expected, actual, expected == actual});
}

EngineConfig recovery_config(const std::string& dir) {
    EngineConfig c;
    c.num_records = 100;
    c.data_dir = dir;
    c.sync_commit = true;
    c.policy = LockPolicy::WAIT_DIE;
    return c;
}

void commit_write(TransactionManager& tm, Key k, Value v) {
    auto t = tm.begin();
    if (tm.write(*t, k, v) != Status::OK || tm.commit(*t) != Status::OK) _exit(3);
}

// Runs `body` in a child process and terminates it with _exit() (no destructors, no flush hooks,
// no graceful shutdown): the closest portable approximation of a process crash.
void run_crashing_child(const std::string& dir, const std::string& scenario) {
    pid_t pid = fork();
    if (pid < 0) throw std::runtime_error("fork failed");
    if (pid == 0) {
        EngineConfig cfg = recovery_config(dir);
        auto tm = std::make_unique<TransactionManager>(cfg);
        if (scenario == "crash") {
            auto t1 = tm->begin();
            tm->write(*t1, 0, 100); tm->write(*t1, 1, 200); tm->commit(*t1);
            commit_write(*tm, 2, 300);
            auto t3 = tm->begin();
            tm->write(*t3, 3, 777);
            tm->rollback(*t3);  // explicit rollback before the crash
            auto t4 = tm->begin();  // never finishes: "in flight" at crash time
            tm->write(*t4, 4, 999);
            tm->write(*t4, 0, 555);
        } else if (scenario == "checkpoint") {
            commit_write(*tm, 0, 1);
            commit_write(*tm, 1, 2);
            if (tm->checkpoint() != Status::OK) _exit(4);
            commit_write(*tm, 2, 3);
            auto t4 = tm->begin();
            tm->write(*t4, 0, 50);  // in flight at crash
        } else if (scenario == "torn_tail") {
            commit_write(*tm, 0, 10);
            commit_write(*tm, 1, 20);
            int fd = ::open(RecoveryManager::log_path(dir).c_str(), O_WRONLY | O_APPEND);
            const char junk[] = "L 9999 U 7 5 0 1 deadbe";  // half-written record, no newline
            if (fd < 0 || ::write(fd, junk, sizeof junk - 1) < 0) _exit(5);
            ::close(fd);
        }
        _exit(0);
    }
    int status = 0;
    waitpid(pid, &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
        throw std::runtime_error("crash child failed unexpectedly");
}
}  // namespace

RecoveryScenarioResult run_recovery_scenario(const std::string& dir, const std::string& scenario) {
    if (scenario != "crash" && scenario != "checkpoint" && scenario != "torn_tail")
        throw std::invalid_argument("unknown recovery scenario: " + scenario);
    ::mkdir(dir.c_str(), 0755);
    RecoveryScenarioResult r;
    r.scenario = scenario;
    run_crashing_child(dir, scenario);

    {
        TransactionManager tm(recovery_config(dir));  // restart => recovery
        r.report = tm.recovery_report();
        if (scenario == "crash") {
            check(r, "committed T1 (key0) survives", "100", str(tm.peek(0)));
            check(r, "committed T1 (key1) survives", "200", str(tm.peek(1)));
            check(r, "committed T2 (key2) survives", "300", str(tm.peek(2)));
            check(r, "rolled-back T3 (key3) stays undone", "0", str(tm.peek(3)));
            check(r, "in-flight T4 (key4) is undone", "0", str(tm.peek(4)));
            check(r, "in-flight T4 did not clobber key0", "100", str(tm.peek(0)));
            check(r, "one loser transaction was undone by recovery", "1", std::to_string(r.report.losers.size()));
            check(r, "undo actions applied for the loser", "2", std::to_string(r.report.undo_applied));
        } else if (scenario == "checkpoint") {
            check(r, "snapshot was loaded", "true", r.report.snapshot_loaded ? "true" : "false");
            check(r, "pre-checkpoint commit key0", "1", str(tm.peek(0)));
            check(r, "pre-checkpoint commit key1", "2", str(tm.peek(1)));
            check(r, "post-checkpoint commit key2 (from log)", "3", str(tm.peek(2)));
            check(r, "in-flight write on key0 undone", "1", str(tm.peek(0)));
            check(r, "only post-checkpoint records were redone", "true",
                  r.report.records_scanned <= 8 ? "true" : "false");
        } else {
            check(r, "torn tail detected", "true", r.report.torn_tail ? "true" : "false");
            check(r, "committed key0 survives", "10", str(tm.peek(0)));
            check(r, "committed key1 survives", "20", str(tm.peek(1)));
            commit_write(tm, 2, 30);  // engine must be fully usable after repairing the log
        }
    }
    {
        // Second restart: recovery must be idempotent and see everything written after repair.
        TransactionManager tm2(recovery_config(dir));
        const RecoveryReport& r2 = tm2.recovery_report();
        check(r, "second restart: no torn tail", "false", r2.torn_tail ? "true" : "false");
        check(r, "second restart: nothing left to undo", "0", std::to_string(r2.undo_applied));
        if (scenario == "crash") {
            check(r, "second restart: key0 stable", "100", str(tm2.peek(0)));
            check(r, "second restart: key4 stable", "0", str(tm2.peek(4)));
        } else if (scenario == "checkpoint") {
            check(r, "second restart: key0 stable", "1", str(tm2.peek(0)));
            check(r, "second restart: key2 stable", "3", str(tm2.peek(2)));
        } else {
            check(r, "write made after log repair is durable", "30", str(tm2.peek(2)));
        }
    }
    r.all_pass = true;
    for (const auto& c : r.checks) r.all_pass = r.all_pass && c.pass;
    return r;
}

}  // namespace itx
