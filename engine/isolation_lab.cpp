#include "isolation_lab.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>

namespace itx {

namespace {

struct Script {
    std::string id, title, anomaly_name, question;
    std::vector<std::string> names;
    std::vector<Value> initial;
    std::vector<LabStep> steps;
};

Script make_script(const std::string& id) {
    using K = LabStep;
    Script s;
    s.id = id;
    if (id == "dirty_read") {
        s.title = "Dirty read";
        s.anomaly_name = "dirty read";
        s.question = "Does T2 read A = 150, a value T1 wrote but then rolled back (it never existed)?";
        s.names = {"A"};
        s.initial = {100};
        s.steps = {{1, K::WRITE_SET, 0, 150}, {2, K::READ, 0, 0}, {1, K::ABORT, 0, 0}, {2, K::COMMIT, 0, 0}};
    } else if (id == "non_repeatable_read") {
        s.title = "Non-repeatable read";
        s.anomaly_name = "non-repeatable read";
        s.question = "Does T1 read A twice and get two different values?";
        s.names = {"A"};
        s.initial = {100};
        s.steps = {{1, K::READ, 0, 0}, {2, K::WRITE_SET, 0, 150}, {2, K::COMMIT, 0, 0},
                   {1, K::READ, 0, 0}, {1, K::COMMIT, 0, 0}};
    } else if (id == "lost_update") {
        s.title = "Lost update";
        s.anomaly_name = "lost update";
        s.question = "Both transactions add to A (100 + 10 + 20). Do both commit but A ends below 130?";
        s.names = {"A"};
        s.initial = {100};
        s.steps = {{1, K::READ, 0, 0}, {2, K::READ, 0, 0}, {1, K::WRITE_ADD_LAST_READ, 0, 10},
                   {2, K::WRITE_ADD_LAST_READ, 0, 20}, {1, K::COMMIT, 0, 0}, {2, K::COMMIT, 0, 0}};
    } else if (id == "phantom") {
        s.title = "Phantom";
        s.anomaly_name = "phantom";
        s.question = "T1 counts accounts with balance >= 100 twice. Does a row 'appear' in between?";
        s.names = {"A", "B", "C", "D", "E"};
        s.initial = {100, 100, 100, 100, 50};
        s.steps = {{1, K::SCAN_COUNT, 0, 100}, {2, K::WRITE_SET, 4, 200}, {2, K::COMMIT, 0, 0},
                   {1, K::SCAN_COUNT, 0, 100}, {1, K::COMMIT, 0, 0}};
    } else {
        throw std::invalid_argument("unknown scenario: " + id +
                                    " (use dirty_read|non_repeatable_read|lost_update|phantom)");
    }
    return s;
}

std::string label_of(const Script& s, const LabStep& st) {
    const std::string& item = st.key < s.names.size() ? s.names[st.key] : "k" + std::to_string(st.key);
    switch (st.kind) {
        case LabStep::READ: return "R(" + item + ")";
        case LabStep::WRITE_SET: return "W(" + item + " = " + std::to_string(st.arg) + ")";
        case LabStep::WRITE_ADD_LAST_READ: return "W(" + item + " = read + " + std::to_string(st.arg) + ")";
        case LabStep::SCAN_COUNT: return "COUNT(balance >= " + std::to_string(st.arg) + ")";
        case LabStep::COMMIT: return "COMMIT";
        case LabStep::ABORT: return "ROLLBACK";
    }
    return "?";
}

std::string abort_note(Status st) {
    switch (st) {
        case Status::ABORT_DEADLOCK: return "deadlock victim: rolled back by the DBMS";
        case Status::ABORT_WAIT_DIE: return "Wait-Die: younger transaction rolled back";
        case Status::ABORT_NO_WAIT: return "NO-WAIT: conflict, rolled back";
        default: return std::string("failed: ") + to_string(st);
    }
}

struct Worker {
    std::unique_ptr<Transaction> txn;
    std::mutex m;
    std::condition_variable cv;
    std::deque<size_t> q;
    bool closed = false;
    std::atomic<int> pending{0};
    std::map<Key, Value> last_read;
    std::thread th;
};

}  // namespace

std::vector<std::string> anomaly_scenarios() {
    return {"dirty_read", "non_repeatable_read", "lost_update", "phantom"};
}

AnomalyResult run_anomaly(const std::string& scenario, IsolationLevel level, LockPolicy policy) {
    if (policy == LockPolicy::NONE || policy == LockPolicy::AVOID)
        throw std::invalid_argument("the anomaly lab needs a locking policy: wait_die, no_wait or detect");
    const Script s = make_script(scenario);

    EngineConfig ec;
    ec.num_records = static_cast<uint32_t>(s.initial.size());
    ec.policy = policy;
    ec.isolation = level;
    ec.record_history = true;
    TransactionManager tm(ec);
    {  // load the initial values (a committed setup transaction, not part of the recorded story)
        auto setup = tm.begin_at(IsolationLevel::SERIALIZABLE);
        for (Key k = 0; k < s.initial.size(); ++k) tm.write(*setup, k, s.initial[k]);
        tm.commit(*setup);
    }
    const size_t setup_ops = tm.history().size();
    const LockStats lock_before = tm.locks().stats();

    AnomalyResult res;
    res.scenario = s.id;
    res.title = s.title;
    res.anomaly_name = s.anomaly_name;
    res.question = s.question;
    res.isolation = to_string(level);
    res.isolation_long = long_name(level);
    res.policy = to_string(policy);
    res.item_names = s.names;
    res.initial_values = s.initial;

    const size_t n = s.steps.size();
    std::vector<LabEvent> ev(n);
    std::vector<char> blocked(n, 0);  // written by the coordinator only
    std::unique_ptr<std::atomic<int>[]> done(new std::atomic<int>[n]);
    for (size_t i = 0; i < n; ++i) done[i] = 0;
    std::atomic<long> clock{0};

    Worker w[2];
    w[0].txn = tm.begin_at(level, 1);  // T1: older
    w[1].txn = tm.begin_at(level, 2);  // T2: younger

    auto exec = [&](Worker& me, size_t i) {
        const LabStep& st = s.steps[i];
        LabEvent& e = ev[i];
        Transaction& t = *me.txn;
        if (t.state != TxnState::ACTIVE) {
            e.status = "skipped";
            e.note = t.state == TxnState::ABORTED ? "transaction was already rolled back" : "already committed";
        } else {
            Status rc = Status::OK;
            Value v = 0;
            switch (st.kind) {
                case LabStep::READ:
                    rc = tm.read(t, st.key, v);
                    if (rc == Status::OK) { me.last_read[st.key] = v; e.note = "read " + std::to_string(v); }
                    break;
                case LabStep::WRITE_SET:
                    v = st.arg;
                    rc = tm.write(t, st.key, v);
                    if (rc == Status::OK) e.note = "wrote " + std::to_string(v);
                    break;
                case LabStep::WRITE_ADD_LAST_READ: {
                    Value base = me.last_read.count(st.key) ? me.last_read[st.key] : 0;
                    v = base + st.arg;
                    rc = tm.write(t, st.key, v);
                    if (rc == Status::OK)
                        e.note = "wrote " + std::to_string(base) + " + " + std::to_string(st.arg) + " = " + std::to_string(v);
                    break;
                }
                case LabStep::SCAN_COUNT:
                    rc = tm.scan_count(t, st.arg, v);
                    if (rc == Status::OK) e.note = "count = " + std::to_string(v);
                    break;
                case LabStep::COMMIT:
                    rc = tm.commit(t);
                    if (rc == Status::OK) e.note = "committed";
                    break;
                case LabStep::ABORT:
                    tm.rollback(t, Status::ABORT_USER);
                    e.note = "rolled back by the application";
                    break;
            }
            e.value = v;
            if (rc == Status::OK) e.status = "done";
            else { e.status = "aborted"; e.note = abort_note(rc); }
        }
        e.finished_tick = ++clock;
        --me.pending;  // BEFORE done: once the coordinator sees `done`, pending is already correct
        done[i] = 1;
    };

    for (int k = 0; k < 2; ++k) {
        Worker& me = w[k];
        me.th = std::thread([&me, &exec] {
            for (;;) {
                size_t i;
                {
                    std::unique_lock<std::mutex> lk(me.m);
                    me.cv.wait(lk, [&] { return me.closed || !me.q.empty(); });
                    if (me.q.empty()) return;
                    i = me.q.front();
                    me.q.pop_front();
                }
                exec(me, i);
            }
        });
    }

    // ---- coordinator: issue the script in order ----
    for (size_t i = 0; i < n; ++i) {
        const LabStep& st = s.steps[i];
        Worker& me = w[st.txn - 1];
        ev[i].step = static_cast<int>(i + 1);
        ev[i].txn = st.txn;
        ev[i].label = label_of(s, st);
        ev[i].issued_tick = ++clock;
        bool behind = me.pending.load() > 0;  // an earlier step of this transaction is still blocked
        ++me.pending;
        {
            std::lock_guard<std::mutex> g(me.m);
            me.q.push_back(i);
        }
        me.cv.notify_one();
        if (behind) { blocked[i] = 1; continue; }
        for (int spin = 0; spin < 20000; ++spin) {  // <= ~4 s
            if (done[i].load()) break;
            if (tm.locks().is_waiting(me.txn->id)) { blocked[i] = 1; break; }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    }
    for (auto& me : w) {
        { std::lock_guard<std::mutex> g(me.m); me.closed = true; }
        me.cv.notify_all();
    }
    for (auto& me : w) me.th.join();

    for (size_t i = 0; i < n; ++i) {
        ev[i].waited = blocked[i] != 0;
        if (ev[i].waited && ev[i].status == "done") ev[i].status = "blocked_then_done";
        if (ev[i].waited && ev[i].status != "skipped") ev[i].note += " (had to wait)";
    }
    res.events = ev;
    res.t1_committed = w[0].txn->state == TxnState::COMMITTED;
    res.t2_committed = w[1].txn->state == TxnState::COMMITTED;
    for (Key k = 0; k < s.initial.size(); ++k) res.final_values.push_back(tm.peek(k));
    LockStats ls = tm.locks().stats();
    res.lock_waits = static_cast<long>(ls.waits - lock_before.waits);
    res.deadlocks = static_cast<long>(ls.deadlock_cycles - lock_before.deadlock_cycles);

    std::vector<HistoryOp> h = tm.history();
    res.history.assign(h.begin() + static_cast<long>(setup_ops), h.end());
    for (auto& op : res.history)  // engine ids -> the T1/T2 labels used everywhere else
        op.txn = op.txn == w[0].txn->id ? 1 : op.txn == w[1].txn->id ? 2 : op.txn;
    res.analysis = analyze_schedule(schedule_from_history(res.history, s.names));

    // ---- did the anomaly happen? ----
    auto ok = [&](size_t i) { return ev[i].status == "done" || ev[i].status == "blocked_then_done"; };
    const std::string lvl = res.isolation_long;
    // A transaction rolled back by the concurrency-control rule (not by the script's ROLLBACK step).
    int cc_victim = 0;
    std::string cc_reason;
    for (const auto& e : ev)
        if (e.status == "aborted" && cc_victim == 0) { cc_victim = e.txn; cc_reason = e.note; }
    const std::string victim_text = cc_victim
        ? "the conflicting request was not allowed to proceed: T" + std::to_string(cc_victim) + " was rolled back (" +
              cc_reason + ") and would have to be retried."
        : "";
    if (s.id == "dirty_read") {
        res.anomaly = ok(1) && ev[1].value == 150;
        res.verdict = res.anomaly
            ? "ANOMALY at " + lvl + ": T2 read A = 150 without taking a lock, but T1 rolled back, so T2 used a value that was never committed."
            : cc_victim ? "Prevented at " + lvl + ": T2's read needed a shared lock on a row T1 had written; " + victim_text
                        : "Prevented at " + lvl + ": T2's read needed a shared lock, so it waited until T1 rolled back and then read the committed value " + std::to_string(ev[1].value) + ".";
    } else if (s.id == "non_repeatable_read") {
        res.anomaly = ok(0) && ok(3) && ev[0].value != ev[3].value;
        res.verdict = res.anomaly
            ? "ANOMALY at " + lvl + ": T1 read A = " + std::to_string(ev[0].value) + " then A = " + std::to_string(ev[3].value) + " inside ONE transaction, because its shared lock was " + (level == IsolationLevel::READ_UNCOMMITTED ? "never taken" : "released right after the first read") + "."
            : cc_victim ? "Prevented at " + lvl + ": T1 kept its shared lock on A, so T2's write conflicted; " + victim_text
                        : "Prevented at " + lvl + ": T1 kept its shared lock on A until commit, so T2's write had to wait; both reads returned " + std::to_string(ev[0].value) + ".";
    } else if (s.id == "lost_update") {
        Value final_a = res.final_values[0];
        res.anomaly = res.t1_committed && res.t2_committed && final_a != 130;
        if (res.anomaly)
            res.verdict = "ANOMALY at " + lvl + ": both transactions committed but A = " + std::to_string(final_a) + " instead of 130: one update was overwritten (lost) because both read A = 100 before either wrote.";
        else if (cc_victim)
            res.verdict = "Prevented at " + lvl + ": the two read-then-write transactions conflicted" + std::string(res.deadlocks ? " (each waited for the other: a deadlock)" : "") + "; " + victim_text + " No committed update was lost.";
        else
            res.verdict = "No anomaly at " + lvl + ": A = " + std::to_string(final_a) + ".";
    } else {  // phantom
        res.anomaly = ok(0) && ok(3) && ev[0].value != ev[3].value;
        res.verdict = res.anomaly
            ? "ANOMALY at " + lvl + ": the same COUNT returned " + std::to_string(ev[0].value) + " then " + std::to_string(ev[3].value) + ": row E moved INTO the predicate because " + (level == IsolationLevel::REPEATABLE_READ ? "only the rows that matched were locked (row locks cannot protect rows that do not match yet)." : "the scan kept no locks.")
            : cc_victim ? "Prevented at " + lvl + ": the scan locked every row it examined (a predicate/table lock), so T2's write to E conflicted; " + victim_text
                        : "Prevented at " + lvl + ": the scan locked every row it examined (a predicate/table lock), so T2's write to E waited until T1 committed; both counts were " + std::to_string(ev[0].value) + ".";
    }
    return res;
}

// ------------------------------------------------------------------ live run

LiveRunResult run_live_schedule(const LiveRunConfig& cfg) {
    if (cfg.transactions < 1 || cfg.transactions > 12) throw std::invalid_argument("transactions must be in [1, 12]");
    if (cfg.workers < 1 || cfg.workers > 8) throw std::invalid_argument("workers must be in [1, 8]");
    if (cfg.records < 1 || cfg.records > 26) throw std::invalid_argument("records must be in [1, 26]");
    if (cfg.ops_per_txn < 1 || cfg.ops_per_txn > 6) throw std::invalid_argument("ops must be in [1, 6]");
    if (cfg.write_ratio < 0 || cfg.write_ratio > 1) throw std::invalid_argument("write ratio must be in [0, 1]");
    if (cfg.think_us < 0 || cfg.think_us > 20000) throw std::invalid_argument("think time must be in [0, 20000] us");
    if (cfg.policy == LockPolicy::AVOID) throw std::invalid_argument("policy avoid is not supported here");

    EngineConfig ec;
    ec.num_records = cfg.records;
    ec.policy = cfg.policy;
    ec.isolation = cfg.isolation;
    ec.record_history = true;
    TransactionManager tm(ec);

    struct Op { Key key; bool rmw; };
    std::vector<std::vector<Op>> specs(static_cast<size_t>(cfg.transactions));
    std::mt19937 rng(cfg.seed);
    std::uniform_int_distribution<uint32_t> key(0, cfg.records - 1);
    std::uniform_real_distribution<double> coin(0, 1);
    for (auto& sp : specs)
        for (int i = 0; i < cfg.ops_per_txn; ++i) sp.push_back({key(rng), coin(rng) < cfg.write_ratio});

    std::atomic<size_t> next{0};
    std::atomic<long> expected{0};
    std::atomic<uint64_t> committed{0}, aborted{0};
    auto worker = [&](unsigned wid) {
        std::mt19937 backoff(cfg.seed * 31u + wid);
        for (size_t i; (i = next.fetch_add(1)) < specs.size();) {
            uint64_t ts = 0;
            for (int attempt = 1; attempt <= 200; ++attempt) {
                auto t = tm.begin(ts);
                ts = t->timestamp;
                Status st = Status::OK;
                long incs = 0;
                for (const Op& op : specs[i]) {
                    Value v = 0;
                    st = tm.read(*t, op.key, v);
                    if (st == Status::OK && op.rmw) {
                        if (cfg.think_us) std::this_thread::sleep_for(std::chrono::microseconds(cfg.think_us));
                        st = tm.write(*t, op.key, v + 1);
                        ++incs;
                    }
                    if (st != Status::OK) break;
                    if (cfg.think_us) std::this_thread::sleep_for(std::chrono::microseconds(cfg.think_us));
                }
                if (st == Status::OK) st = tm.commit(*t);
                if (st == Status::OK) { expected += incs; ++committed; break; }
                tm.rollback(*t, st);
                ++aborted;
                std::this_thread::sleep_for(std::chrono::microseconds(50 + backoff() % (200u * static_cast<unsigned>(attempt < 10 ? attempt : 10))));
            }
        }
    };
    std::vector<std::thread> ths;
    for (int k = 0; k < cfg.workers; ++k) ths.emplace_back(worker, static_cast<unsigned>(k));
    for (auto& t : ths) t.join();

    LiveRunResult r;
    r.isolation = to_string(cfg.isolation);
    r.policy = to_string(cfg.policy);
    r.history = tm.history();
    std::vector<std::string> names;
    for (uint32_t k = 0; k < cfg.records; ++k) names.push_back(std::string(1, static_cast<char>('A' + k)));
    r.analysis = analyze_schedule(schedule_from_history(r.history, names));
    r.committed = committed;
    r.aborted_attempts = aborted;
    r.expected_increment = expected;
    r.actual_increment = static_cast<long>(tm.sum_all());
    r.lost_updates = r.expected_increment - r.actual_increment;
    return r;
}

}  // namespace itx
