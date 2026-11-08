#include "pipeline.h"

#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <mutex>
#include <numeric>
#include <random>
#include <stdexcept>
#include <thread>

#include "disk_scheduler.h"
#include "txn_scheduler.h"
#include "zipf.h"

namespace itx {
namespace {

using Clock = std::chrono::steady_clock;
double since(Clock::time_point t0) { return std::chrono::duration<double, std::milli>(Clock::now() - t0).count(); }

struct Spec { bool audit = false; Key a = 0, b = 0; Value amount = 0; int priority = 1; double cost = 0, work_us = 0; };
struct CrashReport { uint64_t acked_total, acked_transfers, aborted_attempts, completed; double elapsed_ms; };

struct Run {
    const PipelineConfig& cfg;
    TransactionManager& tm;
    const std::vector<Spec>& specs;
    Value expected_total;
    Clock::time_point t_begin;
    std::atomic<uint64_t> acked_transfers{0}, acked_audits{0}, violations{0}, gave_up{0}, aborted{0};
    std::mutex mu;  // samples + latencies
    std::vector<PipelineSample> samples;
    std::vector<double> latencies;
    struct ClassAgg { uint64_t n = 0; double wait_us = 0, turn_us = 0; } classes[3];
    std::vector<int> disk_trace;
    std::mutex ev_mu;  // deadlock trace events (called with the lock-manager mutex held)
    std::vector<StageEvent> deadlock_events;
    uint64_t deadlock_seen = 0;
    Run(const PipelineConfig& c, TransactionManager& t, const std::vector<Spec>& s, Value total)
        : cfg(c), tm(t), specs(s), expected_total(total), t_begin(Clock::now()) {}
    uint64_t acked_total() const { return acked_transfers.load() + acked_audits.load(); }
};

void spin_for_us(double us) {  // CPU-bound work done while holding all locks
    if (us <= 0) return;
    auto end = Clock::now() + std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double, std::micro>(us));
    while (Clock::now() < end) {}
}

void execute(Run& run, const ReadyJob& job, Clock::time_point picked) {
    const Spec& sp = run.specs[job.index];
    TransactionManager& tm = run.tm;
    const Key counter_key = run.cfg.accounts;
    static thread_local std::mt19937 backoff_rng(std::random_device{}());
    auto t0 = Clock::now();
    uint64_t ts = 0;
    int attempts = 0;
    for (;;) {
        ++attempts;
        auto txn = tm.begin(ts);
        ts = txn->timestamp;  // keep age across restarts (no starvation under Wait-Die)
        if (run.cfg.policy == LockPolicy::AVOID) {  // Banker's-style avoidance needs the full claim up front
            std::vector<Key> claim;
            if (sp.audit) for (Key k = 0; k < run.cfg.accounts; ++k) claim.push_back(k);
            else claim = {sp.a, sp.b, counter_key};
            tm.declare(*txn, claim);
        }
        Status st = Status::OK;
        int reads = 0, writes = 0;
        bool violation = false;
        if (sp.audit) {
            Value sum = 0;
            for (Key k = 0; k < run.cfg.accounts && st == Status::OK; ++k) {
                Value v = 0;
                st = tm.read(*txn, k, v);
                sum += v;
                ++reads;
            }
            violation = st == Status::OK && sum != run.expected_total;
        } else {
            Value x = 0, y = 0, c = 0;
            st = tm.read_for_update(*txn, sp.a, x);
            if (st == Status::OK) st = tm.read_for_update(*txn, sp.b, y);
            if (st == Status::OK) st = tm.write(*txn, sp.a, x - sp.amount);
            if (st == Status::OK) st = tm.write(*txn, sp.b, y + sp.amount);
            if (st == Status::OK) st = tm.read_for_update(*txn, counter_key, c);  // hot row
            if (st == Status::OK) st = tm.write(*txn, counter_key, c + 1);
            reads = 3; writes = 3;
        }
        if (st == Status::OK) spin_for_us(sp.work_us);  // the transaction's computation, locks still held
        if (st == Status::OK) st = tm.commit(*txn);
        if (st == Status::OK) {
            double us = std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
            if (violation) run.violations.fetch_add(1);
            (sp.audit ? run.acked_audits : run.acked_transfers).fetch_add(1);  // acknowledged AFTER commit returned
            double wait_us = std::chrono::duration<double, std::micro>(picked - job.arrived).count();
            double turn_us = std::chrono::duration<double, std::micro>(Clock::now() - job.arrived).count();
            std::lock_guard<std::mutex> g(run.mu);
            auto& ca = run.classes[std::min(2, std::max(0, sp.priority - 1))];
            ++ca.n; ca.wait_us += wait_us; ca.turn_us += turn_us;
            if (!sp.audit && run.disk_trace.size() < 3000)
                for (Key k : {sp.a, sp.b, counter_key}) run.disk_trace.push_back(static_cast<int>(k) * 10);
            run.latencies.push_back(us);
            if (static_cast<int>(run.samples.size()) < run.cfg.sample_limit)
                run.samples.push_back({txn->id, attempts, us, reads, writes, sp.audit});
            return;
        }
        tm.rollback(*txn, st);
        run.aborted.fetch_add(1);
        if (!is_abort(st) || attempts >= 100000) { run.gave_up.fetch_add(1); return; }
        int cap = 1 << std::min(attempts, 10);
        std::this_thread::sleep_for(std::chrono::microseconds(1 + backoff_rng() % cap));
    }
}

struct DriveOut {
    double elapsed_ms = 0;
    std::vector<TimelinePoint> timeline;
    std::vector<double> cp_times, cp_durations;
};

// dispatcher (this thread) -> bounded queue -> worker pool; a monitor thread samples progress,
// takes checkpoints concurrently with the load and (crash mode) kills the process.
DriveOut drive(Run& run, const std::vector<size_t>& order, int crash_fd) {
    const PipelineConfig& cfg = run.cfg;
    run.tm.locks().set_trace([&run](const std::string& ev, TxnId txn, const std::string& detail) {
        if (ev != "DEADLOCK_DETECTED") return;
        std::lock_guard<std::mutex> g(run.ev_mu);
        ++run.deadlock_seen;
        if (run.deadlock_events.size() < 10)
            run.deadlock_events.push_back({since(run.t_begin), "DEADLOCK_DETECTED", detail + " [T" + std::to_string(txn) + "]"});
    });
    DriveOut out;
    std::atomic<bool> stop{false};
    std::thread monitor([&] {
        double last_cp = 0;
        while (!stop.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            double t = since(run.t_begin);
            EngineStats es = run.tm.stats();
            out.timeline.push_back({t, es.committed, es.aborted});
            if (crash_fd >= 0 && run.acked_total() >= cfg.crash_after_commits) {
                CrashReport cr{run.acked_total(), run.acked_transfers.load(), run.aborted.load(), 0, t};
                if (::write(crash_fd, &cr, sizeof cr) < 0) _exit(7);
                _exit(0);  // abrupt: no destructors, no flush, workers still running
            }
            if (cfg.wal && cfg.checkpoint_every_ms > 0 && t - last_cp >= cfg.checkpoint_every_ms) {
                double c0 = since(run.t_begin);
                if (run.tm.checkpoint() == Status::OK) {
                    out.cp_times.push_back(c0);
                    out.cp_durations.push_back(since(run.t_begin) - c0);
                }
                last_cp = since(run.t_begin);
            }
        }
    });
    {
        TxnScheduler sched(cfg.admission, cfg.queue_capacity);  // REAL ready queue: the policy picks the next transaction
        std::vector<std::thread> workers;
        for (size_t w = 0; w < cfg.workers; ++w)
            workers.emplace_back([&] { ReadyJob j; while (sched.pop(j)) execute(run, j, Clock::now()); });
        uint64_t seq = 0;
        for (size_t idx : order) {  // dispatcher: transactions arrive in arrival order; push blocks when the ready queue is full
            const Spec& sp = run.specs[idx];
            sched.push({idx, seq++, sp.cost, sp.priority, Clock::now()});
        }
        sched.close();
        for (auto& t : workers) t.join();
    }
    out.elapsed_ms = since(run.t_begin);
    stop = true;
    monitor.join();
    EngineStats es = run.tm.stats();
    out.timeline.push_back({out.elapsed_ms, es.committed, es.aborted});
    return out;
}

double percentile(const std::vector<double>& sorted, double p) {
    if (sorted.empty()) return 0;
    size_t i = static_cast<size_t>(std::ceil(p * sorted.size())) - 1;
    return sorted[std::min(i, sorted.size() - 1)];
}

std::vector<Value> snapshot_values(const TransactionManager& tm, uint32_t n) {
    std::vector<Value> v(n);
    for (uint32_t k = 0; k < n; ++k) v[k] = tm.peek(k);
    return v;
}

}  // namespace

PipelineResult run_pipeline(const PipelineConfig& cfg) {
    if (cfg.transactions < 10 || cfg.transactions > 1000000) throw std::invalid_argument("transactions must be in [10, 1000000]");
    if (cfg.workers == 0 || cfg.workers > 128) throw std::invalid_argument("workers must be in [1, 128]");
    if (cfg.queue_capacity == 0) throw std::invalid_argument("queue capacity must be > 0");
    if (cfg.accounts < 2 || cfg.accounts > 100000) throw std::invalid_argument("accounts must be in [2, 100000]");
    if (cfg.theta < 0 || cfg.theta > 1.5) throw std::invalid_argument("theta must be in [0, 1.5]");
    if (cfg.policy == LockPolicy::NONE) throw std::invalid_argument("policy none is unsafe and not allowed in the pipeline");
    if (cfg.admission != SchedAlgo::FCFS && cfg.admission != SchedAlgo::SJF && cfg.admission != SchedAlgo::PRIORITY)
        throw std::invalid_argument("admission policy must be FCFS, SJF or PRIORITY (preemptive policies would keep a descheduled transaction's locks)");
    if (cfg.work_us < 0 || cfg.work_us > 10000) throw std::invalid_argument("work_us must be in [0, 10000]");
    if (cfg.wal && cfg.dir.empty()) throw std::invalid_argument("a WAL directory is required when wal is enabled");
    if (cfg.crash_after_commits > 0 && !cfg.wal) throw std::invalid_argument("crash testing needs the write-ahead log");
    if (cfg.crash_after_commits >= cfg.transactions) throw std::invalid_argument("crash point must be below the number of transactions");

    PipelineResult r;
    const Clock::time_point t_begin = Clock::now();
    auto stage = [&](const std::string& s, const std::string& d) { r.stages.push_back({since(t_begin), s, d}); };
    auto check = [&](const std::string& n, const std::string& e, const std::string& a, bool ok) { r.checks.push_back({n, e, a, ok}); };
    auto check_eq = [&](const std::string& n, long long e, long long a) { check(n, std::to_string(e), std::to_string(a), e == a); };

    // 1) jobs
    std::vector<Job> jobs = make_transaction_jobs(static_cast<int>(cfg.transactions), cfg.seed, std::max(1.0, cfg.transactions / 10.0));
    uint64_t audits = 0;
    for (const Job& j : jobs) audits += j.priority == 3 ? 1 : 0;
    r.transactions = cfg.transactions;
    r.audits = audits;
    stage("JOB_GENERATION", std::to_string(cfg.transactions - audits) + " transfer transactions (each also increments one hot counter row) + " +
                                std::to_string(audits) + " audit transactions (read all " + std::to_string(cfg.accounts) + " accounts)");

    // 2) admission order from the CPU-scheduling policy (simulated)
    ScheduleResult sim = simulate_schedule(cfg.admission, jobs, cfg.quantum);
    std::vector<size_t> order(jobs.size());
    std::iota(order.begin(), order.end(), 0);
    std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) { return jobs[a].arrival < jobs[b].arrival; });
    r.sim_avg_waiting = sim.avg_waiting; r.sim_avg_turnaround = sim.avg_turnaround; r.sim_avg_response = sim.avg_response;
    stage("ADMISSION_SCHEDULING", std::string(to_string(cfg.admission)) + " (REAL, non-preemptive) picks which waiting transaction a free worker runs next; the simulator predicts avg waiting " +
                                      std::to_string(sim.avg_waiting).substr(0, 6) + ", turnaround " + std::to_string(sim.avg_turnaround).substr(0, 6) + " time units for the same jobs");

    // 3) transaction specifications (deterministic for a seed)
    ZipfGenerator zipf(cfg.accounts, cfg.theta);
    std::vector<Spec> specs(jobs.size());
    for (size_t i = 0; i < jobs.size(); ++i) {
        std::mt19937_64 rng(cfg.seed * 1000003ULL + static_cast<uint64_t>(jobs[i].id));
        specs[i].audit = jobs[i].priority == 3;
        specs[i].priority = jobs[i].priority;
        specs[i].cost = jobs[i].burst;
        specs[i].work_us = cfg.work_us * jobs[i].burst;
        if (!specs[i].audit) {
            do { specs[i].a = zipf.next(rng); specs[i].b = zipf.next(rng); } while (specs[i].a == specs[i].b);
            specs[i].amount = static_cast<Value>(1 + rng() % 50);
        }
    }
    const Value expected_total = static_cast<Value>(cfg.accounts) * cfg.initial_balance;
    const uint32_t records = cfg.accounts + 1;  // + hot counter
    const Value counter_base = cfg.initial_balance;  // every record starts at initial_balance
    EngineConfig ec;
    ec.num_records = records;
    ec.initial_value = cfg.initial_balance;
    ec.policy = cfg.policy;
    ec.data_dir = cfg.wal ? cfg.dir : "";
    ec.sync_commit = cfg.sync_commit;

    auto account_sum = [&](const TransactionManager& tm) { Value s = 0; for (Key k = 0; k < cfg.accounts; ++k) s += tm.peek(k); return s; };

    if (cfg.crash_after_commits == 0) {
        // ---------------- normal run ----------------
        auto tm = std::make_unique<TransactionManager>(ec);
        stage("ENGINE_START", std::string("policy ") + to_string(cfg.policy) + ", " + std::to_string(cfg.workers) + " workers, queue " +
                                  std::to_string(cfg.queue_capacity) + (cfg.wal ? std::string(", WAL on") + (cfg.sync_commit ? " (fsync at commit)" : "") : ", WAL off"));
        std::vector<Value> final_values;
        {
            Run run(cfg, *tm, specs, expected_total);
            stage("DISPATCH", "dispatcher submits " + std::to_string(order.size()) + " transactions; workers execute them concurrently");
            DriveOut out = drive(run, order, -1);
            r.elapsed_ms = out.elapsed_ms; r.timeline = out.timeline;
            r.checkpoint_times_ms = out.cp_times; r.checkpoint_durations_ms = out.cp_durations;
            r.transfers_committed = run.acked_transfers; r.audits_committed = run.acked_audits;
            r.audit_violations = run.violations; r.gave_up = run.gave_up; r.aborted_attempts = run.aborted;
            r.samples = run.samples;
            double tw = 0, tt = 0;
            uint64_t tn = 0;
            for (int c = 0; c < 3; ++c) {
                const auto& ca = run.classes[c];
                if (!ca.n) continue;
                r.classes.push_back({c + 1, ca.n, ca.wait_us / ca.n, ca.turn_us / ca.n});
                tw += ca.wait_us; tt += ca.turn_us; tn += ca.n;
            }
            if (tn) { r.real_avg_wait_us = tw / tn; r.real_avg_turnaround_us = tt / tn; }
            if (!run.disk_trace.empty()) {
                r.disk_requests = run.disk_trace.size();
                for (DiskAlgo a : all_disk_algos())
                    r.disk.push_back({to_string(a), schedule_disk(a, run.disk_trace, 0, static_cast<int>(records) * 10).total_movement});
            }
            std::sort(run.latencies.begin(), run.latencies.end());
            if (!run.latencies.empty()) {
                double s = 0;
                for (double d : run.latencies) s += d;
                r.avg_latency_us = s / run.latencies.size();
                r.p50_latency_us = percentile(run.latencies, 0.50);
                r.p95_latency_us = percentile(run.latencies, 0.95);
                r.p99_latency_us = percentile(run.latencies, 0.99);
            }
            for (const auto& e : run.deadlock_events) r.stages.push_back(e);
            r.deadlock_cycles = run.deadlock_seen;
        }
        EngineStats es = tm->stats();
        LockStats ls = tm->locks().stats();
        r.committed = es.committed; r.checkpoints = es.checkpoints;
        r.aborted_wait_die = es.aborted_wait_die; r.aborted_no_wait = es.aborted_no_wait; r.aborted_deadlock = es.aborted_deadlock;
        r.lock_waits = ls.waits; r.unsafe_denials = ls.unsafe_denials; r.wal_bytes = tm->log_bytes();
        r.throughput_tps = r.elapsed_ms > 0 ? r.committed / (r.elapsed_ms / 1000.0) : 0;
        for (size_t i = 0; i < r.checkpoint_times_ms.size(); ++i)
            r.stages.push_back({r.checkpoint_times_ms[i], "CHECKPOINT", "taken while load runs (waited for quiescence " + std::to_string(r.checkpoint_durations_ms[i]).substr(0, 5) + " ms)"});
        stage("DONE", std::to_string(r.committed) + " committed, " + std::to_string(r.aborted_attempts) + " aborted attempts retried, " +
                          std::to_string(r.deadlock_cycles) + " deadlock cycles broken");

        check_eq("every transaction committed exactly once", static_cast<long long>(cfg.transactions), static_cast<long long>(r.committed));
        check_eq("no transaction gave up after retries", 0, static_cast<long long>(r.gave_up));
        check_eq("money conserved across all accounts (atomicity + consistency)", expected_total, account_sum(*tm));
        check_eq("hot counter == committed transfers (no lost update on the hottest row)", counter_base + static_cast<long long>(r.transfers_committed), tm->peek(cfg.accounts));
        check_eq("audits always saw the same total (isolation: no dirty/non-repeatable reads)", 0, static_cast<long long>(r.audit_violations));
        check_eq("no locks left and no transaction still waiting (no lock leak / no hang)", 0, static_cast<long long>(tm->locks().total_lock_entries() + tm->locks().waiting_count()));
        if (cfg.wal) {
            final_values = snapshot_values(*tm, records);
            tm.reset();  // graceful shutdown, then restart from the WAL (+ snapshot)
            TransactionManager again(ec);
            const RecoveryReport& rep = again.recovery_report();
            r.recovery_redo = rep.redo_applied; r.recovery_undo = rep.undo_applied;
            r.recovery_losers = rep.losers.size(); r.recovery_committed = rep.committed.size();
            check("restart from WAL reproduces exactly the same database (durability)", "identical", snapshot_values(again, records) == final_values ? "identical" : "DIFFERENT",
                  snapshot_values(again, records) == final_values);
            check_eq("restart found no unfinished transactions to undo", 0, static_cast<long long>(rep.losers.size()));
            stage("RESTART_CHECK", "clean restart: redo " + std::to_string(rep.redo_applied) + ", undo " + std::to_string(rep.undo_applied) +
                                       (rep.snapshot_loaded ? ", snapshot loaded" : ", no snapshot"));
        }
    } else {
        // ---------------- crash run: child process is killed mid-load, parent recovers ----------------
        stage("ENGINE_START", std::string("policy ") + to_string(cfg.policy) + ", " + std::to_string(cfg.workers) + " workers, WAL on; the process will be KILLED after " +
                                  std::to_string(cfg.crash_after_commits) + " commits");
        int fds[2];
        if (::pipe(fds) != 0) throw std::runtime_error("pipe failed");
        pid_t pid = ::fork();
        if (pid < 0) throw std::runtime_error("fork failed");
        if (pid == 0) {
            ::close(fds[0]);
            try {
                TransactionManager tm(ec);
                Run run(cfg, tm, specs, expected_total);
                DriveOut out = drive(run, order, fds[1]);
                CrashReport cr{run.acked_total(), run.acked_transfers.load(), run.aborted.load(), 1, out.elapsed_ms};
                if (::write(fds[1], &cr, sizeof cr) < 0) _exit(7);
            } catch (...) {}
            _exit(0);
        }
        ::close(fds[1]);
        CrashReport cr{};
        ssize_t n = ::read(fds[0], &cr, sizeof cr);
        ::close(fds[0]);
        int status = 0;
        ::waitpid(pid, &status, 0);
        if (n != static_cast<ssize_t>(sizeof cr)) throw std::runtime_error("the crash-test process died before reporting");
        if (cr.completed) throw std::runtime_error("workload finished before the crash point; lower the crash point");
        r.crash_injected = true;
        r.elapsed_ms = cr.elapsed_ms;
        r.aborted_attempts = cr.aborted_attempts;
        r.transfers_committed = cr.acked_transfers;
        stage("CRASH_INJECTED", "process killed (_exit) after " + std::to_string(cr.acked_total) + " acknowledged commits, with transactions still in flight");

        auto tm = std::make_unique<TransactionManager>(ec);
        const RecoveryReport& rep = tm->recovery_report();
        r.recovery_redo = rep.redo_applied; r.recovery_undo = rep.undo_applied;
        r.recovery_losers = rep.losers.size(); r.recovery_committed = rep.committed.size();
        r.committed = rep.committed.size();
        stage("RECOVERY", std::string(rep.snapshot_loaded ? "snapshot loaded, " : "no snapshot, ") + "redo " + std::to_string(rep.redo_applied) + ", undo " + std::to_string(rep.undo_applied) +
                              ", " + std::to_string(rep.losers.size()) + " in-flight transaction(s) rolled back" + (rep.torn_tail ? ", torn log tail repaired" : ""));
        Value recovered_transfers = tm->peek(cfg.accounts) - counter_base;
        check_eq("money conserved after crash (no half-applied transfer survived)", expected_total, account_sum(*tm));
        check("every acknowledged transfer survived the crash (durability)", ">= " + std::to_string(cr.acked_transfers), std::to_string(recovered_transfers),
              recovered_transfers >= static_cast<Value>(cr.acked_transfers));
        check("no transfer appears that was never submitted", "<= " + std::to_string(cfg.transactions - audits), std::to_string(recovered_transfers),
              recovered_transfers <= static_cast<Value>(cfg.transactions - audits));
        // engine must be usable again
        for (int i = 0; i < 50; ++i) {
            auto t = tm->begin();
            Value x = 0, y = 0, c = 0;
            if (tm->read_for_update(*t, 0, x) != Status::OK || tm->read_for_update(*t, 1, y) != Status::OK) throw std::runtime_error("post-recovery txn failed");
            tm->write(*t, 0, x - 1); tm->write(*t, 1, y + 1);
            tm->read_for_update(*t, cfg.accounts, c); tm->write(*t, cfg.accounts, c + 1);
            if (tm->commit(*t) != Status::OK) throw std::runtime_error("post-recovery commit failed");
        }
        check_eq("engine keeps working after recovery (50 new transfers, money still conserved)", expected_total, account_sum(*tm));
        check_eq("new transfers were applied on top of the recovered state", recovered_transfers + 50, tm->peek(cfg.accounts) - counter_base);
        std::vector<Value> before = snapshot_values(*tm, records);
        tm.reset();
        TransactionManager again(ec);
        check("second restart is idempotent (same database, nothing left to undo)", "identical", snapshot_values(again, records) == before && again.recovery_report().undo_applied == 0 ? "identical" : "DIFFERENT",
              snapshot_values(again, records) == before && again.recovery_report().undo_applied == 0);
        r.transfers_committed = static_cast<uint64_t>(recovered_transfers);
        stage("DONE", "database recovered and verified");
    }
    std::stable_sort(r.stages.begin(), r.stages.end(), [](const StageEvent& a, const StageEvent& b) { return a.t_ms < b.t_ms; });
    r.all_pass = true;
    for (const auto& c : r.checks) r.all_pass = r.all_pass && c.pass;
    return r;
}

}  // namespace itx
