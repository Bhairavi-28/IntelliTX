#include <cstdlib>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>

#include "deadlock_detector.h"
#include "disk_scheduler.h"
#include "file_allocation.h"
#include "json_writer.h"
#include "pipeline.h"
#include "scenarios.h"
#include "scheduler.h"
#include "workload.h"

using namespace itx;

namespace {

using Args = std::map<std::string, std::string>;

Args parse_args(int argc, char** argv, int from) {
    Args a;
    for (int i = from; i < argc; ++i) {
        std::string k = argv[i];
        if (k.rfind("--", 0) != 0) throw std::invalid_argument("unexpected argument: " + k);
        k = k.substr(2);
        if (i + 1 >= argc) throw std::invalid_argument("missing value for --" + k);
        a[k] = argv[++i];
    }
    return a;
}

std::string get(const Args& a, const std::string& k, const std::string& def) {
    auto it = a.find(k);
    return it == a.end() ? def : it->second;
}
long get_int(const Args& a, const std::string& k, long def, long lo, long hi) {
    auto it = a.find(k);
    if (it == a.end()) return def;
    char* end = nullptr;
    long v = std::strtol(it->second.c_str(), &end, 10);
    if (end == it->second.c_str() || *end != '\0') throw std::invalid_argument("--" + k + " must be an integer");
    if (v < lo || v > hi) throw std::invalid_argument("--" + k + " out of range [" + std::to_string(lo) + "," + std::to_string(hi) + "]");
    return v;
}
double get_double(const Args& a, const std::string& k, double def, double lo, double hi) {
    auto it = a.find(k);
    if (it == a.end()) return def;
    char* end = nullptr;
    double v = std::strtod(it->second.c_str(), &end);
    if (end == it->second.c_str() || *end != '\0') throw std::invalid_argument("--" + k + " must be a number");
    if (v < lo || v > hi) throw std::invalid_argument("--" + k + " out of range");
    return v;
}

std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    std::string cur;
    std::istringstream is(s);
    while (std::getline(is, cur, sep))
        if (!cur.empty()) out.push_back(cur);
    return out;
}
std::vector<int> int_list(const std::string& s) {
    std::vector<int> v;
    for (const auto& t : split(s, ',')) {
        char* end = nullptr;
        long x = std::strtol(t.c_str(), &end, 10);
        if (*end != '\0') throw std::invalid_argument("bad integer list: " + s);
        v.push_back(static_cast<int>(x));
    }
    return v;
}
Matrix matrix_of(const std::string& s) {
    Matrix m;
    for (const auto& row : split(s, ';')) m.push_back(int_list(row));
    return m;
}

void emit_schedule(JsonWriter& w, const ScheduleResult& r) {
    w.begin_object();
    w.field("algorithm", r.algorithm);
    w.field("avg_waiting", r.avg_waiting).field("avg_turnaround", r.avg_turnaround);
    w.field("avg_response", r.avg_response).field("avg_completion", r.avg_completion);
    w.field("makespan", r.makespan).field("cpu_utilization", r.cpu_utilization);
    w.field("throughput_jobs_per_unit", r.throughput).field("context_switches", r.context_switches);
    w.key("jobs").begin_array();
    for (const auto& j : r.jobs) {
        w.begin_object();
        w.field("id", j.id).field("arrival", j.arrival).field("burst", j.burst).field("priority", j.priority);
        w.field("first_start", j.first_start).field("completion", j.completion);
        w.field("turnaround", j.turnaround).field("waiting", j.waiting).field("response", j.response);
        w.end_object();
    }
    w.end_array();
    w.key("gantt").begin_array();
    for (const auto& g : r.gantt) {
        w.begin_object().field("job", g.job_id).field("start", g.start).field("end", g.end).end_object();
    }
    w.end_array();
    w.end_object();
}

int cmd_bench(const Args& a) {
    WorkloadConfig wc;
    if (!parse_workload(get(a, "workload", "ycsb"), wc.type)) throw std::invalid_argument("--workload must be ycsb|transfer");
    LockPolicy policy;
    if (!parse_policy(get(a, "policy", "wait_die"), policy)) throw std::invalid_argument("--policy must be wait_die|no_wait|detect|avoid|none");
    wc.workers = static_cast<size_t>(get_int(a, "workers", 4, 1, 64));
    wc.transactions = static_cast<size_t>(get_int(a, "txns", 2000, 1, 5000000));
    wc.records = static_cast<uint32_t>(get_int(a, "records", 1000, 2, 5000000));
    wc.ops_per_txn = static_cast<int>(get_int(a, "ops", 8, 1, 64));
    wc.write_ratio = get_double(a, "write-ratio", 0.5, 0, 1);
    wc.theta = get_double(a, "theta", 0.6, 0, 1.5);
    wc.ordered_access = get_int(a, "ordered", 0, 0, 1) == 1;
    wc.seed = static_cast<unsigned>(get_int(a, "seed", 42, 0, 2147483647));
    wc.sample_limit = static_cast<int>(get_int(a, "samples", 200, 0, 5000));
    wc.think_micros = static_cast<uint64_t>(get_int(a, "think-us", 0, 0, 100000));

    EngineConfig ec;
    ec.num_records = wc.records;
    ec.policy = policy;
    ec.initial_value = wc.type == WorkloadType::TRANSFER ? 1000 : 0;
    ec.data_dir = get(a, "dir", "");
    ec.sync_commit = get_int(a, "sync", 0, 0, 1) == 1;
    TransactionManager engine(ec);
    LockStats before = engine.locks().stats();
    (void)before;
    WorkloadResult r = run_workload(engine, wc);
    LockStats ls = engine.locks().stats();

    JsonWriter w;
    w.begin_object();
    w.field("kind", "measured_engine");
    w.field("workload", to_string(wc.type)).field("policy", to_string(policy));
    w.field("workers", wc.workers).field("txns_requested", wc.transactions);
    w.field("records", wc.records).field("ops_per_txn", wc.type == WorkloadType::YCSB ? wc.ops_per_txn : 2);
    w.field("write_ratio", wc.write_ratio).field("theta", wc.theta);
    w.field("ordered_access", wc.ordered_access).field("seed", wc.seed);
    w.field("wal_enabled", engine.durable()).field("sync_commit", ec.sync_commit);
    w.field("elapsed_ms", r.elapsed_ms).field("committed", r.committed).field("gave_up", r.gave_up);
    w.field("aborted_attempts", r.aborted_attempts);
    w.field("aborted_wait_die", r.aborted_wait_die).field("aborted_no_wait", r.aborted_no_wait);
    w.field("aborted_deadlock", r.aborted_deadlock).field("lock_waits", r.lock_waits);
    w.field("throughput_tps", r.throughput_tps);
    w.field("abort_rate", r.committed + r.aborted_attempts > 0
                              ? static_cast<double>(r.aborted_attempts) / (r.committed + r.aborted_attempts) : 0.0);
    w.field("avg_latency_us", r.avg_latency_us).field("p50_latency_us", r.p50_latency_us);
    w.field("p95_latency_us", r.p95_latency_us).field("p99_latency_us", r.p99_latency_us);
    w.field("deadlock_cycles_detected", ls.deadlock_cycles);
    w.field("lock_entries_left", engine.locks().total_lock_entries());
    w.field("invariant_ok", r.invariant_ok).field("invariant_detail", r.invariant_detail);
    w.key("txn_samples").begin_array();
    for (const auto& s : r.samples) {
        w.begin_object().field("txn_ref", s.txn_ref).field("attempts", s.attempts);
        w.field("latency_us", s.latency_us).field("reads", s.reads).field("writes", s.writes).end_object();
    }
    w.end_array();
    w.end_object();
    std::cout << w.str() << "\n";
    return r.invariant_ok ? 0 : 1;
}

int cmd_sched(const Args& a) {
    std::vector<Job> jobs;
    std::string source;
    if (a.count("jobs")) {  // id:arrival:burst:priority,...
        source = "explicit";
        for (const auto& item : split(a.at("jobs"), ',')) {
            auto f = split(item, ':');
            if (f.size() != 4) throw std::invalid_argument("job format is id:arrival:burst:priority");
            jobs.push_back({std::stoi(f[0]), std::stod(f[1]), std::stod(f[2]), std::stoi(f[3])});
        }
    } else {
        source = "synthetic_transaction_jobs";
        jobs = make_transaction_jobs(static_cast<int>(get_int(a, "txn-jobs", 12, 1, 5000)),
                                     static_cast<unsigned>(get_int(a, "seed", 42, 0, 2147483647)),
                                     get_double(a, "spread", 20, 0, 100000));
    }
    double quantum = get_double(a, "quantum", 4, 0.001, 1e9);
    std::vector<SchedAlgo> algos;
    std::string wanted = get(a, "algos", "all");
    if (wanted == "all") algos = all_sched_algos();
    else for (const auto& t : split(wanted, ',')) {
        SchedAlgo al;
        if (!parse_sched_algo(t, al)) throw std::invalid_argument("unknown algorithm: " + t);
        algos.push_back(al);
    }
    JsonWriter w;
    w.begin_object().field("kind", "simulator").field("job_source", source).field("quantum", quantum);
    w.key("jobs").begin_array();
    for (const auto& j : jobs)
        w.begin_object().field("id", j.id).field("arrival", j.arrival).field("burst", j.burst).field("priority", j.priority).end_object();
    w.end_array();
    w.key("results").begin_array();
    for (SchedAlgo al : algos) emit_schedule(w, simulate_schedule(al, jobs, quantum));
    w.end_array().end_object();
    std::cout << w.str() << "\n";
    return 0;
}

int cmd_disk(const Args& a) {
    std::vector<int> req = int_list(get(a, "requests", "98,183,37,122,14,124,65,67"));
    int head = static_cast<int>(get_int(a, "head", 53, 0, 100000000));
    int size = static_cast<int>(get_int(a, "size", 200, 1, 100000000));
    bool up = get(a, "direction", "up") == "up";
    std::vector<DiskAlgo> algos;
    std::string wanted = get(a, "algos", "all");
    if (wanted == "all") algos = all_disk_algos();
    else for (const auto& t : split(wanted, ',')) {
        DiskAlgo al;
        if (!parse_disk_algo(t, al)) throw std::invalid_argument("unknown algorithm: " + t);
        algos.push_back(al);
    }
    JsonWriter w;
    w.begin_object().field("kind", "simulator").field("head", head).field("disk_size", size);
    w.field("direction", up ? "up" : "down");
    w.int_array("requests", req);
    w.key("results").begin_array();
    for (DiskAlgo al : algos) {
        DiskResult r = schedule_disk(al, req, head, size, up);
        w.begin_object().field("algorithm", r.algorithm).field("total_movement", r.total_movement);
        w.field("return_jump", r.return_jump).field("avg_movement_per_request", r.avg_movement_per_request);
        w.int_array("service_order", r.service_order).int_array("head_path", r.head_path).end_object();
    }
    w.end_array().end_object();
    std::cout << w.str() << "\n";
    return 0;
}

int cmd_alloc(const Args& a) {
    int blocks = static_cast<int>(get_int(a, "blocks", 32, 1, 1000000));
    int idx_cap = static_cast<int>(get_int(a, "index-capacity", 128, 1, 100000));
    std::vector<FileOp> ops;
    for (const auto& item : split(get(a, "ops", "create:A:5,create:B:3,create:C:6,remove:B,create:D:4,create:E:5"), ',')) {
        auto f = split(item, ':');
        FileOp op;
        if (f.size() == 3 && f[0] == "create") { op.kind = FileOp::CREATE; op.name = f[1]; op.blocks = std::stoi(f[2]); }
        else if (f.size() == 2 && f[0] == "remove") { op.kind = FileOp::REMOVE; op.name = f[1]; }
        else throw std::invalid_argument("op must be create:NAME:BLOCKS or remove:NAME");
        ops.push_back(op);
    }
    std::vector<AllocMethod> methods;
    std::string wanted = get(a, "methods", "all");
    if (wanted == "all")
        methods = {AllocMethod::CONTIGUOUS_FIRST_FIT, AllocMethod::CONTIGUOUS_BEST_FIT, AllocMethod::LINKED, AllocMethod::INDEXED};
    else for (const auto& t : split(wanted, ',')) {
        AllocMethod m;
        if (!parse_alloc_method(t, m)) throw std::invalid_argument("unknown method: " + t);
        methods.push_back(m);
    }
    JsonWriter w;
    w.begin_object().field("kind", "simulator").field("disk_blocks", blocks);
    w.key("results").begin_array();
    for (AllocMethod m : methods) {
        AllocResult r = simulate_allocation(m, blocks, ops, idx_cap);
        w.begin_object().field("method", r.method).field("used_blocks", r.used_blocks);
        w.field("free_blocks", r.free_blocks).field("overhead_blocks", r.overhead_blocks);
        w.field("failed_creates", r.failed_creates);
        w.field("external_fragmentation_pct", r.external_fragmentation_pct);
        w.field("avg_blocks_read_to_reach_last_block", r.avg_blocks_read_to_reach_last_block);
        w.key("outcomes").begin_array();
        for (const auto& o : r.outcomes) {
            w.begin_object().field("op", o.op.kind == FileOp::CREATE ? "create" : "remove");
            w.field("name", o.op.name).field("blocks_requested", o.op.blocks).field("success", o.success);
            w.field("message", o.message).int_array("blocks", o.blocks).end_object();
        }
        w.end_array();
        w.key("free_extents").begin_array();
        for (const auto& e : r.free_extents) w.begin_object().field("start", e.start).field("length", e.length).end_object();
        w.end_array();
        w.key("block_owner").begin_array();
        for (const auto& s : r.block_owner) w.value(s);
        w.end_array().end_object();
    }
    w.end_array().end_object();
    std::cout << w.str() << "\n";
    return 0;
}

int cmd_deadlock(const Args& a) {
    LockPolicy p;
    if (!parse_policy(get(a, "policy", "detect"), p) || p == LockPolicy::NONE)
        throw std::invalid_argument("--policy must be wait_die|no_wait|detect|avoid");
    DeadlockScenarioResult r = run_deadlock_scenario(p);
    JsonWriter w;
    w.begin_object().field("kind", "measured_engine").field("policy", r.policy).field("mechanism", r.mechanism);
    w.field("cycle_detected", r.deadlock_cycle_detected);
    w.field("t1_committed", r.t1_committed).field("t2_committed", r.t2_committed);
    w.field("t1_attempts", r.t1_attempts).field("t2_attempts", r.t2_attempts);
    w.field("aborted_wait_die", r.aborted_wait_die).field("aborted_no_wait", r.aborted_no_wait);
    w.field("aborted_deadlock", r.aborted_deadlock);
    w.field("final_a", r.final_a).field("final_b", r.final_b).field("consistent", r.consistent);
    w.field("lock_entries_left", r.lock_entries_left).field("unsafe_denials", r.unsafe_denials);
    w.key("events").begin_array();
    for (const auto& e : r.events)
        w.begin_object().field("seq", e.seq).field("event", e.event).field("txn", e.txn).field("detail", e.detail).end_object();
    w.end_array().end_object();
    std::cout << w.str() << "\n";
    return r.consistent ? 0 : 1;
}

int cmd_recovery(const Args& a) {
    std::string dir = get(a, "dir", "");
    if (dir.empty()) throw std::invalid_argument("--dir is required");
    RecoveryScenarioResult r = run_recovery_scenario(dir, get(a, "scenario", "crash"));
    JsonWriter w;
    w.begin_object().field("kind", "measured_engine").field("scenario", r.scenario).field("all_pass", r.all_pass);
    w.key("report").begin_object();
    w.field("snapshot_loaded", r.report.snapshot_loaded).field("snapshot_lsn", r.report.snapshot_lsn);
    w.field("records_scanned", r.report.records_scanned).field("redo_applied", r.report.redo_applied);
    w.field("undo_applied", r.report.undo_applied).field("torn_tail", r.report.torn_tail);
    w.field("committed_txns", r.report.committed.size()).field("losers", r.report.losers.size());
    w.field("rolled_back_before_crash", r.report.rolled_back_before_crash.size()).end_object();
    w.key("checks").begin_array();
    for (const auto& c : r.checks)
        w.begin_object().field("name", c.name).field("expected", c.expected).field("actual", c.actual).field("pass", c.pass).end_object();
    w.end_array().end_object();
    std::cout << w.str() << "\n";
    return r.all_pass ? 0 : 1;
}

int cmd_pipeline(const Args& a) {
    PipelineConfig pc;
    if (!parse_policy(get(a, "policy", "wait_die"), pc.policy) || pc.policy == LockPolicy::NONE)
        throw std::invalid_argument("--policy must be wait_die|no_wait|detect|avoid");
    if (!parse_sched_algo(get(a, "admission", "FCFS"), pc.admission)) throw std::invalid_argument("unknown --admission algorithm");
    pc.transactions = static_cast<size_t>(get_int(a, "txns", 1000, 10, 1000000));
    pc.workers = static_cast<size_t>(get_int(a, "workers", 8, 1, 128));
    pc.queue_capacity = static_cast<size_t>(get_int(a, "queue", 256, 1, 100000));
    pc.work_us = get_double(a, "work-us", 10, 0, 10000);
    pc.accounts = static_cast<uint32_t>(get_int(a, "accounts", 100, 2, 100000));
    pc.theta = get_double(a, "theta", 0.8, 0, 1.5);
    pc.quantum = get_double(a, "quantum", 4, 0.001, 1e9);
    pc.wal = get_int(a, "wal", 1, 0, 1) == 1;
    pc.sync_commit = get_int(a, "sync", 0, 0, 1) == 1;
    pc.checkpoint_every_ms = static_cast<int>(get_int(a, "checkpoint-ms", 0, 0, 3600000));
    pc.crash_after_commits = static_cast<uint64_t>(get_int(a, "crash-after", 0, 0, 1000000));
    pc.seed = static_cast<unsigned>(get_int(a, "seed", 42, 0, 2147483647));
    pc.sample_limit = static_cast<int>(get_int(a, "samples", 200, 0, 5000));
    pc.dir = get(a, "dir", "");
    PipelineResult r = run_pipeline(pc);
    JsonWriter w;
    w.begin_object().field("kind", "measured_engine").field("policy", to_string(pc.policy)).field("admission", to_string(pc.admission));
    w.field("transactions", r.transactions).field("audits", r.audits).field("workers", pc.workers).field("accounts", pc.accounts);
    w.field("theta", pc.theta).field("wal", pc.wal).field("sync_commit", pc.sync_commit).field("checkpoint_ms", pc.checkpoint_every_ms);
    w.field("crash_injected", r.crash_injected).field("all_pass", r.all_pass);
    w.field("elapsed_ms", r.elapsed_ms).field("throughput_tps", r.throughput_tps).field("committed", r.committed);
    w.field("transfers_committed", r.transfers_committed).field("audits_committed", r.audits_committed);
    w.field("aborted_attempts", r.aborted_attempts).field("aborted_wait_die", r.aborted_wait_die);
    w.field("aborted_no_wait", r.aborted_no_wait).field("aborted_deadlock", r.aborted_deadlock);
    w.field("lock_waits", r.lock_waits).field("deadlock_cycles_detected", r.deadlock_cycles).field("checkpoints", r.checkpoints);
    w.field("audit_violations", r.audit_violations).field("gave_up", r.gave_up).field("wal_bytes", r.wal_bytes);
    w.field("avg_latency_us", r.avg_latency_us).field("p50_latency_us", r.p50_latency_us);
    w.field("p95_latency_us", r.p95_latency_us).field("p99_latency_us", r.p99_latency_us);
    w.field("recovery_redo", r.recovery_redo).field("recovery_undo", r.recovery_undo);
    w.field("recovery_losers", r.recovery_losers).field("recovery_committed", r.recovery_committed);
    w.field("work_us", pc.work_us).field("unsafe_denials", r.unsafe_denials);
    w.field("real_avg_wait_us", r.real_avg_wait_us).field("real_avg_turnaround_us", r.real_avg_turnaround_us);
    w.key("classes").begin_array();
    for (const auto& c : r.classes) w.begin_object().field("priority", c.priority).field("count", c.count).field("avg_wait_us", c.avg_wait_us).field("avg_turnaround_us", c.avg_turnaround_us).end_object();
    w.end_array();
    w.key("disk").begin_object().field("requests", r.disk_requests).key("results").begin_array();
    for (const auto& d : r.disk) w.begin_object().field("algorithm", d.algorithm).field("total_movement", d.total_movement).end_object();
    w.end_array().end_object();
    w.field("sim_avg_waiting", r.sim_avg_waiting).field("sim_avg_turnaround", r.sim_avg_turnaround).field("sim_avg_response", r.sim_avg_response);
    w.key("stages").begin_array();
    for (const auto& s : r.stages) w.begin_object().field("t_ms", s.t_ms).field("stage", s.stage).field("detail", s.detail).end_object();
    w.end_array();
    w.key("checks").begin_array();
    for (const auto& c : r.checks) w.begin_object().field("name", c.name).field("expected", c.expected).field("actual", c.actual).field("pass", c.pass).end_object();
    w.end_array();
    w.key("timeline").begin_array();
    for (const auto& t : r.timeline) w.begin_object().field("t_ms", t.t_ms).field("committed", t.committed).field("aborted", t.aborted).end_object();
    w.end_array();
    w.key("checkpoint_times_ms").begin_array();
    for (double t : r.checkpoint_times_ms) w.value(t);
    w.end_array();
    w.key("txn_samples").begin_array();
    for (const auto& s : r.samples) {
        w.begin_object().field("txn_ref", s.txn_ref).field("attempts", s.attempts).field("latency_us", s.latency_us);
        w.field("reads", s.reads).field("writes", s.writes).field("audit", s.audit).end_object();
    }
    w.end_array().end_object();
    std::cout << w.str() << "\n";
    return r.all_pass ? 0 : 1;
}

int cmd_osdeadlock(const Args& a) {
    std::string mode = get(a, "mode", "safety");
    std::vector<int> avail = int_list(get(a, "avail", "3,3,2"));
    Matrix alloc = matrix_of(get(a, "alloc", "0,1,0;2,0,0;3,0,2;2,1,1;0,0,2"));
    JsonWriter w;
    w.begin_object().field("kind", "simulator").field("mode", mode);
    if (mode == "safety") {
        Matrix mx = matrix_of(get(a, "max", "7,5,3;3,2,2;9,0,2;2,2,2;4,3,3"));
        SafetyResult s = bankers_safety(avail, alloc, mx);
        w.field("safe", s.safe).int_array("safe_sequence", s.safe_sequence);
    } else if (mode == "request") {
        Matrix mx = matrix_of(get(a, "max", "7,5,3;3,2,2;9,0,2;2,2,2;4,3,3"));
        int proc = static_cast<int>(get_int(a, "process", 1, 0, 1000));
        RequestResult r = bankers_request(avail, alloc, mx, proc, int_list(get(a, "request", "1,0,2")));
        w.field("granted", r.granted).field("reason", r.reason);
    } else if (mode == "detect") {
        Matrix rq = matrix_of(get(a, "request-matrix", "0,0,0;2,0,2;0,0,1;1,0,0;0,0,2"));
        DetectionResult d = detect_deadlock(avail, alloc, rq);
        w.field("deadlocked", d.deadlocked).int_array("deadlocked_processes", d.deadlocked_processes);
    } else {
        throw std::invalid_argument("--mode must be safety|request|detect");
    }
    w.end_object();
    std::cout << w.str() << "\n";
    return 0;
}

}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: itx_cli <bench|pipeline|sched|disk|alloc|deadlock|recovery|osdeadlock> [--option value]...\n";
        return 2;
    }
    std::string cmd = argv[1];
    try {
        Args a = parse_args(argc, argv, 2);
        if (cmd == "bench") return cmd_bench(a);
        if (cmd == "sched") return cmd_sched(a);
        if (cmd == "disk") return cmd_disk(a);
        if (cmd == "alloc") return cmd_alloc(a);
        if (cmd == "deadlock") return cmd_deadlock(a);
        if (cmd == "recovery") return cmd_recovery(a);
        if (cmd == "osdeadlock") return cmd_osdeadlock(a);
        if (cmd == "pipeline") return cmd_pipeline(a);
        throw std::invalid_argument("unknown command: " + cmd);
    } catch (const std::exception& e) {
        JsonWriter w;
        w.begin_object().field("error", e.what()).end_object();
        std::cout << w.str() << "\n";
        return 2;
    }
}
