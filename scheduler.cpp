#include "scheduler.h"

#include <algorithm>
#include <cmath>
#include <deque>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <stdexcept>

namespace itx {

namespace {
constexpr double kEps = 1e-9;

void validate(const std::vector<Job>& jobs, double quantum) {
    if (jobs.empty()) throw std::invalid_argument("job list is empty");
    if (quantum <= 0) throw std::invalid_argument("quantum must be > 0");
    std::set<int> ids;
    for (const Job& j : jobs) {
        if (j.arrival < 0) throw std::invalid_argument("arrival time must be >= 0");
        if (j.burst <= 0) throw std::invalid_argument("burst time must be > 0");
        if (!ids.insert(j.id).second) throw std::invalid_argument("duplicate job id");
    }
}

struct Recorder {
    std::map<int, double> first_start, completion;
    std::vector<GanttSegment> gantt;
    void run(int id, double start, double end) {
        if (!first_start.count(id)) first_start[id] = start;
        if (!gantt.empty() && gantt.back().job_id == id && std::fabs(gantt.back().end - start) < kEps)
            gantt.back().end = end;
        else
            gantt.push_back({id, start, end});
    }
    void idle(double start, double end) {
        if (end - start < kEps) return;
        if (!gantt.empty() && gantt.back().job_id == -1) gantt.back().end = end;
        else gantt.push_back({-1, start, end});
    }
};

ScheduleResult finish(const char* name, const std::vector<Job>& jobs, Recorder& rec) {
    ScheduleResult r;
    r.algorithm = name;
    r.gantt = rec.gantt;
    double busy = 0, first_arrival = std::numeric_limits<double>::max();
    std::vector<Job> sorted = jobs;
    std::sort(sorted.begin(), sorted.end(), [](const Job& a, const Job& b) { return a.id < b.id; });
    for (const Job& j : sorted) {
        JobResult jr;
        jr.id = j.id; jr.arrival = j.arrival; jr.burst = j.burst; jr.priority = j.priority;
        jr.first_start = rec.first_start.at(j.id);
        jr.completion = rec.completion.at(j.id);
        jr.turnaround = jr.completion - jr.arrival;
        jr.waiting = jr.turnaround - jr.burst;
        jr.response = jr.first_start - jr.arrival;
        r.avg_waiting += jr.waiting; r.avg_turnaround += jr.turnaround;
        r.avg_response += jr.response; r.avg_completion += jr.completion;
        r.makespan = std::max(r.makespan, jr.completion);
        first_arrival = std::min(first_arrival, j.arrival);
        busy += j.burst;
        r.jobs.push_back(jr);
    }
    double n = static_cast<double>(jobs.size());
    r.avg_waiting /= n; r.avg_turnaround /= n; r.avg_response /= n; r.avg_completion /= n;
    double span = r.makespan - first_arrival;
    r.cpu_utilization = span > 0 ? busy / span : 1.0;
    r.throughput = r.makespan > 0 ? n / r.makespan : 0;
    int switches = 0, last = -2;
    for (const auto& g : r.gantt) {
        if (g.job_id == -1) continue;
        if (last != -2 && g.job_id != last) ++switches;
        last = g.job_id;
    }
    r.context_switches = switches;
    return r;
}

bool before(const Job& a, const Job& b) {  // arrival order, then id
    return a.arrival < b.arrival - kEps || (std::fabs(a.arrival - b.arrival) < kEps && a.id < b.id);
}

// Non-preemptive: `better(a,b)` says whether a should run before b among ready jobs.
template <class Better>
ScheduleResult nonpreemptive(const char* name, const std::vector<Job>& jobs, Better better) {
    std::vector<Job> pending = jobs;
    std::sort(pending.begin(), pending.end(), before);
    Recorder rec;
    double t = 0;
    while (!pending.empty()) {
        auto best = pending.end();
        for (auto it = pending.begin(); it != pending.end(); ++it) {
            if (it->arrival > t + kEps) continue;
            if (best == pending.end() || better(*it, *best)) best = it;
        }
        if (best == pending.end()) {  // nothing ready: idle until next arrival
            double next = pending.front().arrival;
            rec.idle(t, next);
            t = next;
            continue;
        }
        rec.run(best->id, t, t + best->burst);
        t += best->burst;
        rec.completion[best->id] = t;
        pending.erase(best);
    }
    return finish(name, jobs, rec);
}

// Preemptive on arrival: `better(a_remaining,a, b_remaining,b)`.
template <class Better>
ScheduleResult preemptive(const char* name, const std::vector<Job>& jobs, Better better) {
    std::map<int, double> remaining;
    for (const Job& j : jobs) remaining[j.id] = j.burst;
    std::vector<Job> by_id = jobs;
    Recorder rec;
    double t = 0;
    int running = -1;
    size_t done = 0;
    while (done < jobs.size()) {
        const Job* best = nullptr;
        for (const Job& j : by_id) {
            if (remaining[j.id] <= kEps || j.arrival > t + kEps) continue;
            if (!best) { best = &j; continue; }
            bool b = better(remaining[j.id], j, remaining[best->id], *best);
            bool w = better(remaining[best->id], *best, remaining[j.id], j);
            if (b && !w) best = &j;
            else if (!b && !w && j.id == running) best = &j;  // tie: keep the running job
        }
        if (!best) {
            double next = std::numeric_limits<double>::max();
            for (const Job& j : by_id)
                if (remaining[j.id] > kEps) next = std::min(next, j.arrival);
            rec.idle(t, next);
            t = next;
            running = -1;
            continue;
        }
        double finish_at = t + remaining[best->id];
        double next_arrival = std::numeric_limits<double>::max();
        for (const Job& j : by_id)
            if (j.arrival > t + kEps) next_arrival = std::min(next_arrival, j.arrival);
        double until = std::min(finish_at, next_arrival);
        rec.run(best->id, t, until);
        remaining[best->id] -= (until - t);
        t = until;
        running = best->id;
        if (remaining[best->id] <= kEps) {
            remaining[best->id] = 0;
            rec.completion[best->id] = t;
            ++done;
        }
    }
    return finish(name, jobs, rec);
}

ScheduleResult round_robin(const std::vector<Job>& jobs, double quantum) {
    std::vector<Job> arrivals = jobs;
    std::sort(arrivals.begin(), arrivals.end(), before);
    std::map<int, double> remaining;
    for (const Job& j : jobs) remaining[j.id] = j.burst;
    std::deque<int> ready;
    Recorder rec;
    size_t next = 0, done = 0;
    double t = 0;
    auto admit = [&](double upto) {
        while (next < arrivals.size() && arrivals[next].arrival <= upto + kEps)
            ready.push_back(arrivals[next++].id);
    };
    admit(t);
    while (done < jobs.size()) {
        if (ready.empty()) {
            double at = arrivals[next].arrival;
            rec.idle(t, at);
            t = at;
            admit(t);
            continue;
        }
        int id = ready.front();
        ready.pop_front();
        double slice = std::min(quantum, remaining[id]);
        rec.run(id, t, t + slice);
        t += slice;
        remaining[id] -= slice;
        admit(t);  // arrivals during the slice queue BEFORE the preempted job
        if (remaining[id] <= kEps) {
            remaining[id] = 0;
            rec.completion[id] = t;
            ++done;
        } else {
            ready.push_back(id);
        }
    }
    return finish("RR", jobs, rec);
}
}  // namespace

const char* to_string(SchedAlgo a) {
    switch (a) {
        case SchedAlgo::FCFS: return "FCFS";
        case SchedAlgo::SJF: return "SJF";
        case SchedAlgo::SRTF: return "SRTF";
        case SchedAlgo::PRIORITY: return "PRIORITY";
        case SchedAlgo::PRIORITY_PREEMPTIVE: return "PRIORITY_P";
        case SchedAlgo::ROUND_ROBIN: return "RR";
    }
    return "?";
}

bool parse_sched_algo(const std::string& t, SchedAlgo& out) {
    for (SchedAlgo a : all_sched_algos())
        if (t == to_string(a)) { out = a; return true; }
    return false;
}

std::vector<SchedAlgo> all_sched_algos() {
    return {SchedAlgo::FCFS, SchedAlgo::SJF, SchedAlgo::SRTF, SchedAlgo::PRIORITY,
            SchedAlgo::PRIORITY_PREEMPTIVE, SchedAlgo::ROUND_ROBIN};
}

ScheduleResult simulate_schedule(SchedAlgo algo, std::vector<Job> jobs, double quantum) {
    validate(jobs, quantum);
    switch (algo) {
        case SchedAlgo::FCFS:
            return nonpreemptive("FCFS", jobs, [](const Job& a, const Job& b) { return before(a, b); });
        case SchedAlgo::SJF:
            return nonpreemptive("SJF", jobs, [](const Job& a, const Job& b) {
                if (std::fabs(a.burst - b.burst) > kEps) return a.burst < b.burst;
                return before(a, b);
            });
        case SchedAlgo::PRIORITY:
            return nonpreemptive("PRIORITY", jobs, [](const Job& a, const Job& b) {
                if (a.priority != b.priority) return a.priority < b.priority;
                return before(a, b);
            });
        case SchedAlgo::SRTF:
            return preemptive("SRTF", jobs, [](double ra, const Job& a, double rb, const Job& b) {
                if (std::fabs(ra - rb) > kEps) return ra < rb;
                return before(a, b);
            });
        case SchedAlgo::PRIORITY_PREEMPTIVE:
            return preemptive("PRIORITY_P", jobs, [](double, const Job& a, double, const Job& b) {
                if (a.priority != b.priority) return a.priority < b.priority;
                return before(a, b);
            });
        case SchedAlgo::ROUND_ROBIN:
            return round_robin(jobs, quantum);
    }
    throw std::invalid_argument("unknown algorithm");
}

std::vector<Job> make_transaction_jobs(int count, unsigned seed, double arrival_spread) {
    if (count <= 0) throw std::invalid_argument("count must be > 0");
    std::mt19937 rng(seed);
    std::uniform_real_distribution<double> arrive(0.0, std::max(0.0, arrival_spread));
    std::uniform_int_distribution<int> klass(0, 9);
    std::vector<Job> jobs;
    for (int i = 0; i < count; ++i) {
        int c = klass(rng);
        Job j;
        j.id = i + 1;
        j.arrival = std::round(arrive(rng) * 10) / 10.0;
        if (c < 6) { j.priority = 1; j.burst = 1 + (rng() % 4); }          // short: 1-4 ops
        else if (c < 9) { j.priority = 2; j.burst = 5 + (rng() % 8); }     // normal: 5-12 ops
        else { j.priority = 3; j.burst = 20 + (rng() % 21); }              // report: 20-40 ops
        jobs.push_back(j);
    }
    return jobs;
}

}  // namespace itx
