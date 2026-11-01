// CPU-scheduling SIMULATOR (discrete-event, deterministic).
// This is a model of scheduling POLICIES for the OS syllabus. It does NOT replace or influence the
// Linux scheduler; the real C++ threads of the transaction engine are scheduled by the OS.
#pragma once
#include <string>
#include <vector>

namespace itx {

struct Job {
    int id = 0;
    double arrival = 0;
    double burst = 0;
    int priority = 0;  // lower number = higher priority
};

struct JobResult {
    int id = 0;
    double arrival = 0, burst = 0;
    int priority = 0;
    double first_start = 0, completion = 0, turnaround = 0, waiting = 0, response = 0;
};

struct GanttSegment {
    int job_id;  // -1 = CPU idle
    double start, end;
};

struct ScheduleResult {
    std::string algorithm;
    std::vector<JobResult> jobs;  // ordered by job id
    std::vector<GanttSegment> gantt;
    double avg_waiting = 0, avg_turnaround = 0, avg_response = 0, avg_completion = 0;
    double makespan = 0, cpu_utilization = 0, throughput = 0;  // jobs per time unit
    int context_switches = 0;
};

enum class SchedAlgo { FCFS, SJF, SRTF, PRIORITY, PRIORITY_PREEMPTIVE, ROUND_ROBIN };

const char* to_string(SchedAlgo a);
bool parse_sched_algo(const std::string& text, SchedAlgo& out);
std::vector<SchedAlgo> all_sched_algos();

// Throws std::invalid_argument for empty/invalid input (negative times, non-positive burst or
// quantum, duplicate ids).
ScheduleResult simulate_schedule(SchedAlgo algo, std::vector<Job> jobs, double quantum = 4.0);

// Synthetic transaction-job workload: `count` transactions of three classes. burst is
// proportional to the number of record operations; priority reflects the class
// (1 = short interactive, 2 = normal, 3 = long report). Deterministic for a given seed.
std::vector<Job> make_transaction_jobs(int count, unsigned seed, double arrival_spread);

}  // namespace itx
