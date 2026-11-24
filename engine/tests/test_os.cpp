#include <cmath>

#include "disk_scheduler.h"
#include "file_allocation.h"
#include "scheduler.h"
#include "test_framework.h"

using namespace itx;

static bool near(double a, double b) { return std::fabs(a - b) < 1e-6; }

// Textbook set: P1(0,24) P2(0,3) P3(0,3), all arrive at 0.
static std::vector<Job> book() { return {{1, 0, 24, 1}, {2, 0, 3, 2}, {3, 0, 3, 3}}; }

TEST(sched_fcfs_known_case) {
    auto r = simulate_schedule(SchedAlgo::FCFS, book());
    CHECK(near(r.jobs[0].waiting, 0) && near(r.jobs[1].waiting, 24) && near(r.jobs[2].waiting, 27));
    CHECK(near(r.avg_waiting, 17));
    CHECK(near(r.avg_turnaround, 27));
    CHECK(near(r.jobs[2].completion, 30));
}

TEST(sched_sjf_known_case) {
    auto r = simulate_schedule(SchedAlgo::SJF, book());
    CHECK(near(r.avg_waiting, 3));  // order P2,P3,P1 => waits 0,3,6
    CHECK(near(r.jobs[0].completion, 30));
}

TEST(sched_sjf_arrivals_and_srtf) {
    std::vector<Job> j = {{1, 0, 7, 0}, {2, 2, 4, 0}, {3, 4, 1, 0}, {4, 5, 4, 0}};
    auto sjf = simulate_schedule(SchedAlgo::SJF, j);
    CHECK(near(sjf.avg_waiting, 4));      // classic: 0,6,3,7
    auto srtf = simulate_schedule(SchedAlgo::SRTF, j);
    CHECK(near(srtf.avg_waiting, 3));     // classic: 9,1,0,2
    CHECK(near(srtf.jobs[0].waiting, 9) && near(srtf.jobs[1].waiting, 1));
    CHECK(near(srtf.jobs[2].waiting, 0) && near(srtf.jobs[3].waiting, 2));
}

TEST(sched_priority_known_case) {
    std::vector<Job> j = {{1, 0, 10, 3}, {2, 0, 1, 1}, {3, 0, 2, 4}, {4, 0, 1, 5}, {5, 0, 5, 2}};
    auto r = simulate_schedule(SchedAlgo::PRIORITY, j);  // order P2,P5,P1,P3,P4
    CHECK(near(r.avg_waiting, 8.2));
    CHECK(near(r.jobs[3].completion, 19));
}

TEST(sched_priority_preemptive_preempts) {
    std::vector<Job> j = {{1, 0, 10, 3}, {2, 2, 2, 1}};
    auto r = simulate_schedule(SchedAlgo::PRIORITY_PREEMPTIVE, j);
    CHECK(near(r.jobs[1].completion, 4));   // high-priority job runs immediately on arrival
    CHECK(near(r.jobs[0].completion, 12));
    CHECK(near(r.jobs[1].response, 0));
}

TEST(sched_round_robin_known_case) {
    auto r = simulate_schedule(SchedAlgo::ROUND_ROBIN, book(), 4);
    CHECK(near(r.jobs[0].waiting, 6));      // P1 waits 6
    CHECK(near(r.jobs[1].waiting, 4));
    CHECK(near(r.jobs[2].waiting, 7));
    CHECK(near(r.avg_waiting, 17.0 / 3));
    CHECK(near(r.jobs[1].response, 4));
    CHECK(near(r.jobs[0].completion, 30));
}

TEST(sched_idle_gaps_and_metric_identities) {
    std::vector<Job> j = {{1, 0, 2, 1}, {2, 10, 3, 1}};
    for (SchedAlgo a : all_sched_algos()) {
        auto r = simulate_schedule(a, j, 2);
        CHECK(near(r.jobs[1].first_start, 10));
        CHECK(near(r.makespan, 13));
        for (auto& jr : r.jobs) {  // turnaround = completion - arrival; waiting = turnaround - burst
            CHECK(near(jr.turnaround, jr.completion - jr.arrival));
            CHECK(near(jr.waiting, jr.turnaround - jr.burst));
            CHECK(jr.waiting >= -1e-9);
        }
    }
}

TEST(sched_same_input_every_algorithm_total_busy_time_equal) {
    auto jobs = make_transaction_jobs(40, 5, 30);
    CHECK_EQ(jobs.size(), 40u);
    double total = 0;
    for (auto& j : jobs) total += j.burst;
    for (SchedAlgo a : all_sched_algos()) {
        auto r = simulate_schedule(a, jobs, 4);
        double ran = 0;
        for (auto& g : r.gantt) if (g.job_id != -1) ran += g.end - g.start;
        CHECK(near(ran, total));  // every algorithm executes exactly the same work
    }
    auto again = make_transaction_jobs(40, 5, 30);
    CHECK(again[7].burst == jobs[7].burst && again[7].arrival == jobs[7].arrival);  // deterministic
}

TEST(sched_input_validation) {
    CHECK_THROWS(simulate_schedule(SchedAlgo::FCFS, {}));
    CHECK_THROWS(simulate_schedule(SchedAlgo::FCFS, {{1, 0, 0, 1}}));
    CHECK_THROWS(simulate_schedule(SchedAlgo::FCFS, {{1, -1, 2, 1}}));
    CHECK_THROWS(simulate_schedule(SchedAlgo::FCFS, {{1, 0, 2, 1}, {1, 0, 3, 1}}));
    CHECK_THROWS(simulate_schedule(SchedAlgo::ROUND_ROBIN, book(), 0));
}

// ---------------- disk scheduling: classic textbook queue ----------------
static const std::vector<int> kQ = {98, 183, 37, 122, 14, 124, 65, 67};

TEST(disk_fcfs_sstf) {
    CHECK_EQ(schedule_disk(DiskAlgo::FCFS, kQ, 53, 200).total_movement, 640);
    auto s = schedule_disk(DiskAlgo::SSTF, kQ, 53, 200);
    CHECK_EQ(s.total_movement, 236);
    std::vector<int> order = {65, 67, 37, 14, 98, 122, 124, 183};
    CHECK(s.service_order == order);
}

TEST(disk_scan_cscan_look_clook) {
    CHECK_EQ(schedule_disk(DiskAlgo::SCAN, kQ, 53, 200, true).total_movement, 331);   // to 199, back to 14
    CHECK_EQ(schedule_disk(DiskAlgo::SCAN, kQ, 53, 200, false).total_movement, 236);  // down to 0, then up to 183
    CHECK_EQ(schedule_disk(DiskAlgo::LOOK, kQ, 53, 200, true).total_movement, 299);
    auto c = schedule_disk(DiskAlgo::CSCAN, kQ, 53, 200, true);
    CHECK_EQ(c.total_movement, 382);   // 146 up (to 199) + 199 jump + 37 (counting the return jump)
    CHECK_EQ(c.return_jump, 199);
    auto cl = schedule_disk(DiskAlgo::CLOOK, kQ, 53, 200, true);
    CHECK_EQ(cl.total_movement, 322);  // 130 up + 169 jump + 23
    CHECK_EQ(cl.return_jump, 169);
    std::vector<int> order = {65, 67, 98, 122, 124, 183, 14, 37};
    CHECK(cl.service_order == order);
}

TEST(disk_every_algorithm_serves_each_request_once) {
    for (DiskAlgo a : all_disk_algos()) {
        auto r = schedule_disk(a, kQ, 53, 200);
        auto got = r.service_order;
        auto want = kQ;
        std::sort(got.begin(), got.end());
        std::sort(want.begin(), want.end());
        CHECK(got == want);
        long sum = 0;
        for (size_t i = 1; i < r.head_path.size(); ++i) sum += std::labs(r.head_path[i] - r.head_path[i - 1]);
        CHECK_EQ(sum, r.total_movement);  // head path is consistent with the reported total
    }
}

TEST(disk_input_validation) {
    CHECK_THROWS(schedule_disk(DiskAlgo::FCFS, {}, 0, 10));
    CHECK_THROWS(schedule_disk(DiskAlgo::FCFS, {1}, 10, 10));
    CHECK_THROWS(schedule_disk(DiskAlgo::FCFS, {10}, 0, 10));
}

// ---------------- file allocation ----------------
TEST(alloc_contiguous_external_fragmentation) {
    std::vector<FileOp> ops = {{FileOp::CREATE, "A", 4}, {FileOp::CREATE, "B", 2}, {FileOp::CREATE, "C", 4},
                               {FileOp::REMOVE, "B", 0}, {FileOp::CREATE, "D", 3}};
    auto r = simulate_allocation(AllocMethod::CONTIGUOUS_FIRST_FIT, 12, ops);
    CHECK(!r.outcomes[4].success);                       // 4 blocks free in total, hole of 2 + tail of 2
    CHECK(r.outcomes[4].message.find("fragmentation") != std::string::npos);
    CHECK_EQ(r.failed_creates, 1);
    CHECK(r.external_fragmentation_pct > 0);
    auto linked = simulate_allocation(AllocMethod::LINKED, 12, ops);
    CHECK(linked.outcomes[4].success);                   // linked allocation has no external fragmentation
    CHECK_EQ(linked.free_blocks, 1);
    CHECK(near(linked.external_fragmentation_pct, 0));
}

TEST(alloc_best_fit_vs_first_fit) {
    // holes after removals: [0..4) size 4, [6..8) size 2 ; request 2 blocks
    std::vector<FileOp> ops = {{FileOp::CREATE, "A", 4}, {FileOp::CREATE, "X", 2}, {FileOp::CREATE, "B", 2},
                               {FileOp::CREATE, "Y", 2}, {FileOp::REMOVE, "A", 0}, {FileOp::REMOVE, "B", 0},
                               {FileOp::CREATE, "N", 2}};
    auto ff = simulate_allocation(AllocMethod::CONTIGUOUS_FIRST_FIT, 12, ops);
    auto bf = simulate_allocation(AllocMethod::CONTIGUOUS_BEST_FIT, 12, ops);
    CHECK_EQ(ff.outcomes.back().blocks.front(), 0);   // first hole that fits
    CHECK_EQ(bf.outcomes.back().blocks.front(), 6);   // smallest hole that fits
}

TEST(alloc_indexed_uses_index_block_and_capacity_limit) {
    auto r = simulate_allocation(AllocMethod::INDEXED, 20, {{FileOp::CREATE, "A", 5}, {FileOp::CREATE, "B", 3}}, 4);
    CHECK(!r.outcomes[0].success);                       // 5 blocks > capacity 4
    CHECK(r.outcomes[1].success);
    CHECK_EQ(r.outcomes[1].blocks.size(), 4u);           // 1 index + 3 data
    CHECK_EQ(r.overhead_blocks, 1);
    CHECK_EQ(r.used_blocks, 4);
}

TEST(alloc_remove_frees_and_bad_input) {
    auto r = simulate_allocation(AllocMethod::LINKED, 8, {{FileOp::CREATE, "A", 3}, {FileOp::REMOVE, "A", 0}, {FileOp::REMOVE, "A", 0}});
    CHECK_EQ(r.free_blocks, 8);
    CHECK(!r.outcomes[2].success);
    CHECK_THROWS(simulate_allocation(AllocMethod::LINKED, 8, {{FileOp::CREATE, "A", 0}}));
    CHECK_THROWS(simulate_allocation(AllocMethod::LINKED, 8, {{FileOp::CREATE, "A", 1}, {FileOp::CREATE, "A", 1}}));
    CHECK_THROWS(simulate_allocation(AllocMethod::LINKED, 0, {}));
}
