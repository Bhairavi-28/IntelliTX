#include "disk_scheduler.h"

#include <algorithm>
#include <cstdlib>
#include <stdexcept>

namespace itx {

namespace {
struct Walker {
    DiskResult& r;
    int pos;
    void move_to(int target, bool is_jump = false) {
        long d = std::labs(static_cast<long>(target) - pos);
        r.total_movement += d;
        if (is_jump) r.return_jump += d;
        pos = target;
        r.head_path.push_back(pos);
    }
    void serve(int cyl) {
        move_to(cyl);
        r.service_order.push_back(cyl);
    }
};
}  // namespace

const char* to_string(DiskAlgo a) {
    switch (a) {
        case DiskAlgo::FCFS: return "FCFS";
        case DiskAlgo::SSTF: return "SSTF";
        case DiskAlgo::SCAN: return "SCAN";
        case DiskAlgo::CSCAN: return "C-SCAN";
        case DiskAlgo::LOOK: return "LOOK";
        case DiskAlgo::CLOOK: return "C-LOOK";
    }
    return "?";
}

bool parse_disk_algo(const std::string& t, DiskAlgo& out) {
    for (DiskAlgo a : all_disk_algos())
        if (t == to_string(a)) { out = a; return true; }
    return false;
}

std::vector<DiskAlgo> all_disk_algos() {
    return {DiskAlgo::FCFS, DiskAlgo::SSTF, DiskAlgo::SCAN,
            DiskAlgo::CSCAN, DiskAlgo::LOOK, DiskAlgo::CLOOK};
}

DiskResult schedule_disk(DiskAlgo algo, const std::vector<int>& requests, int head,
                         int disk_size, bool up) {
    if (requests.empty()) throw std::invalid_argument("request queue is empty");
    if (disk_size <= 0) throw std::invalid_argument("disk size must be > 0");
    if (head < 0 || head >= disk_size) throw std::invalid_argument("head outside disk");
    for (int q : requests)
        if (q < 0 || q >= disk_size) throw std::invalid_argument("request outside disk");

    DiskResult res;
    res.algorithm = to_string(algo);
    res.head_path.push_back(head);
    Walker w{res, head};

    std::vector<int> sorted = requests;
    std::sort(sorted.begin(), sorted.end());
    std::vector<int> lower, upper;  // requests below / at-or-above the head
    for (int q : sorted) (q < head ? lower : upper).push_back(q);
    std::reverse(lower.begin(), lower.end());  // nearest-first going down

    const int low_end = 0, high_end = disk_size - 1;
    switch (algo) {
        case DiskAlgo::FCFS:
            for (int q : requests) w.serve(q);
            break;
        case DiskAlgo::SSTF: {
            std::vector<int> pending = requests;
            while (!pending.empty()) {
                auto best = pending.begin();
                for (auto it = pending.begin(); it != pending.end(); ++it) {
                    long d = std::labs(*it - w.pos), bd = std::labs(*best - w.pos);
                    if (d < bd || (d == bd && *it < *best)) best = it;
                }
                int q = *best;
                pending.erase(best);
                w.serve(q);
            }
            break;
        }
        case DiskAlgo::SCAN: {
            const std::vector<int>& first = up ? upper : lower;
            const std::vector<int>& second = up ? lower : upper;
            for (int q : first) w.serve(q);
            if (!second.empty()) {
                w.move_to(up ? high_end : low_end);  // travel to the physical end
                for (int q : second) w.serve(q);
            }
            break;
        }
        case DiskAlgo::LOOK: {
            const std::vector<int>& first = up ? upper : lower;
            const std::vector<int>& second = up ? lower : upper;
            for (int q : first) w.serve(q);
            for (int q : second) w.serve(q);
            break;
        }
        case DiskAlgo::CSCAN: {
            // serve in the travel direction, go to the end, jump to the opposite end, continue.
            std::vector<int> ahead = up ? upper : lower;
            std::vector<int> wrapped = up ? lower : upper;
            std::reverse(wrapped.begin(), wrapped.end());  // up: ascending from 0; down: descending from top
            for (int q : ahead) w.serve(q);
            if (!wrapped.empty()) {
                w.move_to(up ? high_end : low_end);
                w.move_to(up ? low_end : high_end, true);
                for (int q : wrapped) w.serve(q);
            }
            break;
        }
        case DiskAlgo::CLOOK: {
            std::vector<int> ahead = up ? upper : lower;
            std::vector<int> wrapped = up ? lower : upper;
            std::reverse(wrapped.begin(), wrapped.end());
            for (int q : ahead) w.serve(q);
            if (!wrapped.empty()) {
                w.move_to(wrapped.front(), true);  // jump straight to the farthest pending request
                w.r.service_order.push_back(wrapped.front());
                for (size_t i = 1; i < wrapped.size(); ++i) w.serve(wrapped[i]);
            }
            break;
        }
    }
    res.avg_movement_per_request =
        static_cast<double>(res.total_movement) / static_cast<double>(requests.size());
    return res;
}

}  // namespace itx
