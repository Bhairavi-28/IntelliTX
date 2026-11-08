// Disk-arm scheduling SIMULATOR (FCFS, SSTF, SCAN, C-SCAN, LOOK, C-LOOK).
// Conventions: cylinders are 0..disk_size-1. SCAN/C-SCAN travel to the physical end of the disk;
// LOOK/C-LOOK turn at the last request. Circular variants COUNT the return jump in the total
// head movement (reported separately as `return_jump`). SCAN/C-SCAN stop once no request is
// pending (they do not travel to a boundary that serves nobody).
#pragma once
#include <string>
#include <vector>

namespace itx {

enum class DiskAlgo { FCFS, SSTF, SCAN, CSCAN, LOOK, CLOOK };

struct DiskResult {
    std::string algorithm;
    std::vector<int> service_order;
    std::vector<int> head_path;  // every cylinder the head visits, starting at the initial head
    long total_movement = 0;
    long return_jump = 0;  // part of total_movement spent on a C-SCAN/C-LOOK return sweep
    double avg_movement_per_request = 0;
};

const char* to_string(DiskAlgo a);
bool parse_disk_algo(const std::string& text, DiskAlgo& out);
std::vector<DiskAlgo> all_disk_algos();

// Throws std::invalid_argument for an empty queue, head or request outside [0, disk_size-1].
DiskResult schedule_disk(DiskAlgo algo, const std::vector<int>& requests, int head,
                         int disk_size, bool moving_up = true);

}  // namespace itx
