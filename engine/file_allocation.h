// File-allocation SIMULATOR on a block device with a free-space BITMAP (one bit per block).
// Methods: contiguous (first-fit or best-fit), linked, single-level indexed.
// Metrics are model outputs (block counts / access counts), not measurements of a real file system.
#pragma once
#include <string>
#include <vector>

namespace itx {

enum class AllocMethod { CONTIGUOUS_FIRST_FIT, CONTIGUOUS_BEST_FIT, LINKED, INDEXED };

struct FileOp {
    enum Kind { CREATE, REMOVE } kind = CREATE;
    std::string name;
    int blocks = 0;  // CREATE only
};

struct OpOutcome {
    FileOp op;
    bool success = false;
    std::string message;
    std::vector<int> blocks;  // data blocks (+ index block for INDEXED, listed first)
};

struct FreeExtent {
    int start, length;
};

struct AllocResult {
    std::string method;
    int disk_blocks = 0;
    std::vector<OpOutcome> outcomes;
    std::vector<std::string> block_owner;  // "" = free; otherwise file name (index blocks "name*")
    std::vector<FreeExtent> free_extents;  // free-space list derived from the bitmap
    int used_blocks = 0, free_blocks = 0, overhead_blocks = 0;
    int failed_creates = 0;
    double external_fragmentation_pct = 0;  // 1 - largest_free_extent / total_free
    double avg_blocks_read_to_reach_last_block = 0;  // over existing files
};

const char* to_string(AllocMethod m);
bool parse_alloc_method(const std::string& text, AllocMethod& out);

// Throws std::invalid_argument for bad sizes, duplicate create, or removing a missing file is
// reported as a failed outcome (not an exception).
AllocResult simulate_allocation(AllocMethod method, int disk_blocks,
                                const std::vector<FileOp>& ops, int index_capacity = 128);

}  // namespace itx
