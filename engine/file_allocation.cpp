#include "file_allocation.h"

#include <algorithm>
#include <map>
#include <stdexcept>

namespace itx {

const char* to_string(AllocMethod m) {
    switch (m) {
        case AllocMethod::CONTIGUOUS_FIRST_FIT: return "contiguous_first_fit";
        case AllocMethod::CONTIGUOUS_BEST_FIT: return "contiguous_best_fit";
        case AllocMethod::LINKED: return "linked";
        case AllocMethod::INDEXED: return "indexed";
    }
    return "?";
}

bool parse_alloc_method(const std::string& t, AllocMethod& out) {
    for (AllocMethod m : {AllocMethod::CONTIGUOUS_FIRST_FIT, AllocMethod::CONTIGUOUS_BEST_FIT,
                          AllocMethod::LINKED, AllocMethod::INDEXED})
        if (t == to_string(m)) { out = m; return true; }
    return false;
}

namespace {
std::vector<FreeExtent> extents_of(const std::vector<bool>& used) {
    std::vector<FreeExtent> out;
    int n = static_cast<int>(used.size());
    for (int i = 0; i < n;) {
        if (used[i]) { ++i; continue; }
        int j = i;
        while (j < n && !used[j]) ++j;
        out.push_back({i, j - i});
        i = j;
    }
    return out;
}
}  // namespace

AllocResult simulate_allocation(AllocMethod method, int disk_blocks,
                                const std::vector<FileOp>& ops, int index_capacity) {
    if (disk_blocks <= 0) throw std::invalid_argument("disk_blocks must be > 0");
    if (index_capacity <= 0) throw std::invalid_argument("index_capacity must be > 0");
    std::vector<bool> used(disk_blocks, false);
    std::vector<std::string> owner(disk_blocks);
    struct FileInfo { std::vector<int> data; int index_block = -1; };
    std::map<std::string, FileInfo> files;
    AllocResult res;
    res.method = to_string(method);
    res.disk_blocks = disk_blocks;

    auto claim = [&](const std::vector<int>& blocks, const std::string& label) {
        for (int b : blocks) { used[b] = true; owner[b] = label; }
    };

    for (const FileOp& op : ops) {
        OpOutcome out;
        out.op = op;
        if (op.name.empty()) throw std::invalid_argument("file name must not be empty");
        if (op.kind == FileOp::REMOVE) {
            auto it = files.find(op.name);
            if (it == files.end()) {
                out.message = "no such file";
            } else {
                for (int b : it->second.data) { used[b] = false; owner[b].clear(); }
                if (it->second.index_block >= 0) {
                    used[it->second.index_block] = false;
                    owner[it->second.index_block].clear();
                }
                files.erase(it);
                out.success = true;
                out.message = "removed; blocks returned to the free bitmap";
            }
            res.outcomes.push_back(out);
            continue;
        }
        if (op.blocks <= 0) throw std::invalid_argument("file size must be > 0 blocks");
        if (files.count(op.name)) throw std::invalid_argument("duplicate file name: " + op.name);

        std::vector<FreeExtent> ext = extents_of(used);
        int free_total = 0;
        for (auto& e : ext) free_total += e.length;
        FileInfo info;
        bool ok = false;
        switch (method) {
            case AllocMethod::CONTIGUOUS_FIRST_FIT:
            case AllocMethod::CONTIGUOUS_BEST_FIT: {
                const FreeExtent* pick = nullptr;
                for (const auto& e : ext) {
                    if (e.length < op.blocks) continue;
                    if (!pick) pick = &e;
                    else if (method == AllocMethod::CONTIGUOUS_BEST_FIT && e.length < pick->length) pick = &e;
                    if (method == AllocMethod::CONTIGUOUS_FIRST_FIT) break;
                }
                if (pick) {
                    for (int i = 0; i < op.blocks; ++i) info.data.push_back(pick->start + i);
                    ok = true;
                } else {
                    out.message = free_total >= op.blocks
                                      ? "FAILED: enough free blocks exist but no contiguous extent "
                                        "(external fragmentation)"
                                      : "FAILED: not enough free blocks";
                }
                break;
            }
            case AllocMethod::LINKED: {
                if (free_total < op.blocks) { out.message = "FAILED: not enough free blocks"; break; }
                for (const auto& e : ext)
                    for (int i = 0; i < e.length && static_cast<int>(info.data.size()) < op.blocks; ++i)
                        info.data.push_back(e.start + i);
                ok = true;
                break;
            }
            case AllocMethod::INDEXED: {
                if (op.blocks > index_capacity) {
                    out.message = "FAILED: file exceeds single-level index capacity";
                    break;
                }
                if (free_total < op.blocks + 1) { out.message = "FAILED: not enough free blocks"; break; }
                std::vector<int> picked;
                for (const auto& e : ext)
                    for (int i = 0; i < e.length && static_cast<int>(picked.size()) < op.blocks + 1; ++i)
                        picked.push_back(e.start + i);
                info.index_block = picked.front();
                info.data.assign(picked.begin() + 1, picked.end());
                ok = true;
                break;
            }
        }
        if (ok) {
            claim(info.data, op.name);
            if (info.index_block >= 0) claim({info.index_block}, op.name + "*");
            if (info.index_block >= 0) out.blocks.push_back(info.index_block);
            out.blocks.insert(out.blocks.end(), info.data.begin(), info.data.end());
            out.success = true;
            out.message = "allocated";
            files[op.name] = info;
        } else {
            ++res.failed_creates;
        }
        res.outcomes.push_back(out);
    }

    res.block_owner = owner;
    res.free_extents = extents_of(used);
    int largest = 0;
    for (bool u : used) res.used_blocks += u ? 1 : 0;
    res.free_blocks = disk_blocks - res.used_blocks;
    for (const auto& e : res.free_extents) largest = std::max(largest, e.length);
    res.external_fragmentation_pct =
        res.free_blocks > 0 ? 100.0 * (1.0 - static_cast<double>(largest) / res.free_blocks) : 0.0;
    double reads = 0;
    for (const auto& [name, info] : files) {
        res.overhead_blocks += info.index_block >= 0 ? 1 : 0;
        int n = static_cast<int>(info.data.size());
        switch (method) {
            case AllocMethod::CONTIGUOUS_FIRST_FIT:
            case AllocMethod::CONTIGUOUS_BEST_FIT: reads += 1; break;  // compute address directly
            case AllocMethod::LINKED: reads += n; break;               // follow n pointers
            case AllocMethod::INDEXED: reads += 2; break;              // index block + data block
        }
    }
    res.avg_blocks_read_to_reach_last_block = files.empty() ? 0 : reads / files.size();
    return res;
}

}  // namespace itx
