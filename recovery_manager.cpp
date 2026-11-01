#include "recovery_manager.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <map>
#include <sstream>
#include <stdexcept>

namespace itx {

namespace {
uint32_t fnv1a(const std::string& s) {
    uint32_t h = 2166136261u;
    for (unsigned char c : s) {
        h ^= c;
        h *= 16777619u;
    }
    return h;
}

struct TxnTrace {
    bool begun = false, committed = false, aborted = false;
    std::vector<LogRecord> updates;  // UPDATE records after snapshot, in LSN order
    size_t clr_count = 0;
};
}  // namespace

bool RecoveryManager::write_snapshot(const std::string& dir, uint64_t lsn,
                                     const std::atomic<Value>* store, size_t n) {
    std::ostringstream os;
    os << "ITXSNAP 1 " << lsn << ' ' << n << '\n';
    for (size_t i = 0; i < n; ++i) os << store[i].load() << '\n';
    std::string body = os.str();
    char crc[32];
    std::snprintf(crc, sizeof crc, "CRC %08x\n", fnv1a(body));
    body += crc;

    std::string final_path = snapshot_path(dir), tmp = final_path + ".tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;
    size_t done = 0;
    while (done < body.size()) {
        ssize_t w = ::write(fd, body.data() + done, body.size() - done);
        if (w < 0) { ::close(fd); return false; }
        done += static_cast<size_t>(w);
    }
    bool ok = ::fsync(fd) == 0;
    ::close(fd);
    if (!ok || ::rename(tmp.c_str(), final_path.c_str()) != 0) return false;
    return fsync_directory(dir);
}

SnapshotStatus RecoveryManager::load_snapshot(const std::string& dir, uint64_t& lsn,
                                              std::atomic<Value>* store, size_t n) {
    std::ifstream in(snapshot_path(dir), std::ios::binary);
    if (!in) return SnapshotStatus::MISSING;
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    auto crc_pos = data.rfind("CRC ");
    if (crc_pos == std::string::npos) return SnapshotStatus::CORRUPT;
    std::string body = data.substr(0, crc_pos);
    char expected[32];
    std::snprintf(expected, sizeof expected, "CRC %08x\n", fnv1a(body));
    if (data.substr(crc_pos) != expected) return SnapshotStatus::CORRUPT;
    std::istringstream is(body);
    std::string magic;
    int version = 0;
    size_t count = 0;
    if (!(is >> magic >> version >> lsn >> count) || magic != "ITXSNAP" || version != 1)
        return SnapshotStatus::CORRUPT;
    if (count != n) throw std::runtime_error("snapshot record count differs from configuration");
    for (size_t i = 0; i < n; ++i) {
        Value v;
        if (!(is >> v)) return SnapshotStatus::CORRUPT;
        store[i].store(v);
    }
    return SnapshotStatus::OK;
}

RecoveryReport RecoveryManager::recover(const std::string& dir, std::atomic<Value>* store,
                                        size_t n, bool sync_commit,
                                        std::unique_ptr<LogManager>& log_out) {
    RecoveryReport rep;
    uint64_t snap_lsn = 0;
    SnapshotStatus ss = load_snapshot(dir, snap_lsn, store, n);
    if (ss == SnapshotStatus::CORRUPT) throw std::runtime_error("snapshot is corrupt");
    rep.snapshot_loaded = ss == SnapshotStatus::OK;
    rep.snapshot_lsn = snap_lsn;

    const std::string lpath = log_path(dir);
    LogScan scan = scan_log_file(lpath);
    rep.log_found = scan.file_bytes > 0;
    rep.torn_tail = scan.torn_tail;
    rep.records_scanned = scan.records.size();
    if (scan.torn_tail && ::truncate(lpath.c_str(), static_cast<off_t>(scan.valid_bytes)) != 0)
        throw std::runtime_error("cannot truncate torn log tail");

    // ---- analysis ----
    std::map<TxnId, TxnTrace> txns;
    uint64_t max_lsn = snap_lsn;
    for (const LogRecord& r : scan.records) {
        max_lsn = std::max(max_lsn, r.lsn);
        rep.max_txn_id = std::max(rep.max_txn_id, r.txn);
        if (r.lsn <= snap_lsn || r.type == LogType::CHECKPOINT) continue;
        TxnTrace& t = txns[r.txn];
        switch (r.type) {
            case LogType::BEGIN: t.begun = true; break;
            case LogType::UPDATE: t.updates.push_back(r); break;
            case LogType::CLR: ++t.clr_count; break;
            case LogType::COMMIT: t.committed = true; break;
            case LogType::ABORT: t.aborted = true; break;
            default: break;
        }
    }
    rep.max_lsn = max_lsn;

    log_out = std::make_unique<LogManager>(lpath, sync_commit, max_lsn + 1);
    if (!log_out->ok()) throw std::runtime_error("cannot open write-ahead log");

    // ---- redo: repeat history (UPDATE and CLR records after the snapshot) ----
    for (const LogRecord& r : scan.records) {
        if (r.lsn <= snap_lsn) continue;
        if ((r.type == LogType::UPDATE || r.type == LogType::CLR) && r.key < n) {
            store[r.key].store(r.new_value);
            ++rep.redo_applied;
        }
    }

    // ---- undo losers ----
    for (auto& [id, t] : txns) {
        if (t.committed) rep.committed.push_back(id);
        else if (t.aborted) rep.rolled_back_before_crash.push_back(id);
        else if (t.begun || !t.updates.empty()) {
            rep.losers.push_back(id);
            // CLRs already logged compensated the LAST clr_count updates (undo runs in reverse).
            size_t remaining = t.updates.size() >= t.clr_count ? t.updates.size() - t.clr_count : 0;
            for (size_t i = remaining; i-- > 0;) {
                const LogRecord& u = t.updates[i];
                if (u.key >= n) continue;
                Value current = store[u.key].load();
                log_out->append(LogType::CLR, id, u.key, current, u.old_value);
                store[u.key].store(u.old_value);
                ++rep.undo_applied;
            }
            log_out->append(LogType::ABORT, id);
        }
    }
    if (!rep.losers.empty()) log_out->flush();
    return rep;
}

}  // namespace itx
