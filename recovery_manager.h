// Restart recovery and checkpoint snapshots.
//
// PERSISTENCE MODEL (honest summary)
//   * The record store lives in memory. It is NOT written to disk during normal operation.
//   * Durability comes from (1) the write-ahead log and (2) periodic sharp checkpoints
//     (a full snapshot taken while no transaction is active).
//   * WAL rule: an UPDATE record (with before + after image) is appended BEFORE the in-memory
//     record changes; the COMMIT record is flushed (fsync, if sync_commit) BEFORE the commit is
//     acknowledged and before any lock is released.
//   * Recovery = load newest snapshot, REDO every logged change after the snapshot LSN
//     ("repeat history", including logged compensation records of runtime rollbacks), then UNDO
//     "loser" transactions (no COMMIT and no ABORT record) with their before-images, logging
//     compensation records and an ABORT record so recovery is idempotent.
//   * A torn / corrupt log tail is detected by checksum and truncated.
//   * Crash tests: process-crash (fork + _exit) and synthetic torn-tail. Real power loss and disk
//     write-cache behaviour are NOT tested; they rely on fsync semantics of the platform.
#pragma once
#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "wal.h"

namespace itx {

struct RecoveryReport {
    bool log_found = false;
    bool snapshot_loaded = false;
    uint64_t snapshot_lsn = 0;
    size_t records_scanned = 0;
    size_t redo_applied = 0;
    size_t undo_applied = 0;
    std::vector<TxnId> committed;
    std::vector<TxnId> rolled_back_before_crash;  // had ABORT record
    std::vector<TxnId> losers;                    // undone by recovery
    bool torn_tail = false;
    uint64_t max_lsn = 0;
    TxnId max_txn_id = 0;
};

enum class SnapshotStatus { MISSING, OK, CORRUPT };

class RecoveryManager {
public:
    static std::string log_path(const std::string& dir) { return dir + "/wal.log"; }
    static std::string snapshot_path(const std::string& dir) { return dir + "/snapshot.dat"; }

    // Rebuilds `store` (num_records slots, pre-set to the initial value), opens `log_out` for
    // appending (after truncating any torn tail) and returns what happened.
    // Throws std::runtime_error if a snapshot exists but is corrupt or has the wrong size.
    static RecoveryReport recover(const std::string& dir, std::atomic<Value>* store,
                                  size_t num_records, bool sync_commit,
                                  std::unique_ptr<LogManager>& log_out);

    static bool write_snapshot(const std::string& dir, uint64_t lsn,
                               const std::atomic<Value>* store, size_t num_records);
    static SnapshotStatus load_snapshot(const std::string& dir, uint64_t& lsn,
                                        std::atomic<Value>* store, size_t num_records);
};

}  // namespace itx
