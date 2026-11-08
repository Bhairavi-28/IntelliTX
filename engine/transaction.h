// A transaction handle. Created and driven by TransactionManager (never construct directly).
//
// Transaction model:
//   * the database is a fixed array of records addressed by key, each holding one int64 value;
//   * operations are point reads and point writes (no inserts/deletes/range scans, hence no
//     phantoms);
//   * `timestamp` is the transaction's age for Wait-Die. It is assigned at first begin and is
//     REUSED when an aborted transaction is restarted, so a restarted transaction keeps getting
//     older relative to newcomers and cannot starve under Wait-Die.
#pragma once
#include <chrono>
#include <vector>

#include "types.h"

namespace itx {

struct UndoEntry {
    Key key;
    Value old_value;
};

struct Transaction {
    TxnId id = 0;
    uint64_t timestamp = 0;
    TxnState state = TxnState::ACTIVE;
    Status abort_reason = Status::OK;
    std::vector<UndoEntry> undo_log;  // in-memory undo log (before-images), applied in reverse
    uint64_t reads = 0;
    uint64_t writes = 0;
    uint64_t commit_sequence = 0;  // global commit order, assigned while locks are still held
    std::chrono::steady_clock::time_point started_at;
};

}  // namespace itx
