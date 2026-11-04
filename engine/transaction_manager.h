// Transaction manager: begin / read / write / commit / rollback over an in-memory record store
// with Strict 2PL locking, undo logging, write-ahead logging, checkpoints and restart recovery.
//
// Isolation actually provided: with a locking policy other than NONE every transaction holds
// shared locks on what it reads and exclusive locks on what it writes until commit/abort (Strict
// 2PL), so execution is conflict-serializable (this is the strongest SQL level, SERIALIZABLE, for
// point operations; there are no range scans so there are no phantoms). READ UNCOMMITTED, READ
// COMMITTED and REPEATABLE READ are NOT implemented as separate modes. Policy NONE is an unsafe
// baseline that exists only to demonstrate anomalies.
#pragma once
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

#include "lock_manager.h"
#include "recovery_manager.h"
#include "transaction.h"
#include "wal.h"

namespace itx {

struct EngineConfig {
    uint32_t num_records = 1000;
    Value initial_value = 0;
    LockPolicy policy = LockPolicy::WAIT_DIE;
    std::string data_dir;     // empty => volatile engine (no WAL, no recovery)
    bool sync_commit = true;  // fsync the log at commit
};

struct EngineStats {
    uint64_t begun = 0;
    uint64_t committed = 0;
    uint64_t aborted = 0;  // every rollback, whatever the reason
    uint64_t aborted_wait_die = 0;
    uint64_t aborted_no_wait = 0;
    uint64_t aborted_deadlock = 0;
    uint64_t aborted_user = 0;
    uint64_t checkpoints = 0;
};

class TransactionManager {
public:
    explicit TransactionManager(const EngineConfig& config);  // runs recovery if data_dir is set
    ~TransactionManager();

    // `reuse_timestamp` = 0 for a new transaction; pass the old timestamp when restarting one.
    std::unique_ptr<Transaction> begin(uint64_t reuse_timestamp = 0);

    // read takes a SHARED lock; read_for_update / write take an EXCLUSIVE lock.
    // On any ABORT_* status the transaction has ALREADY been rolled back (no lock leaks).
    Status read(Transaction& txn, Key key, Value& out);
    Status read_for_update(Transaction& txn, Key key, Value& out);
    Status write(Transaction& txn, Key key, Value value);

    // AVOID policy: declare every key this transaction may lock, right after begin(). Ignored otherwise.
    Status declare(Transaction& txn, const std::vector<Key>& claim);

    Status commit(Transaction& txn);
    void rollback(Transaction& txn, Status reason = Status::ABORT_USER);  // idempotent

    // Sharp checkpoint: waits until no transaction is active, snapshots the store, truncates the
    // log. Must not be called by a thread that still has an active transaction.
    Status checkpoint();

    // Non-transactional inspection. Only meaningful when no transaction is running.
    Value peek(Key key) const;
    Value sum_all() const;

    EngineStats stats() const;
    LockManager& locks() { return locks_; }
    const EngineConfig& config() const { return cfg_; }
    const RecoveryReport& recovery_report() const { return recovery_; }
    bool durable() const { return log_ != nullptr; }
    uint64_t log_bytes() const { return log_ ? log_->bytes_written() : 0; }

private:
    void finish_txn(Transaction& txn);  // leave the checkpoint gate
    void log_event(LogType t, const Transaction& txn, Key k = 0, Value o = 0, Value n = 0);

    EngineConfig cfg_;
    std::unique_ptr<std::atomic<Value>[]> store_;
    LockManager locks_;
    std::unique_ptr<LogManager> log_;
    RecoveryReport recovery_;

    std::atomic<TxnId> next_txn_id_{1};
    std::atomic<uint64_t> commit_counter_{0};

    std::mutex gate_mu_;  // checkpoint gate
    std::condition_variable gate_cv_;
    size_t active_ = 0;
    bool checkpointing_ = false;

    mutable std::mutex stats_mu_;
    EngineStats stats_;
};

}  // namespace itx
