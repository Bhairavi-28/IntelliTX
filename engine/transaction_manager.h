// Transaction manager: begin / read / write / commit / rollback over an in-memory record store
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
    std::string data_dir;     // empty => volatile engine 
    bool sync_commit = true; 
    IsolationLevel isolation = IsolationLevel::SERIALIZABLE;  // default level for begin()
    bool record_history = false;  
};

struct HistoryOp {
    uint64_t seq = 0;
    TxnId txn = 0;
    char op = 'R';
    Key key = 0;
    Value value = 0; 
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
    explicit TransactionManager(const EngineConfig& config); 
    ~TransactionManager();

   
    std::unique_ptr<Transaction> begin(uint64_t reuse_timestamp = 0);
    std::unique_ptr<Transaction> begin_at(IsolationLevel level, uint64_t reuse_timestamp = 0);

    Status read(Transaction& txn, Key key, Value& out);

  
    Status scan_count(Transaction& txn, Value threshold, Value& count_out,
                      std::vector<Key>* matched_out = nullptr);
    Status read_for_update(Transaction& txn, Key key, Value& out);
    Status write(Transaction& txn, Key key, Value value);

    // AVOID policy: declare every key this transaction may lock, right after begin(). Ignored otherwise.
    Status declare(Transaction& txn, const std::vector<Key>& claim);

    Status commit(Transaction& txn);
    void rollback(Transaction& txn, Status reason = Status::ABORT_USER);  // idempotent

    // Sharp checkpoint: waits until no transaction is active, snapshots the store, truncates the

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
    std::vector<HistoryOp> history() const;  

private:
    void finish_txn(Transaction& txn); 
    void log_event(LogType t, const Transaction& txn, Key k = 0, Value o = 0, Value n = 0);
    Value load_and_record(const Transaction& txn, Key key);         
    void store_and_record(const Transaction& txn, Key key, Value v);
    void record(TxnId txn, char op, Key key = 0, Value v = 0);
    bool takes_shared_locks(const Transaction& txn) const {
        return txn.isolation != IsolationLevel::READ_UNCOMMITTED;
    }

    EngineConfig cfg_;
    std::unique_ptr<std::atomic<Value>[]> store_;
    LockManager locks_;
    std::unique_ptr<LogManager> log_;
    RecoveryReport recovery_;

    std::atomic<TxnId> next_txn_id_{1};
    std::atomic<uint64_t> commit_counter_{0};

    std::mutex gate_mu_;  
    std::condition_variable gate_cv_;
    size_t active_ = 0;
    bool checkpointing_ = false;

    mutable std::mutex stats_mu_;
    EngineStats stats_;

  
    mutable std::mutex hist_mu_;
    std::vector<HistoryOp> history_;
};

}  
