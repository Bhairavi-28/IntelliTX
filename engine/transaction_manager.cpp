#include "transaction_manager.h"

#include <sys/stat.h>

#include <stdexcept>

namespace itx {

TransactionManager::TransactionManager(const EngineConfig& config)
    : cfg_(config), locks_(config.policy) {
    if (cfg_.num_records == 0) throw std::invalid_argument("num_records must be > 0");
    store_.reset(new std::atomic<Value>[cfg_.num_records]);
    for (uint32_t i = 0; i < cfg_.num_records; ++i) store_[i].store(cfg_.initial_value);

    if (!cfg_.data_dir.empty()) {
        ::mkdir(cfg_.data_dir.c_str(), 0755);
        recovery_ = RecoveryManager::recover(cfg_.data_dir, store_.get(), cfg_.num_records,
                                             cfg_.sync_commit, log_);
        next_txn_id_.store(recovery_.max_txn_id + 1);
    }
}

TransactionManager::~TransactionManager() = default;

void TransactionManager::log_event(LogType t, const Transaction& txn, Key k, Value o, Value n) {
    if (log_) log_->append(t, txn.id, k, o, n);
}

std::unique_ptr<Transaction> TransactionManager::begin(uint64_t reuse_timestamp) {
    return begin_at(cfg_.isolation, reuse_timestamp);
}

std::unique_ptr<Transaction> TransactionManager::begin_at(IsolationLevel level, uint64_t reuse_timestamp) {
    {
        std::unique_lock<std::mutex> lk(gate_mu_);
        gate_cv_.wait(lk, [this] { return !checkpointing_; });
        ++active_;
    }
    auto txn = std::make_unique<Transaction>();
    txn->id = next_txn_id_.fetch_add(1);
    txn->timestamp = reuse_timestamp != 0 ? reuse_timestamp : txn->id;
    txn->isolation = level;
    txn->started_at = std::chrono::steady_clock::now();
    log_event(LogType::BEGIN, *txn);
    {
        std::lock_guard<std::mutex> g(stats_mu_);
        ++stats_.begun;
    }
    return txn;
}

void TransactionManager::record(TxnId txn, char op, Key key, Value v) {
    
    HistoryOp h;
    h.seq = history_.size() + 1;
    h.txn = txn;
    h.op = op;
    h.key = key;
    h.value = v;
    history_.push_back(h);
}

Value TransactionManager::load_and_record(const Transaction& txn, Key key) {
    if (!cfg_.record_history) return store_[key].load();
    std::lock_guard<std::mutex> g(hist_mu_);
    Value v = store_[key].load();
    record(txn.id, 'R', key, v);
    return v;
}

void TransactionManager::store_and_record(const Transaction& txn, Key key, Value v) {
    if (!cfg_.record_history) { store_[key].store(v); return; }
    std::lock_guard<std::mutex> g(hist_mu_);
    store_[key].store(v);
    record(txn.id, 'W', key, v);
}

std::vector<HistoryOp> TransactionManager::history() const {
    std::lock_guard<std::mutex> g(hist_mu_);
    return history_;
}

void TransactionManager::finish_txn(Transaction&) {
    std::lock_guard<std::mutex> lk(gate_mu_);
    if (active_ > 0) --active_;
    gate_cv_.notify_all();
}

Status TransactionManager::read(Transaction& txn, Key key, Value& out) {
    if (txn.state != TxnState::ACTIVE) return Status::ERR_BAD_STATE;
    if (key >= cfg_.num_records) return Status::ERR_BAD_KEY;
    if (takes_shared_locks(txn)) {
        Status s = locks_.acquire(txn.id, txn.timestamp, key, LockMode::SHARED);
        if (s != Status::OK) { rollback(txn, s); return s; }
    }
    out = load_and_record(txn, key);
    if (txn.isolation == IsolationLevel::READ_COMMITTED) locks_.release_shared(txn.id, key);
    ++txn.reads;
    return Status::OK;
}

Status TransactionManager::scan_count(Transaction& txn, Value threshold, Value& count_out,
                                      std::vector<Key>* matched_out) {
    if (txn.state != TxnState::ACTIVE) return Status::ERR_BAD_STATE;
    if (matched_out) matched_out->clear();
    count_out = 0;
    const bool lock_all = txn.isolation == IsolationLevel::SERIALIZABLE;
    if (lock_all) {  
        for (Key k = 0; k < cfg_.num_records; ++k) {
            Status s = locks_.acquire(txn.id, txn.timestamp, k, LockMode::SHARED);
            if (s != Status::OK) { rollback(txn, s); return s; }
        }
    }
    for (Key k = 0; k < cfg_.num_records; ++k) {
        Value v = 0;
        if (txn.isolation == IsolationLevel::REPEATABLE_READ) {
            if (store_[k].load() >= threshold) {
                Status s = locks_.acquire(txn.id, txn.timestamp, k, LockMode::SHARED);
                if (s != Status::OK) { rollback(txn, s); return s; }
            }
            v = load_and_record(txn, k);
        } else if (txn.isolation == IsolationLevel::READ_COMMITTED) {
            Status s = locks_.acquire(txn.id, txn.timestamp, k, LockMode::SHARED);
            if (s != Status::OK) { rollback(txn, s); return s; }
            v = load_and_record(txn, k);
            locks_.release_shared(txn.id, k);
        } else { 
            v = load_and_record(txn, k);
        }
        ++txn.reads;
        if (v >= threshold) {
            ++count_out;
            if (matched_out) matched_out->push_back(k);
        }
    }
    return Status::OK;
}

Status TransactionManager::read_for_update(Transaction& txn, Key key, Value& out) {
    if (txn.state != TxnState::ACTIVE) return Status::ERR_BAD_STATE;
    if (key >= cfg_.num_records) return Status::ERR_BAD_KEY;
    Status s = locks_.acquire(txn.id, txn.timestamp, key, LockMode::EXCLUSIVE);
    if (s != Status::OK) { rollback(txn, s); return s; }
    out = load_and_record(txn, key);
    ++txn.reads;
    return Status::OK;
}

Status TransactionManager::write(Transaction& txn, Key key, Value value) {
    if (txn.state != TxnState::ACTIVE) return Status::ERR_BAD_STATE;
    if (key >= cfg_.num_records) return Status::ERR_BAD_KEY;
    Status s = locks_.acquire(txn.id, txn.timestamp, key, LockMode::EXCLUSIVE);
    if (s != Status::OK) { rollback(txn, s); return s; }
    Value old = store_[key].load();
    log_event(LogType::UPDATE, txn, key, old, value);  // WAL: log first ...
    txn.undo_log.push_back({key, old});
    store_and_record(txn, key, value);              
    ++txn.writes;
    return Status::OK;
}

Status TransactionManager::declare(Transaction& txn, const std::vector<Key>& claim) {
    if (txn.state != TxnState::ACTIVE) return Status::ERR_BAD_STATE;
    for (Key k : claim)
        if (k >= cfg_.num_records) return Status::ERR_BAD_KEY;
    locks_.declare(txn.id, txn.timestamp, claim);
    return Status::OK;
}

Status TransactionManager::commit(Transaction& txn) {
    if (txn.state != TxnState::ACTIVE) return Status::ERR_BAD_STATE;
    if (log_) {
        uint64_t lsn = log_->append(LogType::COMMIT, txn.id);
        if (lsn == 0 || !log_->flush_for_commit()) { 
            rollback(txn, Status::ERR_IO);
            return Status::ERR_IO;
        }
    }
    txn.commit_sequence = commit_counter_.fetch_add(1) + 1;  
    txn.state = TxnState::COMMITTED;
    if (cfg_.record_history) {
        std::lock_guard<std::mutex> g(hist_mu_);
        record(txn.id, 'C');
    }
    locks_.release_all(txn.id); 
    {
        std::lock_guard<std::mutex> g(stats_mu_);
        ++stats_.committed;
    }
    finish_txn(txn);
    return Status::OK;
}

void TransactionManager::rollback(Transaction& txn, Status reason) {
    if (txn.state != TxnState::ACTIVE) return;
    for (auto it = txn.undo_log.rbegin(); it != txn.undo_log.rend(); ++it) {
        Value current = store_[it->key].load();
        log_event(LogType::CLR, txn, it->key, current, it->old_value);  
        store_[it->key].store(it->old_value);
    }
    txn.undo_log.clear();
    log_event(LogType::ABORT, txn);
    txn.state = TxnState::ABORTED;
    txn.abort_reason = reason;
    if (cfg_.record_history) {
        std::lock_guard<std::mutex> g(hist_mu_);
        record(txn.id, 'A');
    }
    locks_.release_all(txn.id);
    {
        std::lock_guard<std::mutex> g(stats_mu_);
        ++stats_.aborted;
        switch (reason) {
            case Status::ABORT_WAIT_DIE: ++stats_.aborted_wait_die; break;
            case Status::ABORT_NO_WAIT: ++stats_.aborted_no_wait; break;
            case Status::ABORT_DEADLOCK: ++stats_.aborted_deadlock; break;
            default: ++stats_.aborted_user; break;
        }
    }
    finish_txn(txn);
}

Status TransactionManager::checkpoint() {
    if (!log_) return Status::ERR_BAD_STATE;  // volatile engine
    std::unique_lock<std::mutex> lk(gate_mu_);
    gate_cv_.wait(lk, [this] { return !checkpointing_; });
    checkpointing_ = true;
    gate_cv_.wait(lk, [this] { return active_ == 0; });
    Status result = Status::OK;
    uint64_t lsn = log_->append(LogType::CHECKPOINT, 0);
    if (lsn == 0 || !log_->flush() ||
        !RecoveryManager::write_snapshot(cfg_.data_dir, lsn, store_.get(), cfg_.num_records) ||
        !log_->truncate_after_checkpoint(lsn)) {
        result = Status::ERR_IO;
    } else {
        std::lock_guard<std::mutex> g(stats_mu_);
        ++stats_.checkpoints;
    }
    checkpointing_ = false;
    gate_cv_.notify_all();
    return result;
}

Value TransactionManager::peek(Key key) const {
    if (key >= cfg_.num_records) throw std::out_of_range("peek: bad key");
    return store_[key].load();
}

Value TransactionManager::sum_all() const {
    Value s = 0;
    for (uint32_t i = 0; i < cfg_.num_records; ++i) s += store_[i].load();
    return s;
}

EngineStats TransactionManager::stats() const {
    std::lock_guard<std::mutex> g(stats_mu_);
    return stats_;
}

} 
