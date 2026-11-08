// Shared basic types for the IntelliTX engine.
#pragma once
#include <cstdint>
#include <string>

namespace itx {

using TxnId = uint64_t;
using Key = uint32_t;    // record id: 0 .. num_records-1
using Value = int64_t;

enum class TxnState { ACTIVE, COMMITTED, ABORTED };
enum class LockMode { SHARED, EXCLUSIVE };

// WAIT_DIE         : Strict 2PL + Wait-Die deadlock PREVENTION (older waits, younger dies).
// NO_WAIT          : Strict 2PL + abort on any conflict (prevention without waiting).
// DEADLOCK_DETECT  : Strict 2PL + waiting + wait-for-graph cycle DETECTION and victim abort.
// NONE             : no locking at all. UNSAFE; exists only to demonstrate anomalies (lost update).
// AVOID            : Strict 2PL + Banker's-style deadlock AVOIDANCE: every transaction declares the keys it may
//                    lock; a lock is granted only if a safe sequence still exists (all locks treated as exclusive).
enum class LockPolicy { WAIT_DIE, NO_WAIT, DEADLOCK_DETECT, NONE, AVOID };

enum class Status {
    OK,
    ABORT_WAIT_DIE,
    ABORT_NO_WAIT,
    ABORT_DEADLOCK,
    ABORT_USER,
    ERR_BAD_KEY,
    ERR_BAD_STATE,
    ERR_IO
};

inline bool is_abort(Status s) {
    return s == Status::ABORT_WAIT_DIE || s == Status::ABORT_NO_WAIT ||
           s == Status::ABORT_DEADLOCK || s == Status::ABORT_USER;
}

const char* to_string(TxnState s);
const char* to_string(LockMode m);
const char* to_string(LockPolicy p);
const char* to_string(Status s);
bool parse_policy(const std::string& text, LockPolicy& out);

}  // namespace itx
