#include "types.h"

namespace itx {

const char* to_string(TxnState s) {
    switch (s) {
        case TxnState::ACTIVE: return "ACTIVE";
        case TxnState::COMMITTED: return "COMMITTED";
        case TxnState::ABORTED: return "ABORTED";
    }
    return "?";
}

const char* to_string(LockMode m) { return m == LockMode::SHARED ? "S" : "X"; }

const char* to_string(LockPolicy p) {
    switch (p) {
        case LockPolicy::WAIT_DIE: return "wait_die";
        case LockPolicy::NO_WAIT: return "no_wait";
        case LockPolicy::DEADLOCK_DETECT: return "detect";
        case LockPolicy::NONE: return "none";
        case LockPolicy::AVOID: return "avoid";
    }
    return "?";
}

const char* to_string(Status s) {
    switch (s) {
        case Status::OK: return "OK";
        case Status::ABORT_WAIT_DIE: return "ABORT_WAIT_DIE";
        case Status::ABORT_NO_WAIT: return "ABORT_NO_WAIT";
        case Status::ABORT_DEADLOCK: return "ABORT_DEADLOCK";
        case Status::ABORT_USER: return "ABORT_USER";
        case Status::ERR_BAD_KEY: return "ERR_BAD_KEY";
        case Status::ERR_BAD_STATE: return "ERR_BAD_STATE";
        case Status::ERR_IO: return "ERR_IO";
    }
    return "?";
}

bool parse_policy(const std::string& text, LockPolicy& out) {
    if (text == "wait_die") out = LockPolicy::WAIT_DIE;
    else if (text == "no_wait") out = LockPolicy::NO_WAIT;
    else if (text == "detect") out = LockPolicy::DEADLOCK_DETECT;
    else if (text == "none") out = LockPolicy::NONE;
    else if (text == "avoid") out = LockPolicy::AVOID;
    else return false;
    return true;
}

const char* to_string(IsolationLevel l) {
    switch (l) {
        case IsolationLevel::READ_UNCOMMITTED: return "RU";
        case IsolationLevel::READ_COMMITTED: return "RC";
        case IsolationLevel::REPEATABLE_READ: return "RR";
        case IsolationLevel::SERIALIZABLE: return "SER";
    }
    return "?";
}

const char* long_name(IsolationLevel l) {
    switch (l) {
        case IsolationLevel::READ_UNCOMMITTED: return "READ UNCOMMITTED";
        case IsolationLevel::READ_COMMITTED: return "READ COMMITTED";
        case IsolationLevel::REPEATABLE_READ: return "REPEATABLE READ";
        case IsolationLevel::SERIALIZABLE: return "SERIALIZABLE";
    }
    return "?";
}

bool parse_isolation(const std::string& text, IsolationLevel& out) {
    for (IsolationLevel l : {IsolationLevel::READ_UNCOMMITTED, IsolationLevel::READ_COMMITTED,
                             IsolationLevel::REPEATABLE_READ, IsolationLevel::SERIALIZABLE}) {
        if (text == to_string(l) || text == long_name(l)) { out = l; return true; }
    }
    if (text == "read_uncommitted") out = IsolationLevel::READ_UNCOMMITTED;
    else if (text == "read_committed") out = IsolationLevel::READ_COMMITTED;
    else if (text == "repeatable_read") out = IsolationLevel::REPEATABLE_READ;
    else if (text == "serializable") out = IsolationLevel::SERIALIZABLE;
    else return false;
    return true;
}

}  // namespace itx
