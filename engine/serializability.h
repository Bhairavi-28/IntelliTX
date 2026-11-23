// Schedule analysis (DBMS theory, Silberschatz ch. 17 / Elmasri-Navathe ch. 20):
//
//   * CONFLICT SERIALIZABILITY via the PRECEDENCE GRAPH: nodes = committed transactions,
//     edge Ti -> Tj when an operation of Ti precedes a CONFLICTING operation of Tj on the same item
//     (same item, different transactions, at least one is a write). The schedule is conflict-
//     serializable iff the graph is acyclic; a topological order is an equivalent serial schedule.
//     Aborted transactions are removed first (the "committed projection").
//   * VIEW SERIALIZABILITY (brute force over serial orders, only for <= 8 committed transactions):
//     same initial reads, same reads-from relation, same final writes as some serial schedule.
//   * RECOVERABILITY classes (need explicit commit/abort points): recoverable, cascadeless
//     (avoids cascading aborts), strict.
//
// Input comes either from text ("R1(A) W2(A) C1 C2", see parse_schedule) or from the engine's
// recorded history (TransactionManager::history()).
#pragma once
#include <string>
#include <vector>

#include "transaction_manager.h"
#include "types.h"

namespace itx {

struct ScheduleOp {
    TxnId txn = 0;
    char op = 'R';     // 'R' read, 'W' write, 'C' commit, 'A' abort
    std::string item;  // data item name (empty for C/A)
};

struct ConflictEdge {
    TxnId from = 0, to = 0;
    std::string item;        // item of the FIRST conflict that created the edge
    char from_op = 'R', to_op = 'W';
    size_t from_index = 0, to_index = 0;  // positions in the input schedule (0-based)
    size_t conflicts = 0;    // how many conflicting pairs support this edge
};

enum class Tristate { YES, NO, UNKNOWN };
const char* to_string(Tristate t);

struct SerializabilityResult {
    std::vector<ScheduleOp> schedule;   // the input, as analysed
    std::vector<TxnId> transactions;    // committed (= not aborted) transactions, first-appearance order
    std::vector<TxnId> aborted;         // removed before building the graph
    std::vector<ConflictEdge> edges;
    bool conflict_serializable = false;
    std::vector<TxnId> cycle;           // one cycle when not conflict-serializable
    std::vector<TxnId> serial_order;    // an equivalent serial order when conflict-serializable
    Tristate view_serializable = Tristate::UNKNOWN;
    std::vector<TxnId> view_serial_order;
    std::string view_note;
    Tristate recoverable = Tristate::UNKNOWN, cascadeless = Tristate::UNKNOWN, strict = Tristate::UNKNOWN;
    std::string recoverability_note;    // why a class is violated / why it is unknown
};

// Parses "R1(A) W2(A) R2(B) C2 W1(B) C1" (also accepts r1(x), w1[x], c1, a1, commas/semicolons).
// Throws std::invalid_argument with a helpful message on bad input.
std::vector<ScheduleOp> parse_schedule(const std::string& text);
std::string format_schedule(const std::vector<ScheduleOp>& ops);

SerializabilityResult analyze_schedule(const std::vector<ScheduleOp>& ops);

// Engine history -> schedule (items are named "k<key>" unless `names` gives a name per key).
std::vector<ScheduleOp> schedule_from_history(const std::vector<HistoryOp>& history,
                                              const std::vector<std::string>& names = {});

}  // namespace itx
