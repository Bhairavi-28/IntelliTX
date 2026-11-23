#include "serializability.h"

#include <algorithm>
#include <cctype>
#include <map>
#include <set>
#include <stdexcept>
#include <tuple>

#include "deadlock_detector.h"

namespace itx {

const char* to_string(Tristate t) {
    switch (t) {
        case Tristate::YES: return "yes";
        case Tristate::NO: return "no";
        case Tristate::UNKNOWN: return "unknown";
    }
    return "?";
}

// ------------------------------------------------------------------ parsing / formatting

std::vector<ScheduleOp> parse_schedule(const std::string& text) {
    const std::string help = " (expected operations like R1(A) W2(B) C1 A2)";
    std::vector<ScheduleOp> ops;
    size_t i = 0, n = text.size();
    auto is_sep = [](char c) { return std::isspace(static_cast<unsigned char>(c)) || c == ',' || c == ';'; };
    while (i < n) {
        if (is_sep(text[i])) { ++i; continue; }
        size_t start = i;
        char op = static_cast<char>(std::toupper(static_cast<unsigned char>(text[i++])));
        if (op != 'R' && op != 'W' && op != 'C' && op != 'A' && op != 'B')
            throw std::invalid_argument("unknown operation '" + std::string(1, text[start]) + "' at position " +
                                        std::to_string(start + 1) + help);
        size_t d0 = i;
        while (i < n && std::isdigit(static_cast<unsigned char>(text[i]))) ++i;
        if (i == d0)
            throw std::invalid_argument("missing transaction number after '" + std::string(1, text[start]) +
                                        "' at position " + std::to_string(start + 1) + help);
        TxnId txn = std::stoull(text.substr(d0, i - d0));
        if (txn == 0) throw std::invalid_argument("transaction numbers start at 1" + help);
        ScheduleOp o;
        o.txn = txn;
        o.op = op;
        if (op == 'R' || op == 'W') {
            if (i >= n || (text[i] != '(' && text[i] != '['))
                throw std::invalid_argument(std::string(1, op) + std::to_string(txn) + " needs a data item, e.g. " +
                                            std::string(1, op) + std::to_string(txn) + "(A)");
            char close = text[i] == '(' ? ')' : ']';
            size_t j = text.find(close, i + 1);
            if (j == std::string::npos) throw std::invalid_argument("missing '" + std::string(1, close) + "'" + help);
            std::string item = text.substr(i + 1, j - i - 1);
            item.erase(0, item.find_first_not_of(" \t"));
            item.erase(item.find_last_not_of(" \t") + 1);
            if (item.empty()) throw std::invalid_argument("empty data item" + help);
            o.item = item;
            i = j + 1;
        } else if (i < n && text[i] == '(') {  // tolerate C1() / A1()
            size_t j = text.find(')', i);
            if (j == std::string::npos) throw std::invalid_argument("missing ')'" + help);
            i = j + 1;
        }
        if (op == 'B') continue;  // begin markers carry no information for the analysis
        ops.push_back(o);
    }
    if (ops.empty()) throw std::invalid_argument("the schedule is empty" + help);
    if (ops.size() > 500) throw std::invalid_argument("schedule too long (max 500 operations)");
    return ops;
}

std::string format_schedule(const std::vector<ScheduleOp>& ops) {
    std::string s;
    for (const auto& o : ops) {
        if (!s.empty()) s += ' ';
        s += o.op;
        s += std::to_string(o.txn);
        if (o.op == 'R' || o.op == 'W') s += "(" + o.item + ")";
    }
    return s;
}

std::vector<ScheduleOp> schedule_from_history(const std::vector<HistoryOp>& history,
                                              const std::vector<std::string>& names) {
    std::vector<ScheduleOp> out;
    out.reserve(history.size());
    for (const auto& h : history) {
        ScheduleOp o;
        o.txn = h.txn;
        o.op = h.op;
        if (h.op == 'R' || h.op == 'W')
            o.item = h.key < names.size() && !names[h.key].empty() ? names[h.key] : "k" + std::to_string(h.key);
        out.push_back(o);
    }
    return out;
}

// ------------------------------------------------------------------ analysis helpers

namespace {

bool conflicts(const ScheduleOp& a, const ScheduleOp& b) {
    return a.txn != b.txn && (a.op == 'R' || a.op == 'W') && (b.op == 'R' || b.op == 'W') &&
           a.item == b.item && (a.op == 'W' || b.op == 'W');
}

// View-equivalence signature of a schedule (R/W ops only): for every read, identified by
// (txn, index of the op within its txn), the transaction it reads from (0 = initial value); and,
// for every item, the transaction that writes it last.
struct ViewSig {
    std::map<std::pair<TxnId, size_t>, TxnId> reads_from;
    std::map<std::string, TxnId> final_writer;
    bool operator==(const ViewSig& o) const { return reads_from == o.reads_from && final_writer == o.final_writer; }
};

ViewSig view_signature(const std::vector<ScheduleOp>& ops) {
    ViewSig sig;
    std::map<std::string, TxnId> last_writer;
    std::map<TxnId, size_t> idx;
    for (const auto& o : ops) {
        size_t k = idx[o.txn]++;
        if (o.op == 'R') {
            auto it = last_writer.find(o.item);
            sig.reads_from[{o.txn, k}] = it == last_writer.end() ? 0 : it->second;
        } else if (o.op == 'W') {
            last_writer[o.item] = o.txn;
        }
    }
    sig.final_writer = last_writer;
    return sig;
}

std::vector<ScheduleOp> serial_schedule(const std::vector<ScheduleOp>& ops, const std::vector<TxnId>& order) {
    std::vector<ScheduleOp> out;
    for (TxnId t : order)
        for (const auto& o : ops)
            if (o.txn == t) out.push_back(o);
    return out;
}

std::string tname(TxnId t) { return "T" + std::to_string(t); }

}  // namespace

// ------------------------------------------------------------------ main analysis

SerializabilityResult analyze_schedule(const std::vector<ScheduleOp>& ops) {
    if (ops.empty()) throw std::invalid_argument("the schedule is empty");
    SerializabilityResult res;
    res.schedule = ops;

    // ---- well-formedness + commit/abort positions ----
    std::vector<TxnId> order;  // first appearance
    std::map<TxnId, size_t> commit_pos, abort_pos;
    for (size_t p = 0; p < ops.size(); ++p) {
        const auto& o = ops[p];
        if (std::find(order.begin(), order.end(), o.txn) == order.end()) order.push_back(o.txn);
        if (commit_pos.count(o.txn) || abort_pos.count(o.txn))
            throw std::invalid_argument(tname(o.txn) + " has an operation after its commit/abort");
        if (o.op == 'C') commit_pos[o.txn] = p;
        else if (o.op == 'A') abort_pos[o.txn] = p;
    }
    std::set<TxnId> aborted;
    for (const auto& [t, p] : abort_pos) { (void)p; aborted.insert(t); }
    for (TxnId t : order) (aborted.count(t) ? res.aborted : res.transactions).push_back(t);

    // ---- committed projection: R/W ops of non-aborted transactions, original positions kept ----
    std::vector<size_t> proj;
    std::vector<ScheduleOp> proj_ops;
    for (size_t p = 0; p < ops.size(); ++p)
        if ((ops[p].op == 'R' || ops[p].op == 'W') && !aborted.count(ops[p].txn)) {
            proj.push_back(p);
            proj_ops.push_back(ops[p]);
        }

    // ---- precedence graph ----
    std::map<std::pair<TxnId, TxnId>, size_t> edge_index;
    for (size_t a = 0; a < proj.size(); ++a)
        for (size_t b = a + 1; b < proj.size(); ++b) {
            const auto& x = ops[proj[a]];
            const auto& y = ops[proj[b]];
            if (!conflicts(x, y)) continue;
            auto key = std::make_pair(x.txn, y.txn);
            auto it = edge_index.find(key);
            if (it == edge_index.end()) {
                ConflictEdge e;
                e.from = x.txn; e.to = y.txn; e.item = x.item;
                e.from_op = x.op; e.to_op = y.op;
                e.from_index = proj[a]; e.to_index = proj[b];
                e.conflicts = 1;
                edge_index[key] = res.edges.size();
                res.edges.push_back(e);
            } else {
                ++res.edges[it->second].conflicts;
            }
        }
    WaitForGraph g;  // reuse the DFS cycle finder of the deadlock detector
    for (const auto& e : res.edges) g.add_edge(e.from, e.to);
    res.cycle = g.find_cycle();
    res.conflict_serializable = res.cycle.empty();

    if (res.conflict_serializable) {  // Kahn's algorithm, ties broken by first appearance
        std::map<TxnId, int> indeg;
        for (TxnId t : res.transactions) indeg[t] = 0;
        for (const auto& e : res.edges) ++indeg[e.to];
        std::vector<TxnId> remaining = res.transactions;
        while (!remaining.empty()) {
            auto it = std::find_if(remaining.begin(), remaining.end(), [&](TxnId t) { return indeg[t] == 0; });
            TxnId t = *it;  // exists because the graph is acyclic
            remaining.erase(it);
            res.serial_order.push_back(t);
            for (const auto& e : res.edges)
                if (e.from == t) --indeg[e.to];
        }
    }

    // ---- view serializability ----
    if (res.conflict_serializable) {
        res.view_serializable = Tristate::YES;
        res.view_serial_order = res.serial_order;
        res.view_note = "conflict-serializable schedules are always view-serializable";
    } else if (res.transactions.size() <= 8) {
        ViewSig target = view_signature(proj_ops);
        std::vector<TxnId> perm = res.transactions;
        std::sort(perm.begin(), perm.end());
        res.view_serializable = Tristate::NO;
        do {
            if (view_signature(serial_schedule(proj_ops, perm)) == target) {
                res.view_serializable = Tristate::YES;
                res.view_serial_order = perm;
                break;
            }
        } while (std::next_permutation(perm.begin(), perm.end()));
        res.view_note = res.view_serializable == Tristate::YES
                            ? "view-serializable but NOT conflict-serializable: only possible with blind writes"
                            : "no serial order gives the same reads-from relation and final writes";
    } else {
        res.view_note = "not checked: more than 8 transactions (view serializability is NP-complete)";
    }

    // ---- recoverability (needs explicit end points) ----
    std::vector<TxnId> unfinished;
    for (TxnId t : order)
        if (!commit_pos.count(t) && !abort_pos.count(t)) unfinished.push_back(t);
    if (!unfinished.empty()) {
        res.recoverability_note = "add a commit (C" + std::to_string(unfinished.front()) + ") or abort (A" +
                                  std::to_string(unfinished.front()) +
                                  ") for every transaction to classify recoverability";
        return res;
    }
    auto end_pos = [&](TxnId t) { return commit_pos.count(t) ? commit_pos[t] : abort_pos[t]; };
    struct ReadsFrom { TxnId reader, writer; std::string item; size_t pos; };
    std::vector<ReadsFrom> rf;
    for (size_t p = 0; p < ops.size(); ++p) {
        if (ops[p].op != 'R') continue;
        for (size_t q = p; q-- > 0;) {
            if (ops[q].op != 'W' || ops[q].item != ops[p].item) continue;
            if (abort_pos.count(ops[q].txn) && abort_pos[ops[q].txn] < p) continue;  // write was undone already
            if (ops[q].txn != ops[p].txn) rf.push_back({ops[p].txn, ops[q].txn, ops[p].item, p});
            break;
        }
    }
    res.recoverable = res.cascadeless = res.strict = Tristate::YES;
    std::vector<std::string> notes;
    for (const auto& r : rf) {
        if (commit_pos.count(r.reader) &&
            (!commit_pos.count(r.writer) || commit_pos[r.writer] > commit_pos[r.reader])) {
            if (res.recoverable == Tristate::YES)
                notes.push_back("NOT recoverable: " + tname(r.reader) + " read " + r.item + " written by " +
                                tname(r.writer) + " and committed, but " + tname(r.writer) +
                                (commit_pos.count(r.writer) ? " committed later" : " aborted"));
            res.recoverable = Tristate::NO;
        }
        if (!commit_pos.count(r.writer) || commit_pos[r.writer] > r.pos) {
            if (res.cascadeless == Tristate::YES)
                notes.push_back("NOT cascadeless: " + tname(r.reader) + " read " + r.item + " before its writer " +
                                tname(r.writer) + " committed (dirty read)");
            res.cascadeless = Tristate::NO;
        }
    }
    for (size_t p = 0; p < ops.size() && res.strict == Tristate::YES; ++p) {
        if (ops[p].op != 'R' && ops[p].op != 'W') continue;
        for (size_t q = 0; q < p; ++q)
            if (ops[q].op == 'W' && ops[q].item == ops[p].item && ops[q].txn != ops[p].txn && end_pos(ops[q].txn) > p) {
                notes.push_back("NOT strict: " + std::string(1, ops[p].op) + std::to_string(ops[p].txn) + "(" + ops[p].item +
                                ") touches an item written by " + tname(ops[q].txn) + " before " + tname(ops[q].txn) +
                                " ended");
                res.strict = Tristate::NO;
                break;
            }
    }
    if (notes.empty()) notes.push_back("strict (hence cascadeless and recoverable)");
    for (size_t i = 0; i < notes.size(); ++i) res.recoverability_note += (i ? "; " : "") + notes[i];
    return res;
}

}  // namespace itx
