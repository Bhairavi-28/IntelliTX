// Write-ahead log: append-only, text records, one per line, each protected by a checksum.
//
//   L <lsn> <type> <txn> <key> <old> <new> <crc32hex>\n
//
// Types: B begin, U update (before/after image), C compensation (logged undo), M commit,
//        A abort (rollback finished), K checkpoint marker.
// Records are written with write(2) directly (no user-space buffer), so a process crash
// (SIGKILL / _exit) cannot lose a record that append() has returned for. fsync() is what protects
// against OS crash / power loss; it is issued at commit when sync_commit is true.
#pragma once
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "types.h"

namespace itx {

enum class LogType : char {
    BEGIN = 'B',
    UPDATE = 'U',
    CLR = 'C',
    COMMIT = 'M',
    ABORT = 'A',
    CHECKPOINT = 'K'
};

struct LogRecord {
    uint64_t lsn = 0;
    LogType type = LogType::BEGIN;
    TxnId txn = 0;
    Key key = 0;
    Value old_value = 0;
    Value new_value = 0;
};

std::string serialize_record(const LogRecord& r);
bool parse_record(const std::string& line, LogRecord& out);

struct LogScan {
    std::vector<LogRecord> records;
    uint64_t valid_bytes = 0;  // prefix of the file that parsed cleanly
    uint64_t file_bytes = 0;
    bool torn_tail = false;  // trailing bytes were missing/corrupt
};
LogScan scan_log_file(const std::string& path);  // missing file => empty scan

class LogManager {
public:
    LogManager(const std::string& path, bool sync_commit, uint64_t next_lsn = 1);
    ~LogManager();
    LogManager(const LogManager&) = delete;
    LogManager& operator=(const LogManager&) = delete;

    bool ok() const { return fd_ >= 0; }
    uint64_t append(LogType type, TxnId txn, Key key = 0, Value old_v = 0, Value new_v = 0);
    // Forces everything appended so far to stable storage (fsync).
    bool flush();
    // Called at commit: flush() iff sync_commit.
    bool flush_for_commit();
    void set_next_lsn(uint64_t lsn);
    uint64_t last_lsn() const;
    bool sync_commit() const { return sync_commit_; }
    // After a durable snapshot at `checkpoint_lsn`: atomically replace the log by a single
    // CHECKPOINT record (tmp file + fsync + rename + directory fsync).
    bool truncate_after_checkpoint(uint64_t checkpoint_lsn);
    uint64_t bytes_written() const;

private:
    bool write_all(const std::string& s);

    std::string path_;
    bool sync_commit_;
    int fd_ = -1;
    mutable std::mutex mu_;
    uint64_t next_lsn_;
    uint64_t bytes_ = 0;
};

bool fsync_directory(const std::string& dir);

}  // namespace itx
