#include "wal.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>

namespace itx {

namespace {
uint32_t fnv1a(const std::string& s) {
    uint32_t h = 2166136261u;
    for (unsigned char c : s) {
        h ^= c;
        h *= 16777619u;
    }
    return h;
}
std::string dir_of(const std::string& path) {
    auto pos = path.find_last_of('/');
    return pos == std::string::npos ? "." : path.substr(0, pos);
}
}  // namespace

bool fsync_directory(const std::string& dir) {
    int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) return false;
    bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

std::string serialize_record(const LogRecord& r) {
    std::ostringstream body;
    body << "L " << r.lsn << ' ' << static_cast<char>(r.type) << ' ' << r.txn << ' ' << r.key
         << ' ' << r.old_value << ' ' << r.new_value;
    std::string text = body.str();
    char crc[16];
    std::snprintf(crc, sizeof crc, "%08x", fnv1a(text));
    return text + ' ' + crc + '\n';
}

bool parse_record(const std::string& line, LogRecord& out) {
    auto pos = line.find_last_of(' ');
    if (pos == std::string::npos || line.size() - pos - 1 != 8) return false;
    std::string body = line.substr(0, pos);
    char expected[16];
    std::snprintf(expected, sizeof expected, "%08x", fnv1a(body));
    if (line.compare(pos + 1, 8, expected) != 0) return false;
    std::istringstream is(body);
    std::string tag;
    char type = 0;
    LogRecord r;
    if (!(is >> tag >> r.lsn >> type >> r.txn >> r.key >> r.old_value >> r.new_value)) return false;
    if (tag != "L") return false;
    switch (type) {
        case 'B': case 'U': case 'C': case 'M': case 'A': case 'K': break;
        default: return false;
    }
    r.type = static_cast<LogType>(type);
    out = r;
    return true;
}

LogScan scan_log_file(const std::string& path) {
    LogScan scan;
    std::ifstream in(path, std::ios::binary);
    if (!in) return scan;
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    scan.file_bytes = data.size();
    size_t pos = 0;
    while (pos < data.size()) {
        size_t nl = data.find('\n', pos);
        if (nl == std::string::npos) break;  // partial last line => torn
        LogRecord r;
        if (!parse_record(data.substr(pos, nl - pos), r)) break;  // corrupt => stop
        scan.records.push_back(r);
        pos = nl + 1;
    }
    scan.valid_bytes = pos;
    scan.torn_tail = pos < data.size();
    return scan;
}

LogManager::LogManager(const std::string& path, bool sync_commit, uint64_t next_lsn)
    : path_(path), sync_commit_(sync_commit), next_lsn_(next_lsn) {
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ >= 0) {
        struct stat st;
        if (::fstat(fd_, &st) == 0) bytes_ = static_cast<uint64_t>(st.st_size);
        fsync_directory(dir_of(path));  // make the file's directory entry durable
    }
}

LogManager::~LogManager() {
    if (fd_ >= 0) ::close(fd_);
}

bool LogManager::write_all(const std::string& s) {
    size_t done = 0;
    while (done < s.size()) {
        ssize_t n = ::write(fd_, s.data() + done, s.size() - done);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        done += static_cast<size_t>(n);
    }
    bytes_ += s.size();
    return true;
}

uint64_t LogManager::append(LogType type, TxnId txn, Key key, Value old_v, Value new_v) {
    std::lock_guard<std::mutex> g(mu_);
    LogRecord r;
    r.lsn = next_lsn_++;
    r.type = type;
    r.txn = txn;
    r.key = key;
    r.old_value = old_v;
    r.new_value = new_v;
    if (fd_ < 0 || !write_all(serialize_record(r))) return 0;
    return r.lsn;
}

bool LogManager::flush() {
    std::lock_guard<std::mutex> g(mu_);
    return fd_ >= 0 && ::fsync(fd_) == 0;
}

bool LogManager::flush_for_commit() { return sync_commit_ ? flush() : true; }

void LogManager::set_next_lsn(uint64_t lsn) {
    std::lock_guard<std::mutex> g(mu_);
    next_lsn_ = lsn;
}

uint64_t LogManager::last_lsn() const {
    std::lock_guard<std::mutex> g(mu_);
    return next_lsn_ - 1;
}

uint64_t LogManager::bytes_written() const {
    std::lock_guard<std::mutex> g(mu_);
    return bytes_;
}

bool LogManager::truncate_after_checkpoint(uint64_t checkpoint_lsn) {
    std::lock_guard<std::mutex> g(mu_);
    LogRecord r;
    r.lsn = checkpoint_lsn;
    r.type = LogType::CHECKPOINT;
    std::string tmp = path_ + ".tmp";
    int tfd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (tfd < 0) return false;
    std::string rec = serialize_record(r);
    bool ok = ::write(tfd, rec.data(), rec.size()) == static_cast<ssize_t>(rec.size()) &&
              ::fsync(tfd) == 0;
    ::close(tfd);
    if (!ok || ::rename(tmp.c_str(), path_.c_str()) != 0) return false;
    fsync_directory(dir_of(path_));
    if (fd_ >= 0) ::close(fd_);
    fd_ = ::open(path_.c_str(), O_WRONLY | O_APPEND, 0644);
    bytes_ = rec.size();
    return fd_ >= 0;
}

}  // namespace itx
