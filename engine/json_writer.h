// Tiny streaming JSON writer (no dependency). Output is consumed by the Python backend.
#pragma once
#include <cmath>
#include <cstdio>
#include <string>
#include <vector>

namespace itx {

class JsonWriter {
public:
    JsonWriter& begin_object() { sep(); out_ += '{'; first_.push_back(true); return *this; }
    JsonWriter& end_object() { out_ += '}'; first_.pop_back(); return *this; }
    JsonWriter& begin_array() { sep(); out_ += '['; first_.push_back(true); return *this; }
    JsonWriter& end_array() { out_ += ']'; first_.pop_back(); return *this; }
    JsonWriter& key(const std::string& k) {
        sep();
        out_ += quote(k);
        out_ += ':';
        after_key_ = true;
        return *this;
    }
    JsonWriter& value(const std::string& v) { sep(); out_ += quote(v); return *this; }
    JsonWriter& value(const char* v) { return value(std::string(v)); }
    JsonWriter& value(bool v) { sep(); out_ += v ? "true" : "false"; return *this; }
    JsonWriter& value(int v) { sep(); out_ += std::to_string(v); return *this; }
    JsonWriter& value(long v) { sep(); out_ += std::to_string(v); return *this; }
    JsonWriter& value(long long v) { sep(); out_ += std::to_string(v); return *this; }
    JsonWriter& value(unsigned long v) { sep(); out_ += std::to_string(v); return *this; }
    JsonWriter& value(unsigned long long v) { sep(); out_ += std::to_string(v); return *this; }
    JsonWriter& value(unsigned v) { sep(); out_ += std::to_string(v); return *this; }
    JsonWriter& value(double v) {
        sep();
        if (!std::isfinite(v)) { out_ += "null"; return *this; }
        char buf[64];
        std::snprintf(buf, sizeof buf, "%.6f", v);
        out_ += buf;
        return *this;
    }
    template <class T>
    JsonWriter& field(const std::string& k, const T& v) { key(k); return value(v); }
    template <class T>
    JsonWriter& int_array(const std::string& k, const std::vector<T>& v) {
        key(k);
        begin_array();
        for (const auto& x : v) value(static_cast<long long>(x));
        return end_array();
    }
    const std::string& str() const { return out_; }

private:
    void sep() {
        if (after_key_) { after_key_ = false; return; }
        if (!first_.empty()) {
            if (!first_.back()) out_ += ',';
            first_.back() = false;
        }
    }
    static std::string quote(const std::string& s) {
        std::string o = "\"";
        for (unsigned char c : s) {
            switch (c) {
                case '"': o += "\\\""; break;
                case '\\': o += "\\\\"; break;
                case '\n': o += "\\n"; break;
                case '\r': o += "\\r"; break;
                case '\t': o += "\\t"; break;
                default:
                    if (c < 0x20) { char b[8]; std::snprintf(b, sizeof b, "\\u%04x", c); o += b; }
                    else o += static_cast<char>(c);
            }
        }
        return o + "\"";
    }
    std::string out_;
    std::vector<bool> first_;
    bool after_key_ = false;
};

}  // namespace itx
