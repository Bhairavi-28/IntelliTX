// Minimal self-contained test harness (no external dependency).
#pragma once
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "types.h"
inline std::ostream& operator<<(std::ostream& o, itx::Status s) { return o << itx::to_string(s); }

struct TestCase {
    std::string name;
    std::function<void()> fn;
};
std::vector<TestCase>& test_registry();

struct TestRegistrar {
    TestRegistrar(const char* name, std::function<void()> fn) { test_registry().push_back({name, std::move(fn)}); }
};

struct TestFailure : std::runtime_error {
    using std::runtime_error::runtime_error;
};

#define TEST(name)                                   \
    static void name();                              \
    static TestRegistrar registrar_##name(#name, name); \
    static void name()

#define CHECK(cond)                                                                           \
    do {                                                                                      \
        if (!(cond))                                                                          \
            throw TestFailure(std::string(__FILE__) + ":" + std::to_string(__LINE__) +        \
                              ": CHECK failed: " #cond);                                      \
    } while (0)

template <class A, class B>
void check_eq_impl(const A& a, const B& b, const char* ea, const char* eb, const char* file, int line) {
    if (!(a == b)) {
        std::ostringstream os;
        os << file << ":" << line << ": CHECK_EQ failed: " << ea << " == " << eb << "  (" << a << " vs " << b << ")";
        throw TestFailure(os.str());
    }
}
#define CHECK_EQ(a, b) check_eq_impl((a), (b), #a, #b, __FILE__, __LINE__)

#define CHECK_THROWS(expr)                                                                    \
    do {                                                                                      \
        bool threw_ = false;                                                                  \
        try { expr; } catch (const std::exception&) { threw_ = true; }                        \
        if (!threw_) throw TestFailure(std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
                                       ": expected exception: " #expr);                       \
    } while (0)

// Temporary directory removed on destruction.
struct TempDir {
    std::string path;
    TempDir() {
        char tmpl[] = "/tmp/itx_test_XXXXXX";
        char* p = mkdtemp(tmpl);
        if (!p) throw std::runtime_error("mkdtemp failed");
        path = p;
    }
    ~TempDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
    std::string sub(const std::string& name) const { return path + "/" + name; }
};
