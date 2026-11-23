#include <unistd.h>

#include <chrono>

#include "test_framework.h"

std::vector<TestCase>& test_registry() {
    static std::vector<TestCase> r;
    return r;
}

int main(int argc, char** argv) {
    alarm(600);  // watchdog: a hung test (e.g. a lost wake-up) kills the run instead of blocking forever
    std::string filter = argc > 1 ? argv[1] : "";
    int passed = 0, failed = 0;
    for (const auto& t : test_registry()) {
        if (!filter.empty() && t.name.find(filter) == std::string::npos) continue;
        auto t0 = std::chrono::steady_clock::now();
        try {
            t.fn();
            double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
            std::cout << "[ PASS ] " << t.name << " (" << static_cast<long>(ms) << " ms)\n";
            ++passed;
        } catch (const std::exception& e) {
            std::cout << "[ FAIL ] " << t.name << "\n         " << e.what() << "\n";
            ++failed;
        }
        std::cout.flush();
    }
    std::cout << "\n" << passed << " passed, " << failed << " failed\n";
    return failed == 0 ? 0 : 1;
}
