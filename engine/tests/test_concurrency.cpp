#include <atomic>
#include <set>
#include <thread>

#include "sync_primitives.h"
#include "test_framework.h"
#include "worker_pool.h"

using namespace itx;

TEST(sync_semaphore_basics) {
    Semaphore s(2);
    CHECK(s.try_acquire());
    CHECK(s.try_acquire());
    CHECK(!s.try_acquire());
    s.release();
    CHECK_EQ(s.value(), 1);
    CHECK_THROWS(Semaphore(-1));
}

TEST(sync_critical_section_mutex_protects_counter_and_peterson) {
    long plain = 0;
    std::mutex m;
    PetersonLock pl;
    long petersonCounter = 0;
    std::vector<std::thread> th;
    for (int i = 0; i < 4; ++i)
        th.emplace_back([&] { for (int k = 0; k < 50000; ++k) { std::lock_guard<std::mutex> g(m); ++plain; } });
    for (auto& t : th) t.join();
    CHECK_EQ(plain, 200000);
    std::thread a([&] { for (int k = 0; k < 50000; ++k) { pl.lock(0); ++petersonCounter; pl.unlock(0); } });
    std::thread b([&] { for (int k = 0; k < 50000; ++k) { pl.lock(1); ++petersonCounter; pl.unlock(1); } });
    a.join(); b.join();
    CHECK_EQ(petersonCounter, 100000);  // mutual exclusion held
}

template <class Q, class Put, class Take>
static void producer_consumer(Q& q, Put put, Take take, int producers, int consumers, int per_producer) {
    std::atomic<long> consumed_sum{0}, consumed_cnt{0};
    std::vector<std::thread> th;
    for (int p = 0; p < producers; ++p)
        th.emplace_back([&, p] { for (int i = 0; i < per_producer; ++i) put(q, p * per_producer + i + 1); });
    const long total = static_cast<long>(producers) * per_producer;
    for (int c = 0; c < consumers; ++c)
        th.emplace_back([&] {
            for (;;) {
                if (consumed_cnt.fetch_add(1) >= total) { break; }
                consumed_sum += take(q);
            }
        });
    for (auto& t : th) t.join();
    CHECK_EQ(consumed_sum.load(), total * (total + 1) / 2);  // every item consumed exactly once
}

TEST(producer_consumer_semaphore_buffer_stress) {
    SemaphoreBuffer<int> buf(4);  // tiny buffer => constant blocking
    producer_consumer(buf, [](SemaphoreBuffer<int>& q, int v) { q.put(v); },
                      [](SemaphoreBuffer<int>& q) { return static_cast<long>(q.take()); }, 4, 4, 5000);
}

TEST(producer_consumer_bounded_queue_condvar_stress_and_capacity) {
    BoundedQueue<int> q(3);
    std::atomic<size_t> max_seen{0};
    std::atomic<long> sum{0};
    std::thread prod([&] {
        for (int i = 1; i <= 20000; ++i) { q.push(i); size_t s = q.size(); size_t m = max_seen.load(); while (s > m && !max_seen.compare_exchange_weak(m, s)) {} }
        q.close();
    });
    std::vector<std::thread> cons;
    for (int c = 0; c < 3; ++c)
        cons.emplace_back([&] { int v; while (q.pop(v)) sum += v; });
    prod.join();
    for (auto& t : cons) t.join();
    CHECK_EQ(sum.load(), 20000L * 20001 / 2);
    CHECK(max_seen.load() <= 3);  // bound never exceeded
    int v;
    CHECK(!q.pop(v));             // closed and drained
    CHECK(!q.push(1));
    CHECK_THROWS(BoundedQueue<int>(0));
}

TEST(worker_pool_runs_all_tasks_waits_idle_and_survives_exceptions) {
    WorkerPool pool(4, 8);
    std::atomic<int> done{0};
    std::set<std::thread::id> ids;
    std::mutex m;
    for (int i = 0; i < 1000; ++i)
        pool.submit([&, i] {
            if (i == 500) throw std::runtime_error("boom");
            { std::lock_guard<std::mutex> g(m); ids.insert(std::this_thread::get_id()); }
            ++done;
        });
    pool.wait_idle();
    CHECK_EQ(done.load(), 999);
    CHECK_EQ(pool.failed_tasks(), 1u);
    CHECK(ids.size() >= 1 && ids.size() <= 4);
    pool.shutdown();
    CHECK(!pool.submit([] {}));
    CHECK_THROWS(WorkerPool(0, 1));
}
