// Fixed-size worker thread pool fed by a bounded task queue (producer-consumer).
// submit() blocks when the queue is full, which gives natural back-pressure.
#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "sync_primitives.h"

namespace itx {

class WorkerPool {
public:
    WorkerPool(size_t workers, size_t queue_capacity);
    ~WorkerPool();
    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    // Returns false if the pool has been shut down.
    bool submit(std::function<void()> task);
    void wait_idle();  // blocks until every submitted task has finished
    void shutdown();   // drains queued tasks, then joins the workers (idempotent)
    size_t worker_count() const { return threads_.size(); }
    uint64_t failed_tasks() const { return failed_.load(); }  // tasks that threw

private:
    void worker_loop();

    BoundedQueue<std::function<void()>> queue_;
    std::vector<std::thread> threads_;
    std::mutex idle_mu_;
    std::condition_variable idle_cv_;
    size_t pending_ = 0;
    std::atomic<uint64_t> failed_{0};
    bool shut_down_ = false;
};

}  // namespace itx
