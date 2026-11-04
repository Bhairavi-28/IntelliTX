#include "worker_pool.h"

namespace itx {

WorkerPool::WorkerPool(size_t workers, size_t queue_capacity) : queue_(queue_capacity) {
    if (workers == 0) throw std::invalid_argument("worker count must be > 0");
    for (size_t i = 0; i < workers; ++i) threads_.emplace_back([this] { worker_loop(); });
}

WorkerPool::~WorkerPool() { shutdown(); }

bool WorkerPool::submit(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> g(idle_mu_);
        ++pending_;
    }
    if (!queue_.push(std::move(task))) {
        std::lock_guard<std::mutex> g(idle_mu_);
        --pending_;
        idle_cv_.notify_all();
        return false;
    }
    return true;
}

void WorkerPool::worker_loop() {
    std::function<void()> task;
    while (queue_.pop(task)) {
        try {
            task();
        } catch (...) {
            failed_.fetch_add(1);
        }
        std::lock_guard<std::mutex> g(idle_mu_);
        if (--pending_ == 0) idle_cv_.notify_all();
    }
}

void WorkerPool::wait_idle() {
    std::unique_lock<std::mutex> lk(idle_mu_);
    idle_cv_.wait(lk, [this] { return pending_ == 0; });
}

void WorkerPool::shutdown() {
    {
        std::lock_guard<std::mutex> g(idle_mu_);
        if (shut_down_) return;
        shut_down_ = true;
    }
    queue_.close();
    for (auto& t : threads_) t.join();
}

}  // namespace itx
