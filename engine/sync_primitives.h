// Synchronisation building blocks demonstrated for the OS syllabus:
// counting semaphore (mutex + condition variable), a bounded queue guarded by a mutex and two
// condition variables, the classic semaphore-based bounded buffer (producer-consumer problem),
// a countdown latch, and Peterson's two-thread mutual-exclusion algorithm.
#pragma once
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <stdexcept>

namespace itx {

class Semaphore {
public:
    explicit Semaphore(long initial = 0) : count_(initial) {
        if (initial < 0) throw std::invalid_argument("semaphore count must be >= 0");
    }
    void acquire() {  // P / wait
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [this] { return count_ > 0; });
        --count_;
    }
    bool try_acquire() {
        std::lock_guard<std::mutex> g(m_);
        if (count_ <= 0) return false;
        --count_;
        return true;
    }
    void release() {  // V / signal
        {
            std::lock_guard<std::mutex> g(m_);
            ++count_;
        }
        cv_.notify_one();
    }
    long value() const {
        std::lock_guard<std::mutex> g(m_);
        return count_;
    }

private:
    mutable std::mutex m_;
    std::condition_variable cv_;
    long count_;
};

// Bounded queue using a mutex and two condition variables (not_full / not_empty).
template <class T>
class BoundedQueue {
public:
    explicit BoundedQueue(size_t capacity) : capacity_(capacity) {
        if (capacity == 0) throw std::invalid_argument("capacity must be > 0");
    }
    // Blocks while full. Returns false if the queue was closed.
    bool push(T item) {
        std::unique_lock<std::mutex> lk(m_);
        not_full_.wait(lk, [this] { return closed_ || q_.size() < capacity_; });
        if (closed_) return false;
        q_.push_back(std::move(item));
        lk.unlock();
        not_empty_.notify_one();
        return true;
    }
    // Blocks while empty. Returns false once the queue is closed AND drained.
    bool pop(T& out) {
        std::unique_lock<std::mutex> lk(m_);
        not_empty_.wait(lk, [this] { return closed_ || !q_.empty(); });
        if (q_.empty()) return false;
        out = std::move(q_.front());
        q_.pop_front();
        lk.unlock();
        not_full_.notify_one();
        return true;
    }
    void close() {
        {
            std::lock_guard<std::mutex> g(m_);
            closed_ = true;
        }
        not_full_.notify_all();
        not_empty_.notify_all();
    }
    size_t size() const {
        std::lock_guard<std::mutex> g(m_);
        return q_.size();
    }
    size_t capacity() const { return capacity_; }

private:
    mutable std::mutex m_;
    std::condition_variable not_full_, not_empty_;
    std::deque<T> q_;
    const size_t capacity_;
    bool closed_ = false;
};

// Textbook producer-consumer: semaphores `empty` (free slots), `full` (filled slots) + a mutex.
template <class T>
class SemaphoreBuffer {
public:
    explicit SemaphoreBuffer(size_t capacity)
        : empty_(static_cast<long>(capacity)), full_(0), capacity_(capacity) {}
    void put(T item) {
        empty_.acquire();
        {
            std::lock_guard<std::mutex> g(m_);
            q_.push_back(std::move(item));
        }
        full_.release();
    }
    T take() {
        full_.acquire();
        T item;
        {
            std::lock_guard<std::mutex> g(m_);
            item = std::move(q_.front());
            q_.pop_front();
        }
        empty_.release();
        return item;
    }
    size_t capacity() const { return capacity_; }

private:
    Semaphore empty_, full_;
    std::mutex m_;
    std::deque<T> q_;
    size_t capacity_;
};

class Latch {
public:
    explicit Latch(long count) : count_(count) {}
    void count_down() {
        std::lock_guard<std::mutex> g(m_);
        if (--count_ <= 0) cv_.notify_all();
    }
    void wait() {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [this] { return count_ <= 0; });
    }

private:
    std::mutex m_;
    std::condition_variable cv_;
    long count_;
};

// Peterson's solution for exactly two threads (ids 0 and 1). seq_cst atomics provide the
// ordering that the algorithm assumes on real hardware.
class PetersonLock {
public:
    void lock(int self) {
        int other = 1 - self;
        flag_[self].store(true);
        turn_.store(other);
        while (flag_[other].load() && turn_.load() == other) { /* busy wait */ }
    }
    void unlock(int self) { flag_[self].store(false); }

private:
    std::atomic<bool> flag_[2] = {{false}, {false}};
    std::atomic<int> turn_{0};
};

}  // namespace itx
