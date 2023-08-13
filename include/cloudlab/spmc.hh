#ifndef CLOUDLAB_SPMC_HH
#define CLOUDLAB_SPMC_HH

#include <condition_variable>
#include <deque>
#include <vector>

namespace cloudlab {

/**
 * A single-producer multiple-consumer queue. Used to distribute connections
 * onto server worker threads.
 *
 * @tparam T    Type of data stored in the queue
 */
template <typename T>
class SPMCQueue {
 public:
  SPMCQueue() = default;

  auto produce(T val) -> void {
    std::lock_guard<std::mutex> lock_guard(mtx);
    q.push_back(val);
    cond.notify_one();
  }

  auto consume() -> T {
    std::unique_lock<std::mutex> lock_guard(mtx);
    cond.wait(lock_guard, [this] { return !q.empty(); });
    auto tmp = q.front();
    q.pop_front();
    return tmp;
  }

 private:
  std::deque<T> q{};
  std::mutex mtx{};
  std::condition_variable cond{};
};

}  // namespace cloudlab

#endif  // CLOUDLAB_SPMC_HH
