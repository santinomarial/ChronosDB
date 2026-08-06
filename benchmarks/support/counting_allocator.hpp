#ifndef CHRONOS_BENCHMARKS_SUPPORT_COUNTING_ALLOCATOR_HPP_
#define CHRONOS_BENCHMARKS_SUPPORT_COUNTING_ALLOCATOR_HPP_

#include <cstddef>

namespace chronos::benchmark_support {

struct AllocationCounts {
  std::size_t allocations{};
  std::size_t allocated_bytes{};
};

// Counts regular global new/new[] calls on the current thread. This override is linked only into
// the mutable-head benchmark executable and is disabled during timed iterations.
class ScopedAllocationCounting {
public:
  ScopedAllocationCounting() noexcept;
  ~ScopedAllocationCounting();

  ScopedAllocationCounting(const ScopedAllocationCounting&) = delete;
  ScopedAllocationCounting& operator=(const ScopedAllocationCounting&) = delete;
  ScopedAllocationCounting(ScopedAllocationCounting&&) = delete;
  ScopedAllocationCounting& operator=(ScopedAllocationCounting&&) = delete;

  [[nodiscard]] AllocationCounts stop() noexcept;

private:
  bool active_{true};
};

namespace detail {

void record_allocation(std::size_t size) noexcept;

} // namespace detail
} // namespace chronos::benchmark_support

#endif // CHRONOS_BENCHMARKS_SUPPORT_COUNTING_ALLOCATOR_HPP_
