#include "support/counting_allocator.hpp"

#include <cstdlib>
#include <new>

namespace {

thread_local bool allocation_counting_enabled = false;
thread_local chronos::benchmark_support::AllocationCounts allocation_counts;

[[nodiscard]] void* allocate(const std::size_t requested_size) {
  const std::size_t size = requested_size == 0U ? 1U : requested_size;
  void* const memory = std::malloc(size);
  if (memory == nullptr) {
    throw std::bad_alloc{};
  }
  chronos::benchmark_support::detail::record_allocation(size);
  return memory;
}

} // namespace

// NOLINTNEXTLINE(readability-inconsistent-declaration-parameter-name)
void* operator new(const std::size_t size) {
  return allocate(size);
}

// NOLINTNEXTLINE(readability-inconsistent-declaration-parameter-name)
void* operator new[](const std::size_t size) {
  return allocate(size);
}

// NOLINTNEXTLINE(readability-inconsistent-declaration-parameter-name)
void operator delete(void* const memory) noexcept {
  std::free(memory);
}

// NOLINTNEXTLINE(readability-inconsistent-declaration-parameter-name)
void operator delete[](void* const memory) noexcept {
  std::free(memory);
}

// NOLINTNEXTLINE(readability-inconsistent-declaration-parameter-name)
void operator delete(void* const memory, const std::size_t size) noexcept {
  static_cast<void>(size);
  std::free(memory);
}

// NOLINTNEXTLINE(readability-inconsistent-declaration-parameter-name)
void operator delete[](void* const memory, const std::size_t size) noexcept {
  static_cast<void>(size);
  std::free(memory);
}

namespace chronos::benchmark_support {

ScopedAllocationCounting::ScopedAllocationCounting() noexcept {
  allocation_counts = {};
  allocation_counting_enabled = true;
}

ScopedAllocationCounting::~ScopedAllocationCounting() {
  static_cast<void>(stop());
}

AllocationCounts ScopedAllocationCounting::stop() noexcept {
  if (active_) {
    allocation_counting_enabled = false;
    active_ = false;
  }
  return allocation_counts;
}

namespace detail {

void record_allocation(const std::size_t size) noexcept {
  if (allocation_counting_enabled) {
    ++allocation_counts.allocations;
    allocation_counts.allocated_bytes += size;
  }
}

} // namespace detail
} // namespace chronos::benchmark_support
