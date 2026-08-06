#include "support/failing_allocator.hpp"

#include <cstdlib>
#include <new>

namespace {

thread_local bool allocation_failure_enabled = false;
thread_local std::size_t allocations_before_failure = 0U;
thread_local std::size_t test_observed_allocations = 0U;

[[nodiscard]] void* allocate(const std::size_t requested_size) {
  if (chronos::test::detail::fail_test_allocation()) {
    throw std::bad_alloc{};
  }
  const std::size_t size = requested_size == 0U ? 1U : requested_size;
  void* const memory = std::malloc(size);
  if (memory == nullptr) {
    throw std::bad_alloc{};
  }
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

namespace chronos::test {

ScopedAllocationFailure::ScopedAllocationFailure(const std::size_t fail_after) noexcept {
  allocation_failure_enabled = true;
  allocations_before_failure = fail_after;
  test_observed_allocations = 0U;
}

ScopedAllocationFailure::~ScopedAllocationFailure() {
  disable();
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::size_t ScopedAllocationFailure::observed_allocations() const noexcept {
  return test_observed_allocations;
}

void ScopedAllocationFailure::disable() noexcept {
  if (active_) {
    allocation_failure_enabled = false;
    active_ = false;
  }
}

namespace detail {

bool fail_test_allocation() noexcept {
  if (!allocation_failure_enabled) {
    return false;
  }
  ++test_observed_allocations;
  if (allocations_before_failure == 0U) {
    allocation_failure_enabled = false;
    return true;
  }
  --allocations_before_failure;
  return false;
}

} // namespace detail
} // namespace chronos::test
