#ifndef CHRONOS_TESTS_SUPPORT_FAILING_ALLOCATOR_HPP_
#define CHRONOS_TESTS_SUPPORT_FAILING_ALLOCATOR_HPP_

#include <cstddef>

namespace chronos::test {

// Arms the test executable's global allocator on the current thread only. The zero-based selected
// allocation throws std::bad_alloc once, after which allocation resumes normally so production
// error construction and rollback can complete. This seam is linked only into the dedicated
// allocation-failure test executable and never into a ChronosDB library.
class ScopedAllocationFailure {
public:
  explicit ScopedAllocationFailure(std::size_t fail_after) noexcept;
  ~ScopedAllocationFailure();

  ScopedAllocationFailure(const ScopedAllocationFailure&) = delete;
  ScopedAllocationFailure& operator=(const ScopedAllocationFailure&) = delete;
  ScopedAllocationFailure(ScopedAllocationFailure&&) = delete;
  ScopedAllocationFailure& operator=(ScopedAllocationFailure&&) = delete;

  [[nodiscard]] std::size_t observed_allocations() const noexcept;
  void disable() noexcept;

private:
  bool active_{true};
};

namespace detail {

[[nodiscard]] bool fail_test_allocation() noexcept;

} // namespace detail
} // namespace chronos::test

#endif // CHRONOS_TESTS_SUPPORT_FAILING_ALLOCATOR_HPP_
