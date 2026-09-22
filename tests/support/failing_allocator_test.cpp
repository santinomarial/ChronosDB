#include "support/failing_allocator.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <new>

namespace chronos::test {
namespace {

TEST(FailingAllocatorTest, NothrowAllocationUsesTheMatchingDelete) {
  void* const memory = ::operator new(32U, std::nothrow);
  const bool memory_allocated = memory != nullptr;
  ::operator delete(memory);
  EXPECT_TRUE(memory_allocated);

  void* const array = ::operator new[](32U, std::nothrow);
  const bool array_allocated = array != nullptr;
  ::operator delete[](array);
  EXPECT_TRUE(array_allocated);
}

TEST(FailingAllocatorTest, NothrowAllocationReturnsNullOnInjectedFailure) {
  {
    ScopedAllocationFailure failure{0U};
    EXPECT_EQ(::operator new(32U, std::nothrow), nullptr);
    failure.disable();
    EXPECT_EQ(failure.observed_allocations(), 1U);
  }
  {
    ScopedAllocationFailure failure{0U};
    EXPECT_EQ(::operator new[](32U, std::nothrow), nullptr);
    failure.disable();
    EXPECT_EQ(failure.observed_allocations(), 1U);
  }
}

} // namespace
} // namespace chronos::test
