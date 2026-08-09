#include "chronos/runtime/thread_placement.hpp"

#include <gtest/gtest.h>

namespace chronos::runtime {
namespace {

TEST(ThreadPlacementTest, EmptyHookIsPortableAndNumaFailsExplicitlyWithoutProvider) {
  EXPECT_TRUE(apply_current_thread_placement({}).is_ok());
  const auto numa = apply_current_thread_placement({std::nullopt, 0U});
  EXPECT_EQ(numa.code(), common::StatusCode::kNotSupported);
}

} // namespace
} // namespace chronos::runtime
