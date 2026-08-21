#include "chronos/common/status.hpp"
#include "chronos/raft/node.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <utility>

namespace chronos::raft {
namespace {

[[nodiscard]] RaftNode leader_ready_for_read_barrier(const bool joint_membership) {
  auto leader = RaftNode::create(1U, {1U, 2U, 3U});
  EXPECT_TRUE(leader.has_value());
  EXPECT_TRUE(leader->start_election().has_value());
  EXPECT_TRUE(leader->receive(2U, RequestVoteResponse{1U, true}).has_value());
  EXPECT_TRUE(leader->propose(1U, {std::byte{0x42U}}).has_value());
  EXPECT_TRUE(
      leader->receive(2U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U}).has_value());
  if (joint_membership) {
    EXPECT_TRUE(leader->begin_membership_change({3U, 4U, 5U}).has_value());
    EXPECT_TRUE(leader->joint_membership_active());
  }
  return std::move(*leader);
}

[[nodiscard]] std::uint64_t read_context(const Transition& transition) {
  EXPECT_FALSE(transition.outbound.empty());
  const auto* request = std::get_if<ReadBarrierRequest>(&transition.outbound.front().message);
  EXPECT_NE(request, nullptr);
  return request == nullptr ? 0U : request->context;
}

void expect_read_barrier_allocation_atomic(const bool joint_membership,
                                           const std::size_t expected_probe_count) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_ready_for_read_barrier(joint_membership);
    ASSERT_EQ(leader.role(), Role::kLeader);
    const PersistentState before = leader.persistent_state();

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.begin_read_barrier());
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ((**result).outbound.size(), expected_probe_count);
      EXPECT_EQ(read_context(**result), 1U);
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(leader.role(), Role::kLeader);
    EXPECT_EQ(leader.current_term(), 1U);
    EXPECT_EQ(leader.leader_id(), 1U);
    EXPECT_EQ(leader.persistent_state(), before);

    auto retry = leader.begin_read_barrier();
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(retry->outbound.size(), expected_probe_count);
    EXPECT_EQ(read_context(*retry), 1U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     StableReadBarrierIssuancePreservesStateAndContextUntilPublication) {
  expect_read_barrier_allocation_atomic(false, 2U);
}

TEST(RaftNodeAllocationFailureTest,
     JointReadBarrierIssuancePreservesStateAndContextUntilPublication) {
  expect_read_barrier_allocation_atomic(true, 4U);
}

} // namespace
} // namespace chronos::raft
