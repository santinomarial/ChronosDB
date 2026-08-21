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

[[nodiscard]] const ReadBarrier* completed_read_barrier(const Transition& transition) {
  const std::optional<ReadBarrier>& barrier = transition.read_barrier_ready;
  if (!barrier.has_value())
    return nullptr;
  return &barrier.value();
}

[[nodiscard]] const PersistentState* returned_persistent_state(const Transition& transition) {
  const std::optional<PersistentState>& persistent = transition.persistent_state;
  if (!persistent.has_value())
    return nullptr;
  return &persistent.value();
}

void expect_higher_term_read_probe_transition(const RaftNode& node, const Transition& transition,
                                              PersistentState expected) {
  expected.current_term = 2U;
  expected.voted_for.reset();
  EXPECT_EQ(node.role(), Role::kFollower);
  EXPECT_EQ(node.current_term(), 2U);
  EXPECT_EQ(node.leader_id(), 2U);
  EXPECT_EQ(node.persistent_state(), expected);
  const PersistentState* persistent = returned_persistent_state(transition);
  ASSERT_NE(persistent, nullptr);
  EXPECT_EQ(*persistent, expected);
  ASSERT_EQ(transition.outbound.size(), 1U);
  EXPECT_EQ(transition.outbound.front().destination, 2U);
  const auto* response = std::get_if<ReadBarrierResponse>(&transition.outbound.front().message);
  ASSERT_NE(response, nullptr);
  EXPECT_EQ(*response, (ReadBarrierResponse{2U, 9U, true}));
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

TEST(RaftNodeAllocationFailureTest,
     JointReadBarrierAcknowledgementPreservesQuorumStateUntilPublication) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_ready_for_read_barrier(true);
    auto started = leader.begin_read_barrier();
    ASSERT_TRUE(started.has_value()) << started.error().to_string();
    const std::uint64_t context = read_context(*started);
    const PersistentState before = leader.persistent_state();

    std::optional<common::Result<Transition>> response;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      response.emplace(leader.receive(4U, ReadBarrierResponse{1U, context, true}));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(response.has_value());
    if (response->has_value()) {
      EXPECT_FALSE((**response).read_barrier_ready.has_value());
      auto completed = leader.receive(3U, ReadBarrierResponse{1U, context, true});
      ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
      const ReadBarrier* barrier = completed_read_barrier(*completed);
      ASSERT_NE(barrier, nullptr);
      EXPECT_EQ(barrier->context, context);
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(response->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(leader.role(), Role::kLeader);
    EXPECT_EQ(leader.current_term(), 1U);
    EXPECT_EQ(leader.leader_id(), 1U);
    EXPECT_EQ(leader.persistent_state(), before);

    auto old_quorum_only = leader.receive(3U, ReadBarrierResponse{1U, context, true});
    ASSERT_TRUE(old_quorum_only.has_value()) << old_quorum_only.error().to_string();
    EXPECT_FALSE(old_quorum_only->read_barrier_ready.has_value());
    auto retry = leader.receive(4U, ReadBarrierResponse{1U, context, true});
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    const ReadBarrier* barrier = completed_read_barrier(*retry);
    ASSERT_NE(barrier, nullptr);
    EXPECT_EQ(barrier->context, context);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     HigherTermReadBarrierRequestPreparesResponseBeforeTermObservation) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_ready_for_read_barrier(false);
    const PersistentState before = leader.persistent_state();

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.receive(2U, ReadBarrierRequest{2U, 2U, 9U}));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      expect_higher_term_read_probe_transition(leader, **result, before);
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

    auto retry = leader.receive(2U, ReadBarrierRequest{2U, 2U, 9U});
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    expect_higher_term_read_probe_transition(leader, *retry, before);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::raft
