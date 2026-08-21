#include "chronos/common/status.hpp"
#include "chronos/raft/node.hpp"
#include "support/failing_allocator.hpp"

#include <array>
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

void expect_higher_term_response_transition(const RaftNode& node, const Transition& transition,
                                            PersistentState expected) {
  expected.current_term = 2U;
  expected.voted_for.reset();
  EXPECT_EQ(node.role(), Role::kFollower);
  EXPECT_EQ(node.current_term(), 2U);
  EXPECT_FALSE(node.leader_id().has_value());
  EXPECT_EQ(node.persistent_state(), expected);
  const PersistentState* persistent = returned_persistent_state(transition);
  ASSERT_NE(persistent, nullptr);
  EXPECT_EQ(*persistent, expected);
  EXPECT_TRUE(transition.outbound.empty());
  EXPECT_FALSE(transition.advanced_commit_index.has_value());
  EXPECT_FALSE(transition.snapshot_install.has_value());
  EXPECT_FALSE(transition.read_barrier_ready.has_value());
}

void expect_vote_request_transition(const RaftNode& node, const Transition& transition,
                                    PersistentState expected, const Term term,
                                    const std::optional<NodeId> vote, const bool granted) {
  expected.current_term = term;
  expected.voted_for = vote;
  EXPECT_EQ(node.role(), Role::kFollower);
  EXPECT_EQ(node.current_term(), term);
  EXPECT_FALSE(node.leader_id().has_value());
  EXPECT_EQ(node.persistent_state(), expected);
  const PersistentState* persistent = returned_persistent_state(transition);
  ASSERT_NE(persistent, nullptr);
  EXPECT_EQ(*persistent, expected);
  ASSERT_EQ(transition.outbound.size(), 1U);
  EXPECT_EQ(transition.outbound.front().destination, 2U);
  const auto* response = std::get_if<RequestVoteResponse>(&transition.outbound.front().message);
  ASSERT_NE(response, nullptr);
  EXPECT_EQ(*response, (RequestVoteResponse{term, granted}));
  EXPECT_FALSE(transition.advanced_commit_index.has_value());
  EXPECT_FALSE(transition.snapshot_install.has_value());
  EXPECT_FALSE(transition.read_barrier_ready.has_value());
}

void expect_election_transition(const RaftNode& node, const Transition& transition,
                                PersistentState expected, const Role role,
                                const std::size_t expected_outbound) {
  ++expected.current_term;
  expected.voted_for = 1U;
  EXPECT_EQ(node.role(), role);
  EXPECT_EQ(node.current_term(), expected.current_term);
  EXPECT_EQ(node.leader_id(), role == Role::kLeader ? std::optional<NodeId>{1U} : std::nullopt);
  EXPECT_EQ(node.persistent_state(), expected);
  const PersistentState* persistent = returned_persistent_state(transition);
  ASSERT_NE(persistent, nullptr);
  EXPECT_EQ(*persistent, expected);
  ASSERT_EQ(transition.outbound.size(), expected_outbound);
  for (const OutboundMessage& outbound : transition.outbound) {
    const auto* request = std::get_if<RequestVoteRequest>(&outbound.message);
    ASSERT_NE(request, nullptr);
    EXPECT_EQ(*request,
              (RequestVoteRequest{expected.current_term, 1U, expected.log.empty() ? 0U : 1U,
                                  expected.log.empty() ? 0U : 1U}));
  }
  EXPECT_FALSE(transition.advanced_commit_index.has_value());
  EXPECT_FALSE(transition.snapshot_install.has_value());
  EXPECT_FALSE(transition.read_barrier_ready.has_value());
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

TEST(RaftNodeAllocationFailureTest,
     HigherTermReadBarrierResponsePreparesPersistenceBeforeDemotion) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_ready_for_read_barrier(false);
    auto started = leader.begin_read_barrier();
    ASSERT_TRUE(started.has_value()) << started.error().to_string();
    const std::uint64_t context = read_context(*started);
    const PersistentState before = leader.persistent_state();

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.receive(2U, ReadBarrierResponse{2U, context, true}));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      expect_higher_term_response_transition(leader, **result, before);
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
    auto pending = leader.begin_read_barrier();
    ASSERT_FALSE(pending.has_value());
    EXPECT_EQ(pending.error().code(), common::StatusCode::kUnavailable);

    auto retry = leader.receive(2U, ReadBarrierResponse{2U, context, true});
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    expect_higher_term_response_transition(leader, *retry, before);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest, HigherTermResponsesPreparePersistenceBeforeDemotion) {
  const std::array<Message, 3U> responses{
      RequestVoteResponse{2U, true},
      AppendEntriesResponse{2U, true, 1U, std::nullopt, 0U},
      InstallSnapshotResponse{2U, false, 0U},
  };
  for (const Message& response : responses) {
    SCOPED_TRACE(response.index());
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
        result.emplace(leader.receive(2U, response));
        observed = failure.observed_allocations();
        failure.disable();
      }

      ASSERT_TRUE(result.has_value());
      if (result->has_value()) {
        expect_higher_term_response_transition(leader, **result, before);
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

      auto retry = leader.receive(2U, response);
      ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
      expect_higher_term_response_transition(leader, *retry, before);
    }
    EXPECT_GT(failure_count, 0U);
    EXPECT_TRUE(reached_success);
  }
}

TEST(RaftNodeAllocationFailureTest, HigherTermVoteRequestPreparesGrantOrRejectionBeforeMutation) {
  struct Case {
    RequestVoteRequest request;
    bool granted{};
  };
  const std::array<Case, 2U> cases{
      Case{RequestVoteRequest{2U, 2U, 1U, 1U}, true},
      Case{RequestVoteRequest{2U, 2U, 0U, 0U}, false},
  };
  for (const Case& test_case : cases) {
    SCOPED_TRACE(test_case.granted);
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
        result.emplace(leader.receive(2U, test_case.request));
        observed = failure.observed_allocations();
        failure.disable();
      }

      ASSERT_TRUE(result.has_value());
      if (result->has_value()) {
        expect_vote_request_transition(leader, **result, before, 2U,
                                       test_case.granted ? std::optional<NodeId>{2U} : std::nullopt,
                                       test_case.granted);
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

      auto retry = leader.receive(2U, test_case.request);
      ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
      expect_vote_request_transition(leader, *retry, before, 2U,
                                     test_case.granted ? std::optional<NodeId>{2U} : std::nullopt,
                                     test_case.granted);
    }
    EXPECT_GT(failure_count, 0U);
    EXPECT_TRUE(reached_success);
  }
}

TEST(RaftNodeAllocationFailureTest, SameTermVoteRequestPreparesFirstVoteBeforeMutation) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    PersistentState initial;
    initial.current_term = 1U;
    auto created = RaftNode::create(1U, {1U, 2U, 3U}, initial);
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode follower = std::move(*created);
    const PersistentState before = follower.persistent_state();
    const RequestVoteRequest request{1U, 2U, 0U, 0U};

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(follower.receive(2U, request));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      expect_vote_request_transition(follower, **result, before, 1U, 2U, true);
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(follower.role(), Role::kFollower);
    EXPECT_EQ(follower.current_term(), 1U);
    EXPECT_FALSE(follower.leader_id().has_value());
    EXPECT_EQ(follower.persistent_state(), before);

    auto retry = follower.receive(2U, request);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    expect_vote_request_transition(follower, *retry, before, 1U, 2U, true);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest, MultiVoterElectionStartPreservesStateUntilPublication) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_ready_for_read_barrier(false);
    auto barrier = leader.begin_read_barrier();
    ASSERT_TRUE(barrier.has_value()) << barrier.error().to_string();
    const std::uint64_t context = read_context(*barrier);
    const PersistentState before = leader.persistent_state();

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.start_election());
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      expect_election_transition(leader, **result, before, Role::kCandidate, 2U);
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
    auto completed = leader.receive(2U, ReadBarrierResponse{1U, context, true});
    ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
    ASSERT_NE(completed_read_barrier(*completed), nullptr);

    auto retry = leader.start_election();
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    expect_election_transition(leader, *retry, before, Role::kCandidate, 2U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest, SingleVoterElectionStartPreservesStateUntilLeadership) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U});
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode node = std::move(*created);
    const PersistentState before = node.persistent_state();

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(node.start_election());
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      expect_election_transition(node, **result, before, Role::kLeader, 0U);
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(node.role(), Role::kFollower);
    EXPECT_EQ(node.current_term(), 0U);
    EXPECT_FALSE(node.leader_id().has_value());
    EXPECT_EQ(node.persistent_state(), before);

    auto retry = node.start_election();
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    expect_election_transition(node, *retry, before, Role::kLeader, 0U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::raft
