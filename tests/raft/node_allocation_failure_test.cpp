#include "chronos/common/status.hpp"
#include "chronos/raft/membership.hpp"
#include "chronos/raft/node.hpp"
#include "support/failing_allocator.hpp"

#include <algorithm>
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

[[nodiscard]] RaftNode leader_with_two_pending_entries() {
  RaftLimits limits;
  limits.maximum_append_entries = 1U;
  auto leader = RaftNode::create(1U, {1U, 2U, 3U, 4U, 5U}, {}, limits);
  EXPECT_TRUE(leader.has_value());
  EXPECT_TRUE(leader->start_election().has_value());
  EXPECT_TRUE(leader->receive(4U, RequestVoteResponse{1U, true}).has_value());
  EXPECT_TRUE(leader->receive(5U, RequestVoteResponse{1U, true}).has_value());
  EXPECT_EQ(leader->role(), Role::kLeader);
  EXPECT_TRUE(leader->propose(1U, {std::byte{0x41U}}).has_value());
  EXPECT_TRUE(leader->propose(1U, {std::byte{0x42U}}).has_value());
  return std::move(*leader);
}

[[nodiscard]] RaftNode leader_with_compacted_snapshot() {
  auto leader = RaftNode::create(1U, {1U, 2U, 3U});
  EXPECT_TRUE(leader.has_value());
  EXPECT_TRUE(leader->start_election().has_value());
  EXPECT_TRUE(leader->receive(3U, RequestVoteResponse{1U, true}).has_value());
  for (std::uint8_t value = 1U; value <= 3U; ++value) {
    EXPECT_TRUE(leader->propose(1U, {static_cast<std::byte>(value)}).has_value());
    EXPECT_TRUE(
        leader->receive(3U, AppendEntriesResponse{1U, true, value, std::nullopt, 0U}).has_value());
  }
  EXPECT_TRUE(leader->mark_applied(3U).has_value());
  SnapshotMetadata snapshot{};
  snapshot.last_included_index = 2U;
  snapshot.last_included_term = 1U;
  snapshot.manifest_generation = 7U;
  EXPECT_TRUE(leader->compact_snapshot(snapshot).has_value());
  return std::move(*leader);
}

[[nodiscard]] RaftNode leader_with_committed_joint_membership() {
  auto leader = RaftNode::create(1U, {1U, 2U, 3U});
  EXPECT_TRUE(leader.has_value());
  EXPECT_TRUE(leader->start_election().has_value());
  EXPECT_TRUE(leader->receive(2U, RequestVoteResponse{1U, true}).has_value());
  EXPECT_TRUE(leader->begin_membership_change({2U, 3U, 4U}).has_value());
  EXPECT_TRUE(
      leader->receive(2U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U}).has_value());
  EXPECT_TRUE(
      leader->receive(4U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U}).has_value());
  EXPECT_EQ(leader->commit_index(), 1U);
  EXPECT_TRUE(leader->joint_membership_can_finalize());
  return std::move(*leader);
}

[[nodiscard]] RaftNode leader_awaiting_removing_final_acknowledgement() {
  RaftNode leader = leader_with_committed_joint_membership();
  EXPECT_TRUE(leader.finalize_membership_change().has_value());
  EXPECT_TRUE(
      leader.receive(2U, AppendEntriesResponse{1U, true, 2U, std::nullopt, 0U}).has_value());
  EXPECT_EQ(leader.commit_index(), 1U);
  EXPECT_TRUE(leader.joint_membership_active());
  EXPECT_TRUE(leader.final_membership_pending());
  return leader;
}

[[nodiscard]] SnapshotMetadata incoming_snapshot_metadata() {
  SnapshotMetadata snapshot{};
  snapshot.last_included_index = 1U;
  snapshot.last_included_term = 1U;
  snapshot.manifest_generation = 9U;
  snapshot.part_set_checksum.fill(std::byte{0x91U});
  snapshot.voters = {1U, 2U, 3U};
  return snapshot;
}

[[nodiscard]] const AppendEntriesRequest* append_request_for(const Transition& transition,
                                                             const NodeId destination) {
  const auto found =
      std::ranges::find(transition.outbound, destination, &OutboundMessage::destination);
  if (found == transition.outbound.end())
    return nullptr;
  return std::get_if<AppendEntriesRequest>(&found->message);
}

[[nodiscard]] const InstallSnapshotRequest* snapshot_request_for(const Transition& transition,
                                                                 const NodeId destination) {
  const auto found =
      std::ranges::find(transition.outbound, destination, &OutboundMessage::destination);
  if (found == transition.outbound.end())
    return nullptr;
  return std::get_if<InstallSnapshotRequest>(&found->message);
}

struct ExpectedAppend {
  LogIndex previous{};
  LogIndex entry_index{};
  LogIndex commit_index{};
};

void expect_single_append(const Transition& transition, const NodeId destination,
                          const ExpectedAppend expected) {
  ASSERT_EQ(transition.outbound.size(), 1U);
  const AppendEntriesRequest* request = append_request_for(transition, destination);
  ASSERT_NE(request, nullptr);
  EXPECT_EQ(request->previous_log_index, expected.previous);
  ASSERT_EQ(request->entries.size(), 1U);
  EXPECT_EQ(request->entries.front().index, expected.entry_index);
  EXPECT_EQ(request->leader_commit, expected.commit_index);
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

[[nodiscard]] const PendingSnapshotInstall* pending_snapshot_install(const Transition& transition) {
  const std::optional<PendingSnapshotInstall>& pending = transition.snapshot_install;
  if (!pending.has_value())
    return nullptr;
  return &pending.value();
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

void expect_append_response_to(const Transition& transition, const NodeId destination,
                               const AppendEntriesResponse& expected) {
  ASSERT_EQ(transition.outbound.size(), 1U);
  EXPECT_EQ(transition.outbound.front().destination, destination);
  const auto* response = std::get_if<AppendEntriesResponse>(&transition.outbound.front().message);
  ASSERT_NE(response, nullptr);
  EXPECT_EQ(*response, expected);
}

void expect_append_response(const Transition& transition, const Term term, const bool success,
                            const LogIndex match_index, const std::optional<Term> conflict_term,
                            const LogIndex conflict_index) {
  expect_append_response_to(
      transition, 2U,
      AppendEntriesResponse{term, success, match_index, conflict_term, conflict_index});
}

void expect_snapshot_response_to(const Transition& transition, const NodeId destination,
                                 const InstallSnapshotResponse& expected) {
  ASSERT_EQ(transition.outbound.size(), 1U);
  EXPECT_EQ(transition.outbound.front().destination, destination);
  const auto* response = std::get_if<InstallSnapshotResponse>(&transition.outbound.front().message);
  ASSERT_NE(response, nullptr);
  EXPECT_EQ(*response, expected);
}

void expect_snapshot_response(const Transition& transition, const Term term, const bool success,
                              const LogIndex last_included_index) {
  expect_snapshot_response_to(transition, 2U,
                              InstallSnapshotResponse{term, success, last_included_index});
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

void expect_vote_quorum_transition(const RaftNode& node, const Transition& transition,
                                   const PersistentState& expected) {
  EXPECT_EQ(node.role(), Role::kLeader);
  EXPECT_EQ(node.current_term(), 1U);
  EXPECT_EQ(node.leader_id(), 1U);
  EXPECT_EQ(node.persistent_state(), expected);
  EXPECT_FALSE(transition.persistent_state.has_value());
  ASSERT_EQ(transition.outbound.size(), 4U);
  std::array<bool, 6U> seen_destination{};
  for (const OutboundMessage& outbound : transition.outbound) {
    ASSERT_GE(outbound.destination, 2U);
    ASSERT_LE(outbound.destination, 5U);
    EXPECT_FALSE(seen_destination[outbound.destination]);
    seen_destination[outbound.destination] = true;
    const auto* request = std::get_if<AppendEntriesRequest>(&outbound.message);
    ASSERT_NE(request, nullptr);
    EXPECT_EQ(*request, (AppendEntriesRequest{1U, 1U, 0U, 0U, {}, 0U}));
  }
  EXPECT_TRUE(std::ranges::all_of(seen_destination.begin() + 2, seen_destination.end(),
                                  [](const bool seen) { return seen; }));
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

void expect_snapshot_response_allocation_atomic(const bool installed) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_with_compacted_snapshot();
    ASSERT_EQ(leader.role(), Role::kLeader);
    ASSERT_EQ(leader.commit_index(), 3U);
    const PersistentState before = leader.persistent_state();
    const InstallSnapshotResponse response{1U, installed, installed ? 2U : 0U};

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
      if (installed) {
        expect_single_append(**result, 2U, ExpectedAppend{2U, 3U, 3U});
      } else {
        ASSERT_EQ((**result).outbound.size(), 1U);
        const InstallSnapshotRequest* retry = snapshot_request_for(**result, 2U);
        ASSERT_NE(retry, nullptr);
        EXPECT_EQ(retry->snapshot.last_included_index, 2U);
      }
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

    auto heartbeat = leader.heartbeat();
    ASSERT_TRUE(heartbeat.has_value()) << heartbeat.error().to_string();
    const InstallSnapshotRequest* unchanged = snapshot_request_for(*heartbeat, 2U);
    ASSERT_NE(unchanged, nullptr);
    EXPECT_EQ(unchanged->snapshot.last_included_index, 2U);

    auto retry = leader.receive(2U, response);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    if (installed) {
      expect_single_append(*retry, 2U, ExpectedAppend{2U, 3U, 3U});
    } else {
      ASSERT_EQ(retry->outbound.size(), 1U);
      const InstallSnapshotRequest* snapshot_retry = snapshot_request_for(*retry, 2U);
      ASSERT_NE(snapshot_retry, nullptr);
      EXPECT_EQ(snapshot_retry->snapshot.last_included_index, 2U);
    }
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

void expect_competing_snapshot_request_allocation_atomic(const bool higher_term) {
  const SnapshotMetadata snapshot = incoming_snapshot_metadata();
  SnapshotMetadata competitor = snapshot;
  competitor.last_included_index = 2U;
  competitor.last_included_term = 2U;
  competitor.manifest_generation = 10U;
  competitor.part_set_checksum.fill(std::byte{0xA2U});
  PersistentState initial{};
  initial.current_term = 2U;
  initial.voted_for = 2U;
  const Term competitor_term = higher_term ? 3U : 2U;
  const NodeId competitor_source = higher_term ? 3U : 2U;
  const InstallSnapshotRequest competing_request{competitor_term, competitor_source, competitor};
  PersistentState expected = initial;
  if (higher_term) {
    expected.current_term = competitor_term;
    expected.voted_for.reset();
  }
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U, 2U, 3U}, initial);
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode follower = std::move(*created);
    const InstallSnapshotRequest original_request{2U, 2U, snapshot};
    auto pending = follower.receive(2U, original_request);
    ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
    ASSERT_TRUE(pending->snapshot_install.has_value());
    ASSERT_EQ(follower.leader_id(), 2U);
    Message inbound = competing_request;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(follower.receive(competitor_source, std::move(inbound)));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(follower.role(), Role::kFollower);
      EXPECT_EQ(follower.leader_id(), competitor_source);
      EXPECT_EQ(follower.persistent_state(), expected);
      if (higher_term)
        EXPECT_EQ((**result).persistent_state, std::optional<PersistentState>{expected});
      else
        EXPECT_FALSE((**result).persistent_state.has_value());
      EXPECT_FALSE((**result).snapshot_install.has_value());
      expect_snapshot_response_to(
          **result, competitor_source,
          InstallSnapshotResponse{competitor_term, false, initial.snapshot.last_included_index});
      auto completed = follower.complete_snapshot_install(2U, snapshot, false);
      ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
      expect_snapshot_response(*completed, competitor_term, false, 0U);
      auto duplicate = follower.complete_snapshot_install(2U, snapshot, false);
      ASSERT_FALSE(duplicate.has_value());
      EXPECT_EQ(duplicate.error().code(), common::StatusCode::kInvalidArgument);
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(follower.role(), Role::kFollower);
    EXPECT_EQ(follower.leader_id(), 2U);
    EXPECT_EQ(follower.persistent_state(), initial);
    auto duplicate = follower.receive(2U, original_request);
    ASSERT_TRUE(duplicate.has_value()) << duplicate.error().to_string();
    EXPECT_FALSE(duplicate->persistent_state.has_value());
    EXPECT_FALSE(duplicate->snapshot_install.has_value());
    EXPECT_TRUE(duplicate->outbound.empty());

    auto retry = follower.receive(competitor_source, competing_request);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(follower.leader_id(), competitor_source);
    EXPECT_EQ(follower.persistent_state(), expected);
    if (higher_term)
      EXPECT_EQ(retry->persistent_state, std::optional<PersistentState>{expected});
    else
      EXPECT_FALSE(retry->persistent_state.has_value());
    EXPECT_FALSE(retry->snapshot_install.has_value());
    expect_snapshot_response_to(
        *retry, competitor_source,
        InstallSnapshotResponse{competitor_term, false, initial.snapshot.last_included_index});
    auto completed = follower.complete_snapshot_install(2U, snapshot, false);
    ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
    expect_snapshot_response(*completed, competitor_term, false, 0U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

void expect_current_term_progress_allocation_atomic(const std::vector<NodeId>& voters,
                                                    const bool immediate_commit) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, voters);
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode leader = std::move(*created);
    ASSERT_TRUE(leader.start_election().has_value());
    if (voters.size() > 1U)
      ASSERT_TRUE(leader.receive(voters[1U], RequestVoteResponse{1U, true}).has_value());
    ASSERT_EQ(leader.role(), Role::kLeader);
    const PersistentState before = leader.persistent_state();
    PersistentState expected = before;
    expected.log = {LogEntry{1U, 1U, kLeaderNoopEntryType, {}}};
    if (immediate_commit)
      expected.commit_index = 1U;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.commit_current_term());
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(leader.persistent_state(), expected);
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      EXPECT_EQ((**result).advanced_commit_index,
                immediate_commit ? std::optional<LogIndex>{1U} : std::nullopt);
      ASSERT_EQ((**result).outbound.size(), voters.size() - 1U);
      for (const OutboundMessage& outbound : (**result).outbound) {
        const auto* request = std::get_if<AppendEntriesRequest>(&outbound.message);
        ASSERT_NE(request, nullptr);
        EXPECT_EQ(request->previous_log_index, 0U);
        ASSERT_EQ(request->entries.size(), 1U);
        EXPECT_EQ(request->entries.front(), expected.log.front());
        EXPECT_EQ(request->leader_commit, expected.commit_index);
      }
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(leader.role(), Role::kLeader);
    EXPECT_EQ(leader.leader_id(), 1U);
    EXPECT_EQ(leader.persistent_state(), before);
    auto heartbeat = leader.heartbeat();
    ASSERT_TRUE(heartbeat.has_value()) << heartbeat.error().to_string();
    ASSERT_EQ(heartbeat->outbound.size(), voters.size() - 1U);
    for (const OutboundMessage& outbound : heartbeat->outbound) {
      const auto* request = std::get_if<AppendEntriesRequest>(&outbound.message);
      ASSERT_NE(request, nullptr);
      EXPECT_TRUE(request->entries.empty());
      EXPECT_EQ(request->previous_log_index, 0U);
    }

    auto retry = leader.commit_current_term();
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(leader.persistent_state(), expected);
    const PersistentState* persistent = returned_persistent_state(*retry);
    ASSERT_NE(persistent, nullptr);
    EXPECT_EQ(*persistent, expected);
    EXPECT_EQ(retry->advanced_commit_index,
              immediate_commit ? std::optional<LogIndex>{1U} : std::nullopt);
    EXPECT_EQ(retry->outbound.size(), voters.size() - 1U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

[[nodiscard]] RaftNode prior_term_membership_retry_leader(const bool final_pending) {
  auto leader = RaftNode::create(1U, {1U, 2U, 3U});
  EXPECT_TRUE(leader.has_value());
  EXPECT_TRUE(leader->start_election().has_value());
  EXPECT_TRUE(leader->receive(2U, RequestVoteResponse{1U, true}).has_value());
  EXPECT_TRUE(leader->begin_membership_change({2U, 3U, 4U}).has_value());
  if (final_pending) {
    EXPECT_TRUE(
        leader->receive(2U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U}).has_value());
    EXPECT_TRUE(
        leader->receive(4U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U}).has_value());
    EXPECT_EQ(leader->commit_index(), 1U);
    EXPECT_TRUE(leader->finalize_membership_change().has_value());
  }

  auto restarted = RaftNode::create(1U, {1U, 2U, 3U}, leader->persistent_state());
  EXPECT_TRUE(restarted.has_value());
  EXPECT_TRUE(restarted->start_election().has_value());
  EXPECT_TRUE(restarted->receive(2U, RequestVoteResponse{2U, true}).has_value());
  EXPECT_TRUE(restarted->receive(4U, RequestVoteResponse{2U, true}).has_value());
  EXPECT_EQ(restarted->role(), Role::kLeader);
  EXPECT_TRUE(restarted->joint_membership_active());
  EXPECT_EQ(restarted->final_membership_pending(), final_pending);
  return std::move(*restarted);
}

void expect_prior_term_membership_retry_progress_allocation_atomic(const bool final_pending) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = prior_term_membership_retry_leader(final_pending);
    const PersistentState before = leader.persistent_state();
    PersistentState expected = before;
    expected.log.push_back(LogEntry{leader.last_log_index() + 1U, 2U, kLeaderNoopEntryType, {}});
    std::vector<NodeId> new_voters{2U, 3U, 4U};

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      if (final_pending)
        result.emplace(leader.finalize_membership_change());
      else
        result.emplace(leader.begin_membership_change(std::move(new_voters)));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(leader.persistent_state(), expected);
      EXPECT_EQ(leader.role(), Role::kLeader);
      EXPECT_TRUE(leader.joint_membership_active());
      EXPECT_EQ(leader.final_membership_pending(), final_pending);
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      EXPECT_FALSE((**result).advanced_commit_index.has_value());
      ASSERT_EQ((**result).outbound.size(), 3U);
      for (const OutboundMessage& outbound : (**result).outbound) {
        const auto* request = std::get_if<AppendEntriesRequest>(&outbound.message);
        ASSERT_NE(request, nullptr);
        EXPECT_EQ(request->previous_log_index, before.log.back().index);
        ASSERT_EQ(request->entries.size(), 1U);
        EXPECT_EQ(request->entries.front(), expected.log.back());
        EXPECT_EQ(request->leader_commit, before.commit_index);
      }
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(leader.role(), Role::kLeader);
    EXPECT_EQ(leader.leader_id(), 1U);
    EXPECT_EQ(leader.persistent_state(), before);
    EXPECT_TRUE(leader.joint_membership_active());
    EXPECT_EQ(leader.final_membership_pending(), final_pending);
    EXPECT_TRUE(std::ranges::equal(leader.voters(), std::vector<NodeId>{1U, 2U, 3U, 4U}));

    auto heartbeat = leader.heartbeat();
    ASSERT_TRUE(heartbeat.has_value()) << heartbeat.error().to_string();
    ASSERT_EQ(heartbeat->outbound.size(), 3U);
    for (const OutboundMessage& outbound : heartbeat->outbound) {
      const auto* request = std::get_if<AppendEntriesRequest>(&outbound.message);
      ASSERT_NE(request, nullptr);
      EXPECT_EQ(request->previous_log_index, before.log.back().index);
      EXPECT_TRUE(request->entries.empty());
      EXPECT_EQ(request->leader_commit, before.commit_index);
    }

    auto retry = final_pending ? leader.finalize_membership_change()
                               : leader.begin_membership_change({2U, 3U, 4U});
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(leader.persistent_state(), expected);
    EXPECT_EQ(retry->outbound.size(), 3U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

void expect_membership_append_request_allocation_atomic(const bool final_commit) {
  auto joint_payload = encode_membership_command_v1(
      JointMembershipCommand{.old_voters = {1U, 2U, 3U}, .new_voters = {3U, 4U, 5U}});
  auto final_payload = encode_membership_command_v1(
      FinalMembershipCommand{.joint_index = 1U, .new_voters = {3U, 4U, 5U}});
  ASSERT_TRUE(joint_payload.has_value()) << joint_payload.error().to_string();
  ASSERT_TRUE(final_payload.has_value()) << final_payload.error().to_string();
  const LogEntry joint_entry{1U, 2U, kJointMembershipEntryType, *joint_payload};
  const LogEntry final_entry{2U, 2U, kFinalMembershipEntryType, *final_payload};
  const AppendEntriesRequest joint_request{2U, 4U, 0U, 0U, {joint_entry}, 0U};
  const AppendEntriesRequest committed_joint_request{2U, 4U, 0U, 0U, {joint_entry}, 1U};
  const AppendEntriesRequest final_request{2U, 4U, 1U, 2U, {final_entry}, 2U};

  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(3U, {1U, 2U, 3U});
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode follower = std::move(*created);
    if (final_commit) {
      auto joint = follower.receive(4U, committed_joint_request);
      ASSERT_TRUE(joint.has_value()) << joint.error().to_string();
      ASSERT_EQ(follower.commit_index(), 1U);
      ASSERT_TRUE(follower.joint_membership_active());
    }
    const AppendEntriesRequest& request = final_commit ? final_request : joint_request;
    const PersistentState before = follower.persistent_state();
    const std::optional<NodeId> leader_before = follower.leader_id();
    const std::vector<NodeId> voters_before(follower.voters().begin(), follower.voters().end());
    const std::vector<NodeId> committed_before(follower.committed_voters().begin(),
                                               follower.committed_voters().end());
    const bool joint_before = follower.joint_membership_active();
    const bool final_before = follower.final_membership_pending();
    PersistentState expected = before;
    if (final_commit) {
      expected.log.push_back(final_entry);
      expected.commit_index = 2U;
    } else {
      expected.current_term = 2U;
      expected.voted_for.reset();
      expected.log = {joint_entry};
    }
    Message inbound = request;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(follower.receive(4U, std::move(inbound)));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(follower.role(), Role::kFollower);
      EXPECT_EQ(follower.leader_id(), 4U);
      EXPECT_EQ(follower.persistent_state(), expected);
      EXPECT_EQ(follower.joint_membership_active(), !final_commit);
      EXPECT_FALSE(follower.final_membership_pending());
      const std::vector<NodeId> published_voters =
          final_commit ? std::vector<NodeId>{3U, 4U, 5U} : std::vector<NodeId>{1U, 2U, 3U, 4U, 5U};
      EXPECT_TRUE(std::ranges::equal(follower.voters(), published_voters));
      EXPECT_TRUE(std::ranges::equal(follower.committed_voters(),
                                     final_commit ? std::vector<NodeId>{3U, 4U, 5U}
                                                  : std::vector<NodeId>{1U, 2U, 3U}));
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      EXPECT_EQ((**result).advanced_commit_index,
                final_commit ? std::optional<LogIndex>{2U} : std::nullopt);
      expect_append_response_to(
          **result, 4U, AppendEntriesResponse{2U, true, final_commit ? 2U : 1U, std::nullopt, 0U});
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(follower.role(), Role::kFollower);
    EXPECT_EQ(follower.leader_id(), leader_before);
    EXPECT_EQ(follower.persistent_state(), before);
    EXPECT_EQ(follower.joint_membership_active(), joint_before);
    EXPECT_EQ(follower.final_membership_pending(), final_before);
    EXPECT_TRUE(std::ranges::equal(follower.voters(), voters_before));
    EXPECT_TRUE(std::ranges::equal(follower.committed_voters(), committed_before));

    auto retry = follower.receive(4U, request);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(follower.persistent_state(), expected);
    EXPECT_EQ(follower.joint_membership_active(), !final_commit);
    EXPECT_EQ(retry->advanced_commit_index,
              final_commit ? std::optional<LogIndex>{2U} : std::nullopt);
    expect_append_response_to(
        *retry, 4U, AppendEntriesResponse{2U, true, final_commit ? 2U : 1U, std::nullopt, 0U});
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
     VoteResponsePreservesCandidateQuorumUntilLeadershipPublication) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U, 2U, 3U, 4U, 5U});
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode node = std::move(*created);
    auto election = node.start_election();
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    ASSERT_EQ(node.role(), Role::kCandidate);
    const PersistentState before = node.persistent_state();

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(node.receive(2U, RequestVoteResponse{1U, true}));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(node.role(), Role::kCandidate);
      EXPECT_TRUE((**result).outbound.empty());
      auto elected = node.receive(3U, RequestVoteResponse{1U, true});
      ASSERT_TRUE(elected.has_value()) << elected.error().to_string();
      expect_vote_quorum_transition(node, *elected, before);
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(node.role(), Role::kCandidate);
    EXPECT_EQ(node.current_term(), 1U);
    EXPECT_FALSE(node.leader_id().has_value());
    EXPECT_EQ(node.persistent_state(), before);

    auto other_vote = node.receive(3U, RequestVoteResponse{1U, true});
    ASSERT_TRUE(other_vote.has_value()) << other_vote.error().to_string();
    EXPECT_EQ(node.role(), Role::kCandidate);
    EXPECT_TRUE(other_vote->outbound.empty());
    auto retry = node.receive(2U, RequestVoteResponse{1U, true});
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    expect_vote_quorum_transition(node, *retry, before);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     SuccessfulAppendResponsePreservesProgressAndCommitUntilPublication) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_with_two_pending_entries();
    auto first = leader.receive(3U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U});
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    ASSERT_EQ(leader.commit_index(), 0U);
    const PersistentState before = leader.persistent_state();

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.receive(2U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U}));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(leader.commit_index(), 1U);
      ASSERT_TRUE((**result).persistent_state.has_value());
      EXPECT_EQ((**result).advanced_commit_index, 1U);
      EXPECT_EQ((**result).outbound.size(), 4U);
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(leader.role(), Role::kLeader);
    EXPECT_EQ(leader.current_term(), 1U);
    EXPECT_EQ(leader.leader_id(), 1U);
    EXPECT_EQ(leader.commit_index(), 0U);
    EXPECT_EQ(leader.persistent_state(), before);

    auto heartbeat = leader.heartbeat();
    ASSERT_TRUE(heartbeat.has_value()) << heartbeat.error().to_string();
    const AppendEntriesRequest* unchanged = append_request_for(*heartbeat, 2U);
    ASSERT_NE(unchanged, nullptr);
    EXPECT_EQ(unchanged->previous_log_index, 0U);
    ASSERT_EQ(unchanged->entries.size(), 1U);
    EXPECT_EQ(unchanged->entries.front().index, 1U);
    EXPECT_EQ(unchanged->leader_commit, 0U);

    auto retry = leader.receive(2U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U});
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(leader.commit_index(), 1U);
    EXPECT_EQ(retry->advanced_commit_index, 1U);
    EXPECT_EQ(retry->outbound.size(), 4U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     FinalMembershipCommitPreservesJointLeaderUntilRemovalPublication) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_awaiting_removing_final_acknowledgement();
    ASSERT_EQ(leader.role(), Role::kLeader);
    const PersistentState before = leader.persistent_state();
    PersistentState expected = before;
    expected.commit_index = 2U;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.receive(4U, AppendEntriesResponse{1U, true, 2U, std::nullopt, 0U}));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(leader.role(), Role::kFollower);
      EXPECT_FALSE(leader.leader_id().has_value());
      EXPECT_EQ(leader.persistent_state(), expected);
      EXPECT_TRUE(std::ranges::equal(leader.voters(), std::vector<NodeId>{2U, 3U, 4U}));
      EXPECT_TRUE(std::ranges::equal(leader.committed_voters(), std::vector<NodeId>{2U, 3U, 4U}));
      EXPECT_FALSE(leader.joint_membership_active());
      EXPECT_FALSE(leader.final_membership_pending());
      EXPECT_EQ((**result).advanced_commit_index, 2U);
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      ASSERT_EQ((**result).outbound.size(), 3U);
      std::vector<NodeId> destinations;
      for (const OutboundMessage& outbound : (**result).outbound) {
        destinations.push_back(outbound.destination);
        const auto* request = std::get_if<AppendEntriesRequest>(&outbound.message);
        ASSERT_NE(request, nullptr);
        EXPECT_EQ(request->leader_commit, 2U);
      }
      EXPECT_TRUE(std::ranges::equal(destinations, std::vector<NodeId>{2U, 3U, 4U}));
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(leader.role(), Role::kLeader);
    EXPECT_EQ(leader.leader_id(), 1U);
    EXPECT_EQ(leader.persistent_state(), before);
    EXPECT_TRUE(leader.joint_membership_active());
    EXPECT_TRUE(leader.final_membership_pending());
    EXPECT_TRUE(std::ranges::equal(leader.voters(), std::vector<NodeId>{1U, 2U, 3U, 4U}));

    auto heartbeat = leader.heartbeat();
    ASSERT_TRUE(heartbeat.has_value()) << heartbeat.error().to_string();
    const AppendEntriesRequest* unchanged = append_request_for(*heartbeat, 4U);
    ASSERT_NE(unchanged, nullptr);
    EXPECT_EQ(unchanged->previous_log_index, 1U);
    ASSERT_EQ(unchanged->entries.size(), 1U);
    EXPECT_EQ(unchanged->entries.front().index, 2U);
    EXPECT_EQ(unchanged->leader_commit, 1U);

    auto retry = leader.receive(4U, AppendEntriesResponse{1U, true, 2U, std::nullopt, 0U});
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(leader.role(), Role::kFollower);
    EXPECT_EQ(leader.persistent_state(), expected);
    EXPECT_EQ(retry->advanced_commit_index, 2U);
    EXPECT_EQ(retry->outbound.size(), 3U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest, RejectedAppendResponsePreservesProgressUntilRetryPublication) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_with_two_pending_entries();
    auto advanced = leader.receive(2U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U});
    ASSERT_TRUE(advanced.has_value()) << advanced.error().to_string();
    expect_single_append(*advanced, 2U, ExpectedAppend{1U, 2U, 0U});
    const PersistentState before = leader.persistent_state();

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.receive(2U, AppendEntriesResponse{1U, false, 0U, std::nullopt, 1U}));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      expect_single_append(**result, 2U, ExpectedAppend{0U, 1U, 0U});
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(leader.role(), Role::kLeader);
    EXPECT_EQ(leader.commit_index(), 0U);
    EXPECT_EQ(leader.persistent_state(), before);

    auto heartbeat = leader.heartbeat();
    ASSERT_TRUE(heartbeat.has_value()) << heartbeat.error().to_string();
    const AppendEntriesRequest* unchanged = append_request_for(*heartbeat, 2U);
    ASSERT_NE(unchanged, nullptr);
    EXPECT_EQ(unchanged->previous_log_index, 1U);
    ASSERT_EQ(unchanged->entries.size(), 1U);
    EXPECT_EQ(unchanged->entries.front().index, 2U);

    auto retry = leader.receive(2U, AppendEntriesResponse{1U, false, 0U, std::nullopt, 1U});
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    expect_single_append(*retry, 2U, ExpectedAppend{0U, 1U, 0U});
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     SuccessfulSnapshotResponsePreservesProgressUntilFollowUpPublication) {
  expect_snapshot_response_allocation_atomic(true);
}

TEST(RaftNodeAllocationFailureTest,
     RejectedSnapshotResponsePreservesProgressUntilRetryPublication) {
  expect_snapshot_response_allocation_atomic(false);
}

TEST(RaftNodeAllocationFailureTest, StaleAppendRequestOwnsRejectionBeforeReturning) {
  PersistentState initial{};
  initial.current_term = 2U;
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U, 2U, 3U}, initial);
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode node = std::move(*created);

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(node.receive(2U, AppendEntriesRequest{1U, 2U, 0U, 0U, {}, 0U}));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      expect_append_response(**result, 2U, false, 0U, std::nullopt, 1U);
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(node.role(), Role::kFollower);
    EXPECT_EQ(node.persistent_state(), initial);
    auto retry = node.receive(2U, AppendEntriesRequest{1U, 2U, 0U, 0U, {}, 0U});
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    expect_append_response(*retry, 2U, false, 0U, std::nullopt, 1U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     HigherTermConflictingAppendRequestPreparesDemotionAndFeedbackTogether) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_ready_for_read_barrier(false);
    auto barrier = leader.begin_read_barrier();
    ASSERT_TRUE(barrier.has_value()) << barrier.error().to_string();
    const PersistentState before = leader.persistent_state();
    const AppendEntriesRequest request{2U, 2U, 1U, 2U, {}, 0U};

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.receive(2U, request));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      PersistentState expected = before;
      expected.current_term = 2U;
      expected.voted_for.reset();
      EXPECT_EQ(leader.role(), Role::kFollower);
      EXPECT_EQ(leader.leader_id(), 2U);
      EXPECT_EQ(leader.persistent_state(), expected);
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      expect_append_response(**result, 2U, false, 1U, 1U, 1U);
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

    auto retry = leader.receive(2U, request);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    PersistentState expected = before;
    expected.current_term = 2U;
    expected.voted_for.reset();
    EXPECT_EQ(leader.role(), Role::kFollower);
    EXPECT_EQ(leader.leader_id(), 2U);
    EXPECT_EQ(leader.persistent_state(), expected);
    expect_append_response(*retry, 2U, false, 1U, 1U, 1U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     AcceptedAppendRequestPreservesLeaderUntilSuffixCommitPublication) {
  const AppendEntriesRequest request{
      2U,
      2U,
      0U,
      0U,
      {LogEntry{1U, 2U, 1U, {std::byte{0x51U}}}, LogEntry{2U, 2U, 1U, {std::byte{0x52U}}}},
      2U,
  };
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U, 2U, 3U});
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode leader = std::move(*created);
    ASSERT_TRUE(leader.start_election().has_value());
    ASSERT_TRUE(leader.receive(3U, RequestVoteResponse{1U, true}).has_value());
    ASSERT_TRUE(leader.propose(1U, {std::byte{0x41U}}).has_value());
    ASSERT_TRUE(leader.propose(1U, {std::byte{0x42U}}).has_value());
    ASSERT_EQ(leader.role(), Role::kLeader);
    const PersistentState before = leader.persistent_state();
    Message inbound = request;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.receive(2U, std::move(inbound)));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      PersistentState expected = before;
      expected.current_term = 2U;
      expected.voted_for.reset();
      expected.log = request.entries;
      expected.commit_index = 2U;
      EXPECT_EQ(leader.role(), Role::kFollower);
      EXPECT_EQ(leader.leader_id(), 2U);
      EXPECT_EQ(leader.persistent_state(), expected);
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      EXPECT_EQ((**result).advanced_commit_index, 2U);
      expect_append_response(**result, 2U, true, 2U, std::nullopt, 0U);
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

    auto retry = leader.receive(2U, request);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(leader.role(), Role::kFollower);
    EXPECT_EQ(leader.current_term(), 2U);
    EXPECT_EQ(leader.leader_id(), 2U);
    EXPECT_EQ(leader.commit_index(), 2U);
    EXPECT_EQ(retry->advanced_commit_index, 2U);
    expect_append_response(*retry, 2U, true, 2U, std::nullopt, 0U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     JointMembershipAppendRequestPreservesStableFollowerUntilPublication) {
  expect_membership_append_request_allocation_atomic(false);
}

TEST(RaftNodeAllocationFailureTest,
     FinalMembershipAppendRequestPreservesJointFollowerUntilCommitPublication) {
  expect_membership_append_request_allocation_atomic(true);
}

TEST(RaftNodeAllocationFailureTest, StaleSnapshotRequestOwnsRejectionBeforeReturning) {
  PersistentState initial{};
  initial.current_term = 2U;
  const InstallSnapshotRequest request{1U, 2U, incoming_snapshot_metadata()};
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U, 2U, 3U}, initial);
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode node = std::move(*created);
    Message inbound = request;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(node.receive(2U, std::move(inbound)));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      expect_snapshot_response(**result, 2U, false, 0U);
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(node.role(), Role::kFollower);
    EXPECT_EQ(node.persistent_state(), initial);
    auto retry = node.receive(2U, request);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    expect_snapshot_response(*retry, 2U, false, 0U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     HigherTermInstalledSnapshotRequestPreparesDemotionAndAcknowledgement) {
  const InstallSnapshotRequest request{2U, 2U, incoming_snapshot_metadata()};
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_with_compacted_snapshot();
    auto barrier = leader.begin_read_barrier();
    ASSERT_TRUE(barrier.has_value()) << barrier.error().to_string();
    const PersistentState before = leader.persistent_state();
    Message inbound = request;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.receive(2U, std::move(inbound)));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      PersistentState expected = before;
      expected.current_term = 2U;
      expected.voted_for.reset();
      EXPECT_EQ(leader.role(), Role::kFollower);
      EXPECT_EQ(leader.leader_id(), 2U);
      EXPECT_EQ(leader.persistent_state(), expected);
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      expect_snapshot_response(**result, 2U, true, 2U);
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

    auto retry = leader.receive(2U, request);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(leader.role(), Role::kFollower);
    EXPECT_EQ(leader.current_term(), 2U);
    EXPECT_EQ(leader.leader_id(), 2U);
    expect_snapshot_response(*retry, 2U, true, 2U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     NewSnapshotRequestPreservesLeaderUntilPendingInstallPublication) {
  const SnapshotMetadata snapshot = incoming_snapshot_metadata();
  const InstallSnapshotRequest request{2U, 2U, snapshot};
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 96U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_ready_for_read_barrier(false);
    auto barrier = leader.begin_read_barrier();
    ASSERT_TRUE(barrier.has_value()) << barrier.error().to_string();
    const PersistentState before = leader.persistent_state();
    Message inbound = request;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.receive(2U, std::move(inbound)));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      PersistentState expected = before;
      expected.current_term = 2U;
      expected.voted_for.reset();
      EXPECT_EQ(leader.role(), Role::kFollower);
      EXPECT_EQ(leader.leader_id(), 2U);
      EXPECT_EQ(leader.persistent_state(), expected);
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      const PendingSnapshotInstall* pending_install = pending_snapshot_install(**result);
      ASSERT_NE(pending_install, nullptr);
      EXPECT_EQ(pending_install->source, 2U);
      EXPECT_EQ(pending_install->snapshot, snapshot);
      EXPECT_TRUE((**result).outbound.empty());
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

    auto retry = leader.receive(2U, request);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(leader.role(), Role::kFollower);
    EXPECT_EQ(leader.current_term(), 2U);
    EXPECT_EQ(leader.leader_id(), 2U);
    const PendingSnapshotInstall* pending_install = pending_snapshot_install(*retry);
    ASSERT_NE(pending_install, nullptr);
    EXPECT_EQ(pending_install->source, 2U);
    EXPECT_EQ(pending_install->snapshot, snapshot);
    EXPECT_TRUE(retry->outbound.empty());
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     SameTermCompetingSnapshotRequestPreservesOriginalPendingInstall) {
  expect_competing_snapshot_request_allocation_atomic(false);
}

TEST(RaftNodeAllocationFailureTest,
     HigherTermCompetingSnapshotRequestPreservesOriginalPendingInstallUntilFeedbackPublication) {
  expect_competing_snapshot_request_allocation_atomic(true);
}

TEST(RaftNodeAllocationFailureTest,
     RejectedSnapshotCompletionPreservesPendingIdentityUntilResponsePublication) {
  const SnapshotMetadata snapshot = incoming_snapshot_metadata();
  PersistentState initial{};
  initial.current_term = 2U;
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U, 2U, 3U}, initial);
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode follower = std::move(*created);
    auto pending = follower.receive(2U, InstallSnapshotRequest{2U, 2U, snapshot});
    ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
    ASSERT_TRUE(pending->snapshot_install.has_value());
    SnapshotMetadata completion = snapshot;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(follower.complete_snapshot_install(2U, std::move(completion), false));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(follower.persistent_state(), initial);
      EXPECT_FALSE((**result).persistent_state.has_value());
      expect_snapshot_response(**result, 2U, false, 0U);
      auto duplicate = follower.complete_snapshot_install(2U, snapshot, false);
      ASSERT_FALSE(duplicate.has_value());
      EXPECT_EQ(duplicate.error().code(), common::StatusCode::kInvalidArgument);
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(follower.persistent_state(), initial);
    auto retry = follower.complete_snapshot_install(2U, snapshot, false);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_FALSE(retry->persistent_state.has_value());
    expect_snapshot_response(*retry, 2U, false, 0U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     StaleTermSnapshotCompletionPreservesPendingIdentityUntilResponsePublication) {
  const SnapshotMetadata snapshot = incoming_snapshot_metadata();
  SnapshotMetadata competitor = snapshot;
  competitor.last_included_index = 2U;
  competitor.last_included_term = 2U;
  competitor.manifest_generation = 10U;
  competitor.part_set_checksum.fill(std::byte{0xA2U});
  PersistentState initial{};
  initial.current_term = 2U;
  PersistentState advanced = initial;
  advanced.current_term = 3U;
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U, 2U, 3U}, initial);
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode follower = std::move(*created);
    auto pending = follower.receive(2U, InstallSnapshotRequest{2U, 2U, snapshot});
    ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
    ASSERT_TRUE(pending->snapshot_install.has_value());
    auto higher = follower.receive(3U, InstallSnapshotRequest{3U, 3U, competitor});
    ASSERT_TRUE(higher.has_value()) << higher.error().to_string();
    ASSERT_TRUE(higher->persistent_state.has_value());
    EXPECT_EQ(higher->persistent_state, std::optional<PersistentState>{advanced});
    EXPECT_FALSE(higher->snapshot_install.has_value());
    ASSERT_EQ(higher->outbound.size(), 1U);
    EXPECT_EQ(higher->outbound.front().destination, 3U);
    EXPECT_EQ(std::get<InstallSnapshotResponse>(higher->outbound.front().message),
              (InstallSnapshotResponse{3U, false, 0U}));
    ASSERT_EQ(follower.persistent_state(), advanced);
    ASSERT_EQ(follower.leader_id(), 3U);
    SnapshotMetadata completion = snapshot;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(follower.complete_snapshot_install(2U, std::move(completion), true));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(follower.persistent_state(), advanced);
      EXPECT_EQ(follower.leader_id(), 3U);
      EXPECT_FALSE((**result).persistent_state.has_value());
      expect_snapshot_response(**result, 3U, false, 0U);
      auto duplicate = follower.complete_snapshot_install(2U, snapshot, true);
      ASSERT_FALSE(duplicate.has_value());
      EXPECT_EQ(duplicate.error().code(), common::StatusCode::kInvalidArgument);
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(follower.persistent_state(), advanced);
    EXPECT_EQ(follower.leader_id(), 3U);
    auto retry = follower.complete_snapshot_install(2U, snapshot, true);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_FALSE(retry->persistent_state.has_value());
    expect_snapshot_response(*retry, 3U, false, 0U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     SuccessfulSnapshotCompletionPreservesPendingIdentityUntilDurableTransitionPublication) {
  SnapshotMetadata snapshot = incoming_snapshot_metadata();
  snapshot.voters = {1U, 2U, 4U};
  PersistentState initial{};
  initial.current_term = 2U;
  initial.log = {LogEntry{1U, 1U, 1U, {std::byte{0x11U}}},
                 LogEntry{2U, 2U, 1U, {std::byte{0x22U}}}};
  initial.commit_index = 2U;
  initial.applied_index = 2U;
  PersistentState expected = initial;
  expected.snapshot = snapshot;
  expected.log = {initial.log.back()};
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U, 2U, 3U}, initial);
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode follower = std::move(*created);
    auto pending = follower.receive(2U, InstallSnapshotRequest{2U, 2U, snapshot});
    ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
    ASSERT_TRUE(pending->snapshot_install.has_value());
    SnapshotMetadata completion = snapshot;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(follower.complete_snapshot_install(2U, std::move(completion), true));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(follower.persistent_state(), expected);
      EXPECT_TRUE(std::ranges::equal(follower.voters(), snapshot.voters));
      EXPECT_TRUE(std::ranges::equal(follower.committed_voters(), snapshot.voters));
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      EXPECT_EQ((**result).advanced_commit_index, 2U);
      expect_snapshot_response(**result, 2U, true, 1U);
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(follower.persistent_state(), initial);
    EXPECT_TRUE(std::ranges::equal(follower.voters(), std::vector<NodeId>{1U, 2U, 3U}));
    EXPECT_TRUE(std::ranges::equal(follower.committed_voters(), std::vector<NodeId>{1U, 2U, 3U}));
    auto retry = follower.complete_snapshot_install(2U, snapshot, true);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(follower.persistent_state(), expected);
    EXPECT_TRUE(std::ranges::equal(follower.voters(), snapshot.voters));
    EXPECT_TRUE(std::ranges::equal(follower.committed_voters(), snapshot.voters));
    const PersistentState* persistent = returned_persistent_state(*retry);
    ASSERT_NE(persistent, nullptr);
    EXPECT_EQ(*persistent, expected);
    EXPECT_EQ(retry->advanced_commit_index, 2U);
    expect_snapshot_response(*retry, 2U, true, 1U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     LocalSnapshotCompactionPreservesStateUntilDurableTransitionPublication) {
  PersistentState initial{};
  initial.current_term = 2U;
  initial.log = {LogEntry{1U, 1U, 1U, {std::byte{0x11U}}}, LogEntry{2U, 1U, 1U, {std::byte{0x22U}}},
                 LogEntry{3U, 2U, 1U, {std::byte{0x33U}}}};
  initial.commit_index = 3U;
  initial.applied_index = 3U;
  SnapshotMetadata snapshot{};
  snapshot.last_included_index = 2U;
  snapshot.last_included_term = 1U;
  snapshot.manifest_generation = 11U;
  snapshot.part_set_checksum.fill(std::byte{0xA5U});
  PersistentState expected = initial;
  expected.snapshot = snapshot;
  expected.snapshot.voters = {1U, 2U, 3U};
  expected.log = {initial.log.back()};

  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 96U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U, 2U, 3U}, initial);
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode follower = std::move(*created);
    SnapshotMetadata candidate = snapshot;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(follower.compact_snapshot(std::move(candidate)));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(follower.persistent_state(), expected);
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      EXPECT_TRUE((**result).outbound.empty());
      EXPECT_FALSE((**result).advanced_commit_index.has_value());
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(follower.persistent_state(), initial);
    auto retry = follower.compact_snapshot(snapshot);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(follower.persistent_state(), expected);
    const PersistentState* persistent = returned_persistent_state(*retry);
    ASSERT_NE(persistent, nullptr);
    EXPECT_EQ(*persistent, expected);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     FinalMembershipBoundaryCompactionPreservesLogUntilCheckpointPublication) {
  auto joint = encode_membership_command_v1(
      JointMembershipCommand{.old_voters = {1U, 2U, 3U}, .new_voters = {2U, 3U, 4U}});
  auto final = encode_membership_command_v1(
      FinalMembershipCommand{.joint_index = 1U, .new_voters = {2U, 3U, 4U}});
  ASSERT_TRUE(joint.has_value()) << joint.error().to_string();
  ASSERT_TRUE(final.has_value()) << final.error().to_string();
  PersistentState initial{};
  initial.current_term = 1U;
  initial.log = {
      LogEntry{1U, 1U, kJointMembershipEntryType, *joint},
      LogEntry{2U, 1U, kFinalMembershipEntryType, *final},
      LogEntry{3U, 1U, 1U, {std::byte{0x33U}}},
  };
  initial.commit_index = 3U;
  initial.applied_index = 3U;
  SnapshotMetadata snapshot{};
  snapshot.last_included_index = 2U;
  snapshot.last_included_term = 1U;
  snapshot.manifest_generation = 19U;
  snapshot.part_set_checksum.fill(std::byte{0xB6U});
  PersistentState expected = initial;
  expected.snapshot = snapshot;
  expected.snapshot.configuration_index = 2U;
  expected.snapshot.voters = {2U, 3U, 4U};
  expected.log = {initial.log.back()};

  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 192U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(2U, {1U, 2U, 3U}, initial);
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode follower = std::move(*created);
    ASSERT_FALSE(follower.joint_membership_active());
    ASSERT_TRUE(std::ranges::equal(follower.voters(), std::vector<NodeId>{2U, 3U, 4U}));
    SnapshotMetadata candidate = snapshot;

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(follower.compact_snapshot(std::move(candidate)));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(follower.persistent_state(), expected);
      EXPECT_FALSE(follower.joint_membership_active());
      EXPECT_TRUE(std::ranges::equal(follower.voters(), std::vector<NodeId>{2U, 3U, 4U}));
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      EXPECT_TRUE((**result).outbound.empty());
      auto reopened = RaftNode::create(2U, {1U, 2U, 3U}, expected);
      ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
      EXPECT_TRUE(std::ranges::equal(reopened->voters(), std::vector<NodeId>{2U, 3U, 4U}));
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(follower.persistent_state(), initial);
    EXPECT_FALSE(follower.joint_membership_active());
    EXPECT_TRUE(std::ranges::equal(follower.voters(), std::vector<NodeId>{2U, 3U, 4U}));

    auto retry = follower.compact_snapshot(snapshot);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(follower.persistent_state(), expected);
    const PersistentState* persistent = returned_persistent_state(*retry);
    ASSERT_NE(persistent, nullptr);
    EXPECT_EQ(*persistent, expected);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     AppliedIndexAdvancementPreservesProgressUntilDurableTransitionPublication) {
  PersistentState initial{};
  initial.current_term = 1U;
  initial.log = {LogEntry{1U, 1U, 1U, {std::byte{0x11U}}},
                 LogEntry{2U, 1U, 1U, {std::byte{0x22U}}}};
  initial.commit_index = 2U;
  PersistentState expected = initial;
  expected.applied_index = 1U;

  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U, 2U, 3U}, initial);
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode follower = std::move(*created);

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(follower.mark_applied(1U));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(follower.persistent_state(), expected);
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      EXPECT_TRUE((**result).outbound.empty());
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(follower.persistent_state(), initial);
    EXPECT_EQ(follower.committed_unapplied().size(), 2U);
    auto retry = follower.mark_applied(1U);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(follower.persistent_state(), expected);
    const PersistentState* persistent = returned_persistent_state(*retry);
    ASSERT_NE(persistent, nullptr);
    EXPECT_EQ(*persistent, expected);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     MultiVoterProposalPreservesLogAndReplicationUntilTransitionPublication) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U, 2U, 3U});
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode leader = std::move(*created);
    ASSERT_TRUE(leader.start_election().has_value());
    ASSERT_TRUE(leader.receive(2U, RequestVoteResponse{1U, true}).has_value());
    ASSERT_EQ(leader.role(), Role::kLeader);
    const PersistentState before = leader.persistent_state();
    PersistentState expected = before;
    expected.log = {LogEntry{1U, 1U, 1U, {std::byte{0x42U}}}};
    std::vector<std::byte> payload{std::byte{0x42U}};

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.propose(1U, std::move(payload)));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(leader.persistent_state(), expected);
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      EXPECT_FALSE((**result).advanced_commit_index.has_value());
      ASSERT_EQ((**result).outbound.size(), 2U);
      for (const NodeId peer : {2U, 3U}) {
        const AppendEntriesRequest* request = append_request_for(**result, peer);
        ASSERT_NE(request, nullptr);
        EXPECT_EQ(request->previous_log_index, 0U);
        ASSERT_EQ(request->entries.size(), 1U);
        EXPECT_EQ(request->entries.front(), expected.log.front());
        EXPECT_EQ(request->leader_commit, 0U);
      }
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(leader.role(), Role::kLeader);
    EXPECT_EQ(leader.leader_id(), 1U);
    EXPECT_EQ(leader.persistent_state(), before);
    auto heartbeat = leader.heartbeat();
    ASSERT_TRUE(heartbeat.has_value()) << heartbeat.error().to_string();
    ASSERT_EQ(heartbeat->outbound.size(), 2U);
    for (const OutboundMessage& outbound : heartbeat->outbound) {
      const auto* request = std::get_if<AppendEntriesRequest>(&outbound.message);
      ASSERT_NE(request, nullptr);
      EXPECT_TRUE(request->entries.empty());
      EXPECT_EQ(request->previous_log_index, 0U);
    }

    auto retry = leader.propose(1U, {std::byte{0x42U}});
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(leader.persistent_state(), expected);
    const PersistentState* persistent = returned_persistent_state(*retry);
    ASSERT_NE(persistent, nullptr);
    EXPECT_EQ(*persistent, expected);
    EXPECT_EQ(retry->outbound.size(), 2U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     SingleVoterProposalPreservesStateUntilImmediateCommitPublication) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 96U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U});
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode leader = std::move(*created);
    ASSERT_TRUE(leader.start_election().has_value());
    ASSERT_EQ(leader.role(), Role::kLeader);
    const PersistentState before = leader.persistent_state();
    PersistentState expected = before;
    expected.log = {LogEntry{1U, 1U, 1U, {std::byte{0x42U}}}};
    expected.commit_index = 1U;
    std::vector<std::byte> payload{std::byte{0x42U}};

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.propose(1U, std::move(payload)));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(leader.persistent_state(), expected);
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      EXPECT_EQ((**result).advanced_commit_index, 1U);
      EXPECT_TRUE((**result).outbound.empty());
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(leader.role(), Role::kLeader);
    EXPECT_EQ(leader.leader_id(), 1U);
    EXPECT_EQ(leader.persistent_state(), before);
    auto retry = leader.propose(1U, {std::byte{0x42U}});
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(leader.persistent_state(), expected);
    const PersistentState* persistent = returned_persistent_state(*retry);
    ASSERT_NE(persistent, nullptr);
    EXPECT_EQ(*persistent, expected);
    EXPECT_EQ(retry->advanced_commit_index, 1U);
    EXPECT_TRUE(retry->outbound.empty());
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     MultiVoterCurrentTermProgressPreservesStateUntilReplicationPublication) {
  expect_current_term_progress_allocation_atomic({1U, 2U, 3U}, false);
}

TEST(RaftNodeAllocationFailureTest,
     SingleVoterCurrentTermProgressPreservesStateUntilImmediateCommitPublication) {
  expect_current_term_progress_allocation_atomic({1U}, true);
}

TEST(RaftNodeAllocationFailureTest,
     PriorTermJointMembershipRetryPreservesEntryUntilProgressPublication) {
  expect_prior_term_membership_retry_progress_allocation_atomic(false);
}

TEST(RaftNodeAllocationFailureTest,
     PriorTermFinalMembershipRetryPreservesEntryUntilProgressPublication) {
  expect_prior_term_membership_retry_progress_allocation_atomic(true);
}

TEST(RaftNodeAllocationFailureTest,
     JointMembershipProposalPreservesStableConfigurationUntilPublication) {
  auto encoded = encode_membership_command_v1(
      JointMembershipCommand{.old_voters = {1U, 2U, 3U}, .new_voters = {2U, 3U, 4U}});
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::vector<std::byte> expected_payload = *encoded;
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftNode::create(1U, {1U, 2U, 3U});
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    RaftNode leader = std::move(*created);
    ASSERT_TRUE(leader.start_election().has_value());
    ASSERT_TRUE(leader.receive(2U, RequestVoteResponse{1U, true}).has_value());
    ASSERT_EQ(leader.role(), Role::kLeader);
    const PersistentState before = leader.persistent_state();
    PersistentState expected = before;
    expected.log = {
        LogEntry{1U, 1U, kJointMembershipEntryType, expected_payload},
    };
    std::vector<NodeId> new_voters{2U, 3U, 4U};

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.begin_membership_change(std::move(new_voters)));
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(leader.persistent_state(), expected);
      EXPECT_TRUE(leader.joint_membership_active());
      EXPECT_FALSE(leader.final_membership_pending());
      EXPECT_TRUE(std::ranges::equal(leader.voters(), std::vector<NodeId>{1U, 2U, 3U, 4U}));
      EXPECT_TRUE(std::ranges::equal(leader.committed_voters(), std::vector<NodeId>{1U, 2U, 3U}));
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      EXPECT_FALSE((**result).advanced_commit_index.has_value());
      ASSERT_EQ((**result).outbound.size(), 3U);
      for (const OutboundMessage& outbound : (**result).outbound) {
        const auto* request = std::get_if<AppendEntriesRequest>(&outbound.message);
        ASSERT_NE(request, nullptr);
        ASSERT_EQ(request->entries.size(), 1U);
        EXPECT_EQ(request->entries.front(), expected.log.front());
        EXPECT_EQ(request->leader_commit, 0U);
      }
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(leader.role(), Role::kLeader);
    EXPECT_EQ(leader.persistent_state(), before);
    EXPECT_FALSE(leader.joint_membership_active());
    EXPECT_TRUE(std::ranges::equal(leader.voters(), std::vector<NodeId>{1U, 2U, 3U}));
    auto heartbeat = leader.heartbeat();
    ASSERT_TRUE(heartbeat.has_value()) << heartbeat.error().to_string();
    ASSERT_EQ(heartbeat->outbound.size(), 2U);
    for (const OutboundMessage& outbound : heartbeat->outbound) {
      const auto* request = std::get_if<AppendEntriesRequest>(&outbound.message);
      ASSERT_NE(request, nullptr);
      EXPECT_TRUE(request->entries.empty());
    }

    auto retry = leader.begin_membership_change({2U, 3U, 4U});
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(leader.persistent_state(), expected);
    EXPECT_TRUE(leader.joint_membership_active());
    EXPECT_EQ(retry->outbound.size(), 3U);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftNodeAllocationFailureTest,
     FinalMembershipProposalPreservesJointConfigurationUntilPublication) {
  auto encoded = encode_membership_command_v1(
      FinalMembershipCommand{.joint_index = 1U, .new_voters = {2U, 3U, 4U}});
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::vector<std::byte> expected_payload = *encoded;
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RaftNode leader = leader_with_committed_joint_membership();
    ASSERT_EQ(leader.role(), Role::kLeader);
    const PersistentState before = leader.persistent_state();
    PersistentState expected = before;
    expected.log.push_back(LogEntry{2U, 1U, kFinalMembershipEntryType, expected_payload});

    std::optional<common::Result<Transition>> result;
    std::size_t observed = 0U;
    {
      test::ScopedAllocationFailure failure{fail_after};
      result.emplace(leader.finalize_membership_change());
      observed = failure.observed_allocations();
      failure.disable();
    }

    ASSERT_TRUE(result.has_value());
    if (result->has_value()) {
      EXPECT_EQ(leader.persistent_state(), expected);
      EXPECT_TRUE(leader.joint_membership_active());
      EXPECT_TRUE(leader.final_membership_pending());
      EXPECT_TRUE(std::ranges::equal(leader.voters(), std::vector<NodeId>{1U, 2U, 3U, 4U}));
      const PersistentState* persistent = returned_persistent_state(**result);
      ASSERT_NE(persistent, nullptr);
      EXPECT_EQ(*persistent, expected);
      EXPECT_FALSE((**result).advanced_commit_index.has_value());
      ASSERT_EQ((**result).outbound.size(), 3U);
      for (const OutboundMessage& outbound : (**result).outbound) {
        const auto* request = std::get_if<AppendEntriesRequest>(&outbound.message);
        ASSERT_NE(request, nullptr);
        ASSERT_FALSE(request->entries.empty());
        EXPECT_EQ(request->entries.back(), expected.log.back());
        EXPECT_EQ(request->leader_commit, 1U);
      }
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result->error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(leader.role(), Role::kLeader);
    EXPECT_EQ(leader.persistent_state(), before);
    EXPECT_TRUE(leader.joint_membership_active());
    EXPECT_TRUE(leader.joint_membership_can_finalize());
    EXPECT_FALSE(leader.final_membership_pending());
    EXPECT_TRUE(std::ranges::equal(leader.voters(), std::vector<NodeId>{1U, 2U, 3U, 4U}));

    auto retry = leader.finalize_membership_change();
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(leader.persistent_state(), expected);
    EXPECT_TRUE(leader.final_membership_pending());
    EXPECT_EQ(retry->outbound.size(), 3U);
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
