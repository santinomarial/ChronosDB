#include "chronos/raft/membership.hpp"
#include "chronos/raft/node.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <gtest/gtest.h>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

struct Envelope {
  NodeId source{};
  OutboundMessage outbound;
};

[[nodiscard]] const ReadBarrier* completed_read_barrier(const Transition& transition) {
  const std::optional<ReadBarrier>& barrier = transition.read_barrier_ready;
  if (!barrier.has_value())
    return nullptr;
  return &barrier.value();
}

class Cluster {
public:
  Cluster() {
    for (NodeId id = 1U; id <= 3U; ++id) {
      auto node = RaftNode::create(id, {1U, 2U, 3U});
      EXPECT_TRUE(node.has_value());
      nodes_.emplace(id, std::move(*node));
    }
  }

  [[nodiscard]] RaftNode& node(const NodeId id) {
    return nodes_.at(id);
  }

  void enqueue(const NodeId source, Transition transition) {
    for (OutboundMessage& outbound : transition.outbound) {
      queue_.push_back(Envelope{source, std::move(outbound)});
    }
  }

  void drain(const std::set<NodeId>& available = {1U, 2U, 3U}) {
    std::size_t delivered = 0U;
    while (!queue_.empty()) {
      ASSERT_LT(delivered++, 10'000U);
      Envelope envelope = std::move(queue_.front());
      queue_.pop_front();
      if (!available.contains(envelope.source) ||
          !available.contains(envelope.outbound.destination)) {
        continue;
      }
      auto transition = nodes_.at(envelope.outbound.destination)
                            .receive(envelope.source, std::move(envelope.outbound.message));
      ASSERT_TRUE(transition.has_value()) << transition.error().to_string();
      enqueue(envelope.outbound.destination, std::move(*transition));
    }
  }

  void replace(const NodeId id, PersistentState state) {
    auto restarted = RaftNode::create(id, {1U, 2U, 3U}, std::move(state));
    ASSERT_TRUE(restarted.has_value());
    nodes_.erase(id);
    nodes_.emplace(id, std::move(*restarted));
  }

private:
  std::map<NodeId, RaftNode> nodes_;
  std::deque<Envelope> queue_;
};

TEST(RaftNodeTest, ElectsReplicatesFailsOverRejectsStaleLeaderAndCatchesUpRestart) {
  Cluster cluster;
  auto election = cluster.node(1U).start_election();
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->persistent_state.has_value());
  cluster.enqueue(1U, std::move(*election));
  cluster.drain();
  ASSERT_EQ(cluster.node(1U).role(), Role::kLeader);
  EXPECT_EQ(cluster.node(1U).current_term(), 1U);

  auto first = cluster.node(1U).propose(1U, {std::byte{0x11}});
  ASSERT_TRUE(first.has_value());
  cluster.enqueue(1U, std::move(*first));
  cluster.drain();
  for (NodeId id = 1U; id <= 3U; ++id) {
    EXPECT_EQ(cluster.node(id).commit_index(), 1U);
  }
  ASSERT_EQ(cluster.node(1U).committed_unapplied().size(), 1U);
  auto applied = cluster.node(1U).mark_applied(1U);
  ASSERT_TRUE(applied.has_value());
  EXPECT_TRUE(applied->persistent_state.has_value());

  auto replacement_election = cluster.node(2U).start_election();
  ASSERT_TRUE(replacement_election.has_value());
  cluster.enqueue(2U, std::move(*replacement_election));
  cluster.drain({2U, 3U});
  ASSERT_EQ(cluster.node(2U).role(), Role::kLeader);
  EXPECT_EQ(cluster.node(2U).current_term(), 2U);

  auto second = cluster.node(2U).propose(1U, {std::byte{0x22}});
  ASSERT_TRUE(second.has_value());
  cluster.enqueue(2U, std::move(*second));
  cluster.drain({2U, 3U});
  EXPECT_EQ(cluster.node(2U).commit_index(), 2U);
  EXPECT_EQ(cluster.node(3U).commit_index(), 2U);

  auto stale = cluster.node(1U).heartbeat();
  ASSERT_TRUE(stale.has_value());
  cluster.enqueue(1U, std::move(*stale));
  cluster.drain({1U, 3U});
  EXPECT_EQ(cluster.node(1U).role(), Role::kFollower);
  EXPECT_EQ(cluster.node(1U).current_term(), 2U);
  EXPECT_FALSE(cluster.node(1U).propose(1U, {std::byte{0x33}}).has_value());

  const PersistentState restarted_state = cluster.node(1U).persistent_state();
  cluster.replace(1U, restarted_state);
  auto catch_up = cluster.node(2U).heartbeat();
  ASSERT_TRUE(catch_up.has_value());
  cluster.enqueue(2U, std::move(*catch_up));
  cluster.drain();
  EXPECT_EQ(cluster.node(1U).last_log_index(), 2U);
  EXPECT_EQ(cluster.node(1U).commit_index(), 2U);
  EXPECT_EQ(cluster.node(1U).persistent_state().log, cluster.node(2U).persistent_state().log);
}

TEST(RaftNodeTest, RejectsStaleVoteAndCandidateWithOlderLog) {
  PersistentState state{};
  state.current_term = 3U;
  state.log.push_back(LogEntry{1U, 3U, 1U, {std::byte{1U}}});
  auto node = RaftNode::create(1U, {1U, 2U, 3U}, state);
  ASSERT_TRUE(node.has_value());
  auto stale = node->receive(2U, RequestVoteRequest{2U, 2U, 0U, 0U});
  ASSERT_TRUE(stale.has_value());
  ASSERT_EQ(stale->outbound.size(), 1U);
  EXPECT_FALSE(std::get<RequestVoteResponse>(stale->outbound.front().message).granted);
  auto older = node->receive(2U, RequestVoteRequest{3U, 2U, 0U, 0U});
  ASSERT_TRUE(older.has_value());
  EXPECT_FALSE(std::get<RequestVoteResponse>(older->outbound.front().message).granted);
}

TEST(RaftNodeTest, RejectsMalformedHigherTermWithoutChangingPersistentState) {
  auto node = RaftNode::create(1U, {1U, 2U, 3U});
  ASSERT_TRUE(node.has_value());
  const PersistentState before = node->persistent_state();

  auto malformed = node->receive(2U, RequestVoteRequest{9U, 3U, 0U, 0U});

  ASSERT_FALSE(malformed.has_value());
  EXPECT_EQ(malformed.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(node->persistent_state(), before);
}

TEST(RaftNodeTest, RejectsPersistentSnapshotNewerThanCurrentTerm) {
  PersistentState state{};
  state.current_term = 2U;
  state.snapshot.last_included_index = 1U;
  state.snapshot.last_included_term = 3U;
  state.snapshot.manifest_generation = 1U;
  state.snapshot.voters = {1U, 2U, 3U};
  state.commit_index = 1U;
  state.applied_index = 1U;

  auto node = RaftNode::create(1U, {1U, 2U, 3U}, std::move(state));

  ASSERT_FALSE(node.has_value());
  EXPECT_EQ(node.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(RaftNodeTest, RejectsUnencodableMembershipVoterLimitAtConstruction) {
  RaftLimits limits{};
  limits.maximum_voters = kMaximumMembershipVoters + 1U;

  auto node = RaftNode::create(1U, {1U}, {}, limits);

  ASSERT_FALSE(node.has_value());
  EXPECT_EQ(node.error().code(), common::StatusCode::kInvalidArgument);

  limits.maximum_voters = kMaximumMembershipVoters;
  node = RaftNode::create(1U, {1U}, {}, limits);
  ASSERT_TRUE(node.has_value()) << node.error().to_string();
}

TEST(RaftNodeTest, RejectsNoncanonicalEmptyPersistentState) {
  PersistentState term_zero_vote{};
  term_zero_vote.voted_for = 2U;
  PersistentState empty_snapshot_generation{};
  empty_snapshot_generation.snapshot.manifest_generation = 1U;
  PersistentState empty_snapshot_checksum{};
  empty_snapshot_checksum.snapshot.part_set_checksum.front() = std::byte{1U};

  for (PersistentState state :
       {term_zero_vote, empty_snapshot_generation, empty_snapshot_checksum}) {
    auto node = RaftNode::create(1U, {1U, 2U, 3U}, std::move(state));

    EXPECT_FALSE(node.has_value());
    if (node.has_value())
      continue;
    EXPECT_EQ(node.error().code(), common::StatusCode::kInvalidArgument);
  }
}

TEST(RaftNodeTest, RejectsNoncanonicalTermsAndResponseStateBeforeObservingTerm) {
  SnapshotMetadata future_term_snapshot{};
  future_term_snapshot.last_included_index = 1U;
  future_term_snapshot.last_included_term = 10U;
  future_term_snapshot.manifest_generation = 1U;
  future_term_snapshot.voters = {1U, 2U, 3U};
  SnapshotMetadata exhausted_index_snapshot = future_term_snapshot;
  exhausted_index_snapshot.last_included_index = std::numeric_limits<LogIndex>::max();
  exhausted_index_snapshot.last_included_term = 9U;
  const std::vector<Message> malformed{
      RequestVoteRequest{0U, 2U, 0U, 0U},
      RequestVoteRequest{9U, 2U, 0U, 1U},
      RequestVoteRequest{9U, 2U, std::numeric_limits<LogIndex>::max(), 9U},
      AppendEntriesRequest{9U, 2U, 0U, 1U, {}, 0U},
      AppendEntriesRequest{9U, 2U, std::numeric_limits<LogIndex>::max(), 9U, {}, 0U},
      AppendEntriesRequest{9U, 2U, 0U, 0U, {}, std::numeric_limits<LogIndex>::max()},
      AppendEntriesResponse{9U, true, 0U, 1U, 1U},
      AppendEntriesResponse{9U, false, 0U, 0U, 1U},
      AppendEntriesResponse{9U, false, 0U, 10U, 1U},
      AppendEntriesResponse{9U, false, 0U, std::nullopt, 0U},
      AppendEntriesResponse{9U, false, std::numeric_limits<LogIndex>::max(), std::nullopt, 1U},
      InstallSnapshotRequest{9U, 2U, std::move(future_term_snapshot)},
      InstallSnapshotRequest{9U, 2U, std::move(exhausted_index_snapshot)},
      InstallSnapshotResponse{9U, true, 0U},
      InstallSnapshotResponse{9U, false, std::numeric_limits<LogIndex>::max()},
      ReadBarrierResponse{9U, 0U, true},
  };
  for (const Message& message : malformed) {
    auto node = RaftNode::create(1U, {1U, 2U, 3U});
    ASSERT_TRUE(node.has_value());
    auto election = node->start_election();
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    ASSERT_EQ(node->role(), Role::kCandidate);
    const PersistentState before = node->persistent_state();

    auto rejected = node->receive(2U, message);

    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
    EXPECT_EQ(node->persistent_state(), before);
    EXPECT_EQ(node->role(), Role::kCandidate);
  }
}

TEST(RaftNodeTest, RejectsDivergentBytesAtMatchingTermAndIndex) {
  PersistentState state{};
  state.current_term = 1U;
  state.log.push_back(LogEntry{1U, 1U, 1U, {std::byte{0x11}}});
  auto node = RaftNode::create(1U, {1U, 2U, 3U}, state);
  ASSERT_TRUE(node.has_value());

  auto divergent = node->receive(
      2U, AppendEntriesRequest{1U, 2U, 0U, 0U, {LogEntry{1U, 1U, 1U, {std::byte{0x22}}}}, 0U});

  ASSERT_FALSE(divergent.has_value());
  EXPECT_EQ(divergent.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(node->persistent_state(), state);
}

TEST(RaftNodeTest, RejectsCommittedOverwriteBeforeObservingHigherTerm) {
  PersistentState state{};
  state.current_term = 2U;
  state.voted_for = 3U;
  state.log = {LogEntry{1U, 1U, 1U, {std::byte{0x11}}}, LogEntry{2U, 2U, 1U, {std::byte{0x22}}}};
  state.commit_index = 2U;
  state.applied_index = 1U;
  const std::vector<AppendEntriesRequest> hostile{
      AppendEntriesRequest{3U, 2U, 0U, 0U, {LogEntry{1U, 3U, 1U, {std::byte{0x33}}}}, 0U},
      AppendEntriesRequest{3U, 2U, 0U, 0U, {LogEntry{1U, 1U, 1U, {std::byte{0x44}}}}, 0U},
  };

  for (const AppendEntriesRequest& request : hostile) {
    SCOPED_TRACE(request.entries.front().term);
    auto node = RaftNode::create(1U, {1U, 2U, 3U}, state);
    ASSERT_TRUE(node.has_value()) << node.error().to_string();

    auto rejected = node->receive(2U, request);

    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
    EXPECT_EQ(node->persistent_state(), state);
    EXPECT_EQ(node->current_term(), 2U);
    EXPECT_EQ(node->role(), Role::kFollower);
  }
}

TEST(RaftNodeTest, RejectsAppendPrefixBeforeSnapshotWithoutReadingCompactedEntry) {
  PersistentState state{};
  state.current_term = 2U;
  state.voted_for = 3U;
  state.snapshot.last_included_index = 1U;
  state.snapshot.last_included_term = 1U;
  state.snapshot.manifest_generation = 1U;
  state.snapshot.configuration_index = 1U;
  state.snapshot.voters = {1U, 2U, 3U};
  state.commit_index = 1U;
  state.applied_index = 1U;

  for (const Term term : {2U, 3U}) {
    SCOPED_TRACE(term);
    auto node = RaftNode::create(1U, {1U, 2U, 3U}, state);
    ASSERT_TRUE(node.has_value());

    auto rejected = node->receive(
        2U, AppendEntriesRequest{term, 2U, 0U, 0U, {LogEntry{1U, 1U, 1U, {std::byte{0x11}}}}, 0U});

    ASSERT_TRUE(rejected.has_value()) << rejected.error().to_string();
    ASSERT_EQ(rejected->outbound.size(), 1U);
    const auto& response = std::get<AppendEntriesResponse>(rejected->outbound.front().message);
    EXPECT_EQ(response, (AppendEntriesResponse{term, false, 1U, std::nullopt, 2U}));
    EXPECT_EQ(rejected->persistent_state.has_value(), term > state.current_term);
    EXPECT_EQ(node->current_term(), term);
    EXPECT_EQ(node->last_log_index(), 1U);
    EXPECT_EQ(node->commit_index(), 1U);
    EXPECT_TRUE(node->persistent_state().log.empty());
    if (term > state.current_term) {
      EXPECT_EQ(rejected->persistent_state,
                std::optional<PersistentState>{node->persistent_state()});
      EXPECT_FALSE(node->persistent_state().voted_for.has_value());
    } else {
      EXPECT_EQ(node->persistent_state(), state);
    }
  }
}

TEST(RaftNodeTest, RejectsIndexExhaustionBeforeNextIndexCanWrap) {
  PersistentState state{};
  state.current_term = 1U;
  state.snapshot.last_included_index = std::numeric_limits<LogIndex>::max() - 1U;
  state.snapshot.last_included_term = 1U;
  state.snapshot.manifest_generation = 1U;
  state.snapshot.voters = {1U};
  state.commit_index = state.snapshot.last_included_index;
  state.applied_index = state.snapshot.last_included_index;
  auto node = RaftNode::create(1U, {1U}, state);
  ASSERT_TRUE(node.has_value());
  auto election = node->start_election();
  ASSERT_TRUE(election.has_value());
  ASSERT_EQ(node->role(), Role::kLeader);

  auto exhausted = node->propose(1U, {std::byte{0x11}});

  ASSERT_FALSE(exhausted.has_value());
  EXPECT_EQ(exhausted.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(node->last_log_index(), std::numeric_limits<LogIndex>::max() - 1U);
}

TEST(RaftNodeTest, CommitsMembershipChangeUnderJointQuorumsAndRemovesLeader) {
  auto node = RaftNode::create(1U, {1U, 2U, 3U});
  ASSERT_TRUE(node.has_value());
  EXPECT_FALSE(node->joint_membership_active());
  EXPECT_TRUE(std::ranges::equal(node->committed_voters(), std::vector<NodeId>{1U, 2U, 3U}));
  ASSERT_TRUE(node->start_election().has_value());
  auto elected = node->receive(2U, RequestVoteResponse{1U, true});
  ASSERT_TRUE(elected.has_value()) << elected.error().to_string();
  ASSERT_EQ(node->role(), Role::kLeader);

  auto joint = node->begin_membership_change({2U, 3U, 4U});
  ASSERT_TRUE(joint.has_value()) << joint.error().to_string();
  EXPECT_TRUE(joint->persistent_state.has_value());
  auto joint_retry = node->begin_membership_change({2U, 3U, 4U});
  ASSERT_TRUE(joint_retry.has_value()) << joint_retry.error().to_string();
  EXPECT_FALSE(joint_retry->persistent_state.has_value());
  EXPECT_TRUE(joint_retry->outbound.empty());
  EXPECT_EQ(node->begin_membership_change({1U, 2U, 4U}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(node->joint_membership_active());
  EXPECT_TRUE(std::ranges::equal(node->voters(), std::vector<NodeId>{1U, 2U, 3U, 4U}));
  EXPECT_EQ(node->commit_index(), 0U);
  EXPECT_FALSE(node->finalize_membership_change().has_value());

  auto old_majority = node->receive(2U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U});
  ASSERT_TRUE(old_majority.has_value()) << old_majority.error().to_string();
  EXPECT_EQ(node->commit_index(), 0U);
  auto both_majorities = node->receive(4U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U});
  ASSERT_TRUE(both_majorities.has_value()) << both_majorities.error().to_string();
  EXPECT_EQ(node->commit_index(), 1U);

  auto final = node->finalize_membership_change();
  ASSERT_TRUE(final.has_value()) << final.error().to_string();
  EXPECT_EQ(node->commit_index(), 1U);
  auto final_retry = node->finalize_membership_change();
  ASSERT_TRUE(final_retry.has_value()) << final_retry.error().to_string();
  EXPECT_FALSE(final_retry->persistent_state.has_value());
  EXPECT_TRUE(final_retry->outbound.empty());
  auto final_old = node->receive(2U, AppendEntriesResponse{1U, true, 2U, std::nullopt, 0U});
  ASSERT_TRUE(final_old.has_value()) << final_old.error().to_string();
  EXPECT_EQ(node->commit_index(), 1U);
  auto final_both = node->receive(4U, AppendEntriesResponse{1U, true, 2U, std::nullopt, 0U});
  ASSERT_TRUE(final_both.has_value()) << final_both.error().to_string();
  EXPECT_EQ(node->commit_index(), 2U);
  EXPECT_FALSE(node->joint_membership_active());
  EXPECT_TRUE(std::ranges::equal(node->voters(), std::vector<NodeId>{2U, 3U, 4U}));
  EXPECT_EQ(node->role(), Role::kFollower);
  EXPECT_FALSE(node->start_election().has_value());

  auto recovered = RaftNode::create(4U, {1U, 2U, 3U}, node->persistent_state());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_TRUE(std::ranges::equal(recovered->voters(), std::vector<NodeId>{2U, 3U, 4U}));
  EXPECT_FALSE(recovered->joint_membership_active());
}

TEST(RaftNodeTest, ExactRetainedProposalSuppressesRetryAndAddsPriorTermProgressNoop) {
  auto leader = RaftNode::create(1U, {1U, 2U, 3U});
  ASSERT_TRUE(leader.has_value());
  ASSERT_TRUE(leader->start_election().has_value());
  ASSERT_TRUE(leader->receive(2U, RequestVoteResponse{1U, true}).has_value());
  const std::vector<std::byte> payload{std::byte{0x42U}};
  auto first = leader->propose_exact_retained(1U, payload);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(first->persistent_state.has_value());
  EXPECT_EQ(leader->last_log_index(), 1U);
  auto retry = leader->propose_exact_retained(1U, payload);
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  EXPECT_FALSE(retry->persistent_state.has_value());
  EXPECT_TRUE(retry->outbound.empty());
  EXPECT_EQ(leader->last_log_index(), 1U);

  auto restarted = RaftNode::create(1U, {1U, 2U, 3U}, leader->persistent_state());
  ASSERT_TRUE(restarted.has_value()) << restarted.error().to_string();
  ASSERT_TRUE(restarted->start_election().has_value());
  ASSERT_TRUE(restarted->receive(2U, RequestVoteResponse{2U, true}).has_value());
  auto prior_term = restarted->propose_exact_retained(1U, payload);
  ASSERT_TRUE(prior_term.has_value()) << prior_term.error().to_string();
  ASSERT_TRUE(prior_term->persistent_state.has_value());
  EXPECT_EQ(restarted->last_log_index(), 2U);
  EXPECT_EQ(restarted->persistent_state().log.back().type, kLeaderNoopEntryType);
  EXPECT_TRUE(restarted->persistent_state().log.back().payload.empty());
  auto noop_retry = restarted->propose_exact_retained(1U, payload);
  ASSERT_TRUE(noop_retry.has_value()) << noop_retry.error().to_string();
  EXPECT_FALSE(noop_retry->persistent_state.has_value());
  EXPECT_EQ(restarted->last_log_index(), 2U);
}

TEST(RaftNodeTest, JointElectionRequiresOldAndNewMajorities) {
  auto node = RaftNode::create(1U, {1U, 2U, 3U});
  ASSERT_TRUE(node.has_value());
  ASSERT_TRUE(node->start_election().has_value());
  ASSERT_TRUE(node->receive(2U, RequestVoteResponse{1U, true}).has_value());
  ASSERT_EQ(node->role(), Role::kLeader);
  ASSERT_TRUE(node->begin_membership_change({2U, 3U, 4U}).has_value());

  const PersistentState joint_state = node->persistent_state();
  auto restarted = RaftNode::create(1U, {1U, 2U, 3U}, joint_state);
  ASSERT_TRUE(restarted.has_value()) << restarted.error().to_string();
  auto election = restarted->start_election();
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_EQ(restarted->role(), Role::kCandidate);
  ASSERT_TRUE(restarted->receive(2U, RequestVoteResponse{2U, true}).has_value());
  EXPECT_EQ(restarted->role(), Role::kCandidate);
  ASSERT_TRUE(restarted->receive(4U, RequestVoteResponse{2U, true}).has_value());
  EXPECT_EQ(restarted->role(), Role::kLeader);

  auto prior_term_retry = restarted->begin_membership_change({2U, 3U, 4U});
  ASSERT_TRUE(prior_term_retry.has_value()) << prior_term_retry.error().to_string();
  ASSERT_TRUE(prior_term_retry->persistent_state.has_value());
  EXPECT_EQ(restarted->last_log_index(), 2U);
  EXPECT_EQ(restarted->persistent_state().log.back().type, kLeaderNoopEntryType);
  EXPECT_TRUE(restarted->persistent_state().log.back().payload.empty());
}

TEST(RaftNodeTest, RejectsProposalThatCannotFitFullPersistentStateBeforeMutation) {
  auto node = RaftNode::create(1U, {1U});
  ASSERT_TRUE(node.has_value()) << node.error().to_string();
  ASSERT_TRUE(node->start_election().has_value());
  ASSERT_EQ(node->role(), Role::kLeader);
  const PersistentState before = node->persistent_state();

  auto rejected =
      node->propose(1U, std::vector<std::byte>(RaftLimits{}.maximum_entry_bytes, std::byte{0x42U}));

  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(node->persistent_state(), before);
}

TEST(RaftNodeTest, RejectsOversizedFollowerSuffixBeforeHigherTermObservation) {
  RaftLimits limits;
  limits.maximum_persistent_state_bytes =
      kRaftPersistentStateFixedSizeV1 + kRaftPersistentLogEntryFixedSizeV1 + 1U;
  auto node = RaftNode::create(1U, {1U, 2U}, {}, limits);
  ASSERT_TRUE(node.has_value()) << node.error().to_string();
  const PersistentState before = node->persistent_state();

  auto rejected = node->receive(
      2U, AppendEntriesRequest{.term = 2U,
                               .leader_id = 2U,
                               .previous_log_index = 0U,
                               .previous_log_term = 0U,
                               .entries = {{1U, 2U, 1U, {std::byte{0x11U}, std::byte{0x22U}}}},
                               .leader_commit = 0U});

  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(node->current_term(), 0U);
  EXPECT_EQ(node->role(), Role::kFollower);
  EXPECT_EQ(node->persistent_state(), before);
}

TEST(RaftNodeTest, RejectsReservedProposalsLearnerElectionsAndInvalidMembershipHistory) {
  auto learner = RaftNode::create(4U, {1U, 2U, 3U});
  ASSERT_TRUE(learner.has_value());
  EXPECT_EQ(learner->start_election().error().code(), common::StatusCode::kUnavailable);

  auto leader = RaftNode::create(1U, {1U});
  ASSERT_TRUE(leader.has_value());
  ASSERT_TRUE(leader->start_election().has_value());
  EXPECT_EQ(leader->propose(kJointMembershipEntryType, {}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(leader->propose(kLeaderNoopEntryType, {}).error().code(),
            common::StatusCode::kInvalidArgument);

  PersistentState invalid_noop;
  invalid_noop.current_term = 1U;
  invalid_noop.log.push_back(LogEntry{1U, 1U, kLeaderNoopEntryType, {std::byte{0x01U}}});
  EXPECT_EQ(RaftNode::create(1U, {1U}, invalid_noop).error().code(),
            common::StatusCode::kInvalidArgument);

  const PersistentState before = learner->persistent_state();
  auto malformed_noop = learner->receive(
      1U, AppendEntriesRequest{
              1U, 1U, 0U, 0U, {LogEntry{1U, 1U, kLeaderNoopEntryType, {std::byte{0x01U}}}}, 0U});
  ASSERT_FALSE(malformed_noop.has_value());
  EXPECT_EQ(malformed_noop.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(learner->persistent_state(), before);

  auto final_payload = encode_membership_command_v1(
      FinalMembershipCommand{.joint_index = 1U, .new_voters = {1U, 2U, 3U}});
  ASSERT_TRUE(final_payload.has_value());
  auto malformed = learner->receive(
      1U,
      AppendEntriesRequest{1U,
                           1U,
                           0U,
                           0U,
                           {LogEntry{1U, 1U, kFinalMembershipEntryType, std::move(*final_payload)}},
                           0U});
  ASSERT_FALSE(malformed.has_value());
  EXPECT_EQ(malformed.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(learner->persistent_state(), before);
}

TEST(RaftNodeTest, RejectsUnprovenNonvoterAppendBeforeHigherTermObservation) {
  auto leader = RaftNode::create(1U, {1U, 2U, 3U});
  ASSERT_TRUE(leader.has_value());
  ASSERT_TRUE(leader->start_election().has_value());
  ASSERT_TRUE(leader->receive(2U, RequestVoteResponse{1U, true}).has_value());
  ASSERT_EQ(leader->role(), Role::kLeader);
  const PersistentState before = leader->persistent_state();

  auto rejected = leader->receive(4U, AppendEntriesRequest{2U, 4U, 0U, 0U, {}, 0U});

  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(leader->current_term(), 1U);
  EXPECT_EQ(leader->role(), Role::kLeader);
  EXPECT_EQ(leader->leader_id(), 1U);
  EXPECT_EQ(leader->persistent_state(), before);
}

TEST(RaftNodeTest, LaggingNewVoterAcceptsConfigurationFromNewOnlyLeader) {
  auto node = RaftNode::create(3U, {1U, 2U, 3U});
  ASSERT_TRUE(node.has_value());
  auto joint = encode_membership_command_v1(
      JointMembershipCommand{.old_voters = {1U, 2U, 3U}, .new_voters = {3U, 4U, 5U}});
  auto final = encode_membership_command_v1(
      FinalMembershipCommand{.joint_index = 1U, .new_voters = {3U, 4U, 5U}});
  ASSERT_TRUE(joint.has_value());
  ASSERT_TRUE(final.has_value());

  auto caught_up = node->receive(
      4U, AppendEntriesRequest{2U,
                               4U,
                               0U,
                               0U,
                               {LogEntry{1U, 2U, kJointMembershipEntryType, std::move(*joint)},
                                LogEntry{2U, 2U, kFinalMembershipEntryType, std::move(*final)}},
                               2U});

  ASSERT_TRUE(caught_up.has_value()) << caught_up.error().to_string();
  EXPECT_EQ(node->commit_index(), 2U);
  EXPECT_FALSE(node->joint_membership_active());
  EXPECT_TRUE(std::ranges::equal(node->voters(), std::vector<NodeId>{3U, 4U, 5U}));
  ASSERT_EQ(caught_up->outbound.size(), 1U);
  EXPECT_TRUE(std::get<AppendEntriesResponse>(caught_up->outbound.front().message).success);
}

TEST(RaftNodeTest, SnapshotMembershipCheckpointSupersedesBootstrapConfiguration) {
  PersistentState state{};
  state.current_term = 2U;
  state.snapshot.last_included_index = 5U;
  state.snapshot.last_included_term = 2U;
  state.snapshot.manifest_generation = 1U;
  state.snapshot.configuration_index = 4U;
  state.snapshot.voters = {2U, 3U, 4U};
  state.commit_index = 5U;
  state.applied_index = 5U;
  auto recovered = RaftNode::create(4U, {1U, 2U, 3U}, state);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_TRUE(std::ranges::equal(recovered->voters(), std::vector<NodeId>{2U, 3U, 4U}));

  state.snapshot.voters = {2U, 2U, 4U};
  EXPECT_EQ(RaftNode::create(4U, {1U, 2U, 3U}, state).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(RaftNodeTest, CompactsAppliedPrefixAndInstallsSnapshotBeforeAcknowledgingLeader) {
  auto leader = RaftNode::create(1U, {1U, 2U, 3U});
  auto follower = RaftNode::create(2U, {1U, 2U, 3U});
  ASSERT_TRUE(leader.has_value());
  ASSERT_TRUE(follower.has_value());
  ASSERT_TRUE(leader->start_election().has_value());
  ASSERT_TRUE(leader->receive(3U, RequestVoteResponse{1U, true}).has_value());
  ASSERT_EQ(leader->role(), Role::kLeader);
  for (std::uint8_t value = 1U; value <= 3U; ++value) {
    ASSERT_TRUE(leader->propose(1U, {static_cast<std::byte>(value)}).has_value());
    ASSERT_TRUE(
        leader->receive(3U, AppendEntriesResponse{1U, true, value, std::nullopt, 0U}).has_value());
  }
  ASSERT_TRUE(leader->mark_applied(3U).has_value());
  SnapshotMetadata snapshot{};
  snapshot.last_included_index = 2U;
  snapshot.last_included_term = 1U;
  snapshot.manifest_generation = 7U;
  auto compacted = leader->compact_snapshot(snapshot);
  ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
  ASSERT_TRUE(compacted->persistent_state.has_value());
  EXPECT_EQ(leader->persistent_state().snapshot.last_included_index, 2U);
  EXPECT_TRUE(std::ranges::equal(leader->persistent_state().snapshot.voters,
                                 std::vector<NodeId>{1U, 2U, 3U}));
  ASSERT_EQ(leader->persistent_state().log.size(), 1U);
  EXPECT_EQ(leader->persistent_state().log.front().index, 3U);

  auto rewind = leader->receive(2U, AppendEntriesResponse{1U, false, 0U, std::nullopt, 1U});
  ASSERT_TRUE(rewind.has_value()) << rewind.error().to_string();
  ASSERT_EQ(rewind->outbound.size(), 1U);
  const auto* request = std::get_if<InstallSnapshotRequest>(&rewind->outbound.front().message);
  ASSERT_NE(request, nullptr);
  auto pending = follower->receive(1U, *request);
  ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
  ASSERT_TRUE(pending->snapshot_install.has_value());
  EXPECT_TRUE(pending->outbound.empty());
  EXPECT_EQ(follower->persistent_state().snapshot.last_included_index, 0U);

  auto installed = follower->complete_snapshot_install(request->leader_id, request->snapshot, true);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  ASSERT_TRUE(installed->persistent_state.has_value());
  EXPECT_EQ(follower->commit_index(), 2U);
  EXPECT_EQ(follower->applied_index(), 2U);
  auto duplicate_install = follower->complete_snapshot_install(1U, request->snapshot, true);
  ASSERT_FALSE(duplicate_install.has_value());
  EXPECT_EQ(duplicate_install.error().code(), common::StatusCode::kInvalidArgument);
  ASSERT_EQ(installed->outbound.size(), 1U);
  const Message response = installed->outbound.front().message;
  auto caught_up = leader->receive(2U, response);
  ASSERT_TRUE(caught_up.has_value()) << caught_up.error().to_string();
  ASSERT_EQ(caught_up->outbound.size(), 1U);
  const Message suffix = caught_up->outbound.front().message;
  auto appended = follower->receive(1U, suffix);
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(follower->last_log_index(), 3U);
  EXPECT_EQ(follower->commit_index(), 3U);
}

TEST(RaftNodeTest, RejectsSnapshotBoundaryWithJointMembershipState) {
  auto joint = encode_membership_command_v1(
      JointMembershipCommand{.old_voters = {1U, 2U, 3U}, .new_voters = {2U, 3U, 4U}});
  auto final = encode_membership_command_v1(
      FinalMembershipCommand{.joint_index = 1U, .new_voters = {2U, 3U, 4U}});
  ASSERT_TRUE(joint.has_value()) << joint.error().to_string();
  ASSERT_TRUE(final.has_value()) << final.error().to_string();
  PersistentState initial{};
  initial.current_term = 1U;
  initial.log = {
      LogEntry{1U, 1U, kJointMembershipEntryType, std::move(*joint)},
      LogEntry{2U, 1U, kFinalMembershipEntryType, std::move(*final)},
      LogEntry{3U, 1U, 1U, {std::byte{0x33U}}},
  };
  initial.commit_index = 3U;
  initial.applied_index = 3U;
  auto node = RaftNode::create(2U, {1U, 2U, 3U}, initial);
  ASSERT_TRUE(node.has_value()) << node.error().to_string();
  ASSERT_FALSE(node->joint_membership_active());
  ASSERT_TRUE(std::ranges::equal(node->voters(), std::vector<NodeId>{2U, 3U, 4U}));
  const PersistentState before = node->persistent_state();
  SnapshotMetadata snapshot{};
  snapshot.last_included_index = 1U;
  snapshot.last_included_term = 1U;
  snapshot.manifest_generation = 17U;

  auto rejected_metadata = node->prepare_snapshot_metadata(snapshot);
  auto rejected = node->compact_snapshot(snapshot);

  ASSERT_FALSE(rejected_metadata.has_value());
  EXPECT_EQ(rejected_metadata.error().code(), common::StatusCode::kInvalidArgument);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(node->persistent_state(), before);
  EXPECT_FALSE(node->joint_membership_active());
  EXPECT_TRUE(std::ranges::equal(node->voters(), std::vector<NodeId>{2U, 3U, 4U}));
}

TEST(RaftNodeTest, CompactsStablePrefixBeforeLaterMembershipChange) {
  auto joint = encode_membership_command_v1(
      JointMembershipCommand{.old_voters = {1U, 2U, 3U}, .new_voters = {2U, 3U, 4U}});
  auto final = encode_membership_command_v1(
      FinalMembershipCommand{.joint_index = 2U, .new_voters = {2U, 3U, 4U}});
  ASSERT_TRUE(joint.has_value()) << joint.error().to_string();
  ASSERT_TRUE(final.has_value()) << final.error().to_string();
  PersistentState initial{};
  initial.current_term = 1U;
  initial.log = {
      LogEntry{1U, 1U, 1U, {std::byte{0x11U}}},
      LogEntry{2U, 1U, kJointMembershipEntryType, std::move(*joint)},
      LogEntry{3U, 1U, kFinalMembershipEntryType, std::move(*final)},
      LogEntry{4U, 1U, 1U, {std::byte{0x44U}}},
  };
  initial.commit_index = 4U;
  initial.applied_index = 4U;
  auto node = RaftNode::create(2U, {1U, 2U, 3U}, initial);
  ASSERT_TRUE(node.has_value()) << node.error().to_string();
  SnapshotMetadata snapshot{};
  snapshot.last_included_index = 1U;
  snapshot.last_included_term = 1U;
  snapshot.manifest_generation = 18U;

  auto prepared = node->prepare_snapshot_metadata(snapshot);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  EXPECT_EQ(prepared->configuration_index, 0U);
  EXPECT_TRUE(std::ranges::equal(prepared->voters, std::vector<NodeId>{1U, 2U, 3U}));
  EXPECT_EQ(node->persistent_state(), initial);
  auto compacted = node->compact_snapshot(snapshot);

  ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
  ASSERT_TRUE(compacted->persistent_state.has_value());
  const PersistentState& state = node->persistent_state();
  EXPECT_EQ(state.snapshot.last_included_index, 1U);
  EXPECT_EQ(state.snapshot.configuration_index, 0U);
  EXPECT_TRUE(std::ranges::equal(state.snapshot.voters, std::vector<NodeId>{1U, 2U, 3U}));
  EXPECT_TRUE(std::ranges::equal(node->voters(), std::vector<NodeId>{2U, 3U, 4U}));
  ASSERT_EQ(state.log.size(), 3U);
  EXPECT_EQ(state.log.front().index, 2U);
  EXPECT_EQ(compacted->persistent_state, std::optional<PersistentState>{state});

  auto reopened = RaftNode::create(2U, {1U, 2U, 3U}, state);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_FALSE(reopened->joint_membership_active());
  EXPECT_TRUE(std::ranges::equal(reopened->voters(), std::vector<NodeId>{2U, 3U, 4U}));
}

TEST(RaftNodeTest, SerializesPendingSnapshotInstallAgainstLocalCompaction) {
  PersistentState state{};
  state.current_term = 2U;
  state.log = {LogEntry{1U, 1U, 1U, {std::byte{0x11}}}, LogEntry{2U, 2U, 1U, {std::byte{0x22}}}};
  state.commit_index = 2U;
  state.applied_index = 2U;
  auto follower = RaftNode::create(1U, {1U, 2U, 3U}, state);
  ASSERT_TRUE(follower.has_value());

  SnapshotMetadata incoming{};
  incoming.last_included_index = 1U;
  incoming.last_included_term = 1U;
  incoming.manifest_generation = 9U;
  incoming.part_set_checksum.fill(std::byte{0x99});
  incoming.voters = {1U, 2U, 3U};
  auto pending = follower->receive(2U, InstallSnapshotRequest{2U, 2U, incoming});
  ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
  ASSERT_TRUE(pending->snapshot_install.has_value());
  ASSERT_TRUE(pending->outbound.empty());
  const PersistentState before_compaction = follower->persistent_state();

  SnapshotMetadata local = incoming;
  local.manifest_generation = 7U;
  local.part_set_checksum.fill(std::byte{0x77});
  auto compacted = follower->compact_snapshot(local);

  ASSERT_FALSE(compacted.has_value());
  EXPECT_EQ(compacted.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(follower->persistent_state(), before_compaction);

  auto installed = follower->complete_snapshot_install(2U, incoming, true);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  ASSERT_TRUE(installed->persistent_state.has_value());
  EXPECT_EQ(follower->persistent_state().snapshot, incoming);
  ASSERT_EQ(follower->persistent_state().log.size(), 1U);
  EXPECT_EQ(follower->persistent_state().log.front(), state.log.back());
  ASSERT_EQ(installed->outbound.size(), 1U);
  EXPECT_EQ(std::get<InstallSnapshotResponse>(installed->outbound.front().message),
            (InstallSnapshotResponse{2U, true, 1U}));

  auto rejected_follower = RaftNode::create(1U, {1U, 2U, 3U}, state);
  ASSERT_TRUE(rejected_follower.has_value());
  auto rejected_pending = rejected_follower->receive(2U, InstallSnapshotRequest{2U, 2U, incoming});
  ASSERT_TRUE(rejected_pending.has_value()) << rejected_pending.error().to_string();
  ASSERT_TRUE(rejected_pending->snapshot_install.has_value());
  auto declined = rejected_follower->complete_snapshot_install(2U, incoming, false);
  ASSERT_TRUE(declined.has_value()) << declined.error().to_string();
  EXPECT_FALSE(declined->persistent_state.has_value());
  ASSERT_EQ(declined->outbound.size(), 1U);
  EXPECT_EQ(std::get<InstallSnapshotResponse>(declined->outbound.front().message),
            (InstallSnapshotResponse{2U, false, 0U}));

  auto compacted_after_rejection = rejected_follower->compact_snapshot(local);
  ASSERT_TRUE(compacted_after_rejection.has_value())
      << compacted_after_rejection.error().to_string();
  ASSERT_TRUE(compacted_after_rejection->persistent_state.has_value());
  EXPECT_EQ(rejected_follower->persistent_state().snapshot, local);
}

TEST(RaftNodeTest, PreservesPendingSnapshotAfterCommittedPrefixConflict) {
  PersistentState state{};
  state.current_term = 2U;
  state.log = {LogEntry{1U, 1U, 1U, {std::byte{0x11}}}, LogEntry{2U, 2U, 1U, {std::byte{0x22}}}};
  state.commit_index = 2U;
  state.applied_index = 2U;
  auto follower = RaftNode::create(1U, {1U, 2U, 3U}, state);
  ASSERT_TRUE(follower.has_value());

  SnapshotMetadata conflicting{};
  conflicting.last_included_index = 1U;
  conflicting.last_included_term = 2U;
  conflicting.manifest_generation = 9U;
  conflicting.part_set_checksum.fill(std::byte{0x99});
  conflicting.voters = {1U, 2U, 3U};
  auto pending = follower->receive(2U, InstallSnapshotRequest{2U, 2U, conflicting});
  ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
  ASSERT_TRUE(pending->snapshot_install.has_value());
  const PersistentState before_completion = follower->persistent_state();

  auto rejected = follower->complete_snapshot_install(2U, conflicting, true);

  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(follower->persistent_state(), before_completion);

  auto declined = follower->complete_snapshot_install(2U, conflicting, false);
  ASSERT_TRUE(declined.has_value()) << declined.error().to_string();
  EXPECT_FALSE(declined->persistent_state.has_value());
  ASSERT_EQ(declined->outbound.size(), 1U);
  EXPECT_EQ(std::get<InstallSnapshotResponse>(declined->outbound.front().message),
            (InstallSnapshotResponse{2U, false, 0U}));
}

TEST(RaftNodeTest, CoalescesDuplicateAndRejectsCompetingPendingSnapshots) {
  PersistentState state{};
  state.current_term = 2U;
  state.voted_for = 2U;
  state.log = {LogEntry{1U, 1U, 1U, {std::byte{0x11}}}, LogEntry{2U, 2U, 1U, {std::byte{0x22}}}};
  state.commit_index = 2U;
  state.applied_index = 2U;

  SnapshotMetadata first{};
  first.last_included_index = 1U;
  first.last_included_term = 1U;
  first.manifest_generation = 9U;
  first.part_set_checksum.fill(std::byte{0x99});
  first.voters = {1U, 2U, 3U};
  SnapshotMetadata second = first;
  second.last_included_index = 2U;
  second.last_included_term = 2U;
  second.manifest_generation = 10U;
  second.part_set_checksum.fill(std::byte{0xaa});

  auto follower = RaftNode::create(1U, {1U, 2U, 3U}, state);
  ASSERT_TRUE(follower.has_value());
  auto pending = follower->receive(2U, InstallSnapshotRequest{2U, 2U, first});
  ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
  ASSERT_TRUE(pending->snapshot_install.has_value());

  auto duplicate = follower->receive(2U, InstallSnapshotRequest{2U, 2U, first});
  ASSERT_TRUE(duplicate.has_value()) << duplicate.error().to_string();
  EXPECT_FALSE(duplicate->snapshot_install.has_value());
  EXPECT_TRUE(duplicate->outbound.empty());
  EXPECT_FALSE(duplicate->persistent_state.has_value());

  auto competing = follower->receive(2U, InstallSnapshotRequest{2U, 2U, second});
  ASSERT_TRUE(competing.has_value()) << competing.error().to_string();
  EXPECT_FALSE(competing->snapshot_install.has_value());
  ASSERT_EQ(competing->outbound.size(), 1U);
  EXPECT_EQ(std::get<InstallSnapshotResponse>(competing->outbound.front().message),
            (InstallSnapshotResponse{2U, false, 0U}));
  EXPECT_EQ(follower->persistent_state(), state);

  auto installed = follower->complete_snapshot_install(2U, first, true);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(follower->persistent_state().snapshot, first);

  auto term_follower = RaftNode::create(1U, {1U, 2U, 3U}, state);
  ASSERT_TRUE(term_follower.has_value());
  auto term_pending = term_follower->receive(2U, InstallSnapshotRequest{2U, 2U, first});
  ASSERT_TRUE(term_pending.has_value()) << term_pending.error().to_string();
  ASSERT_TRUE(term_pending->snapshot_install.has_value());

  auto higher = term_follower->receive(3U, InstallSnapshotRequest{3U, 3U, second});
  ASSERT_TRUE(higher.has_value()) << higher.error().to_string();
  EXPECT_FALSE(higher->snapshot_install.has_value());
  EXPECT_EQ(higher->persistent_state,
            std::optional<PersistentState>{term_follower->persistent_state()});
  EXPECT_EQ(term_follower->current_term(), 3U);
  EXPECT_FALSE(term_follower->persistent_state().voted_for.has_value());
  ASSERT_EQ(higher->outbound.size(), 1U);
  EXPECT_EQ(std::get<InstallSnapshotResponse>(higher->outbound.front().message),
            (InstallSnapshotResponse{3U, false, 0U}));

  auto stale_completion = term_follower->complete_snapshot_install(2U, first, true);
  ASSERT_TRUE(stale_completion.has_value()) << stale_completion.error().to_string();
  EXPECT_FALSE(stale_completion->persistent_state.has_value());
  ASSERT_EQ(stale_completion->outbound.size(), 1U);
  EXPECT_EQ(std::get<InstallSnapshotResponse>(stale_completion->outbound.front().message),
            (InstallSnapshotResponse{3U, false, 0U}));

  auto retry = term_follower->receive(3U, InstallSnapshotRequest{3U, 3U, second});
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  EXPECT_TRUE(retry->snapshot_install.has_value());
  EXPECT_TRUE(retry->outbound.empty());
  auto retry_declined = term_follower->complete_snapshot_install(3U, second, false);
  ASSERT_TRUE(retry_declined.has_value()) << retry_declined.error().to_string();
}

TEST(RaftNodeTest, ConfirmsLinearizableReadAtCommittedIndexBeforeApplicationVisibility) {
  auto leader = RaftNode::create(1U, {1U, 2U, 3U});
  ASSERT_TRUE(leader.has_value());
  ASSERT_TRUE(leader->start_election().has_value());
  ASSERT_TRUE(leader->receive(2U, RequestVoteResponse{1U, true}).has_value());
  ASSERT_EQ(leader->role(), Role::kLeader);
  EXPECT_EQ(leader->begin_read_barrier().error().code(), common::StatusCode::kUnavailable);

  ASSERT_TRUE(leader->propose(1U, {std::byte{0x11}}).has_value());
  ASSERT_TRUE(
      leader->receive(2U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U}).has_value());
  ASSERT_EQ(leader->commit_index(), 1U);
  ASSERT_EQ(leader->applied_index(), 0U);

  auto started = leader->begin_read_barrier();
  ASSERT_TRUE(started.has_value()) << started.error().to_string();
  ASSERT_EQ(started->outbound.size(), 2U);
  EXPECT_FALSE(started->read_barrier_ready.has_value());
  const auto* request = std::get_if<ReadBarrierRequest>(&started->outbound.front().message);
  ASSERT_NE(request, nullptr);
  EXPECT_EQ(request->term, 1U);
  EXPECT_EQ(request->leader_id, 1U);
  EXPECT_NE(request->context, 0U);
  EXPECT_EQ(leader->begin_read_barrier().error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(leader->begin_membership_change({2U, 3U, 4U}).error().code(),
            common::StatusCode::kUnavailable);

  auto wrong = leader->receive(2U, ReadBarrierResponse{1U, request->context + 1U, true});
  ASSERT_TRUE(wrong.has_value());
  EXPECT_FALSE(wrong->read_barrier_ready.has_value());
  auto confirmed = leader->receive(2U, ReadBarrierResponse{1U, request->context, true});
  ASSERT_TRUE(confirmed.has_value()) << confirmed.error().to_string();
  const ReadBarrier* confirmed_barrier = completed_read_barrier(*confirmed);
  ASSERT_NE(confirmed_barrier, nullptr);
  EXPECT_EQ(*confirmed_barrier,
            (ReadBarrier{.term = 1U, .context = request->context, .read_index = 1U}));
  auto duplicate = leader->receive(2U, ReadBarrierResponse{1U, request->context, true});
  ASSERT_TRUE(duplicate.has_value());
  EXPECT_FALSE(duplicate->read_barrier_ready.has_value());
  EXPECT_LT(leader->applied_index(), confirmed_barrier->read_index);
  ASSERT_TRUE(leader->mark_applied(confirmed_barrier->read_index).has_value());
  EXPECT_EQ(leader->applied_index(), confirmed_barrier->read_index);
}

TEST(RaftNodeTest, ReadBarrierUsesFrozenJointConsensusQuorums) {
  auto leader = RaftNode::create(1U, {1U, 2U, 3U});
  ASSERT_TRUE(leader.has_value());
  ASSERT_TRUE(leader->start_election().has_value());
  ASSERT_TRUE(leader->receive(2U, RequestVoteResponse{1U, true}).has_value());
  ASSERT_TRUE(leader->propose(1U, {std::byte{0x11}}).has_value());
  ASSERT_TRUE(
      leader->receive(2U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U}).has_value());
  ASSERT_TRUE(leader->begin_membership_change({3U, 4U, 5U}).has_value());
  ASSERT_TRUE(leader->joint_membership_active());

  auto started = leader->begin_read_barrier();
  ASSERT_TRUE(started.has_value()) << started.error().to_string();
  ASSERT_EQ(started->outbound.size(), 4U);
  const auto context = std::get<ReadBarrierRequest>(started->outbound.front().message).context;
  auto old_only = leader->receive(2U, ReadBarrierResponse{1U, context, true});
  ASSERT_TRUE(old_only.has_value());
  EXPECT_FALSE(old_only->read_barrier_ready.has_value());
  auto new_only = leader->receive(4U, ReadBarrierResponse{1U, context, true});
  ASSERT_TRUE(new_only.has_value());
  EXPECT_FALSE(new_only->read_barrier_ready.has_value());
  auto both = leader->receive(3U, ReadBarrierResponse{1U, context, true});
  ASSERT_TRUE(both.has_value());
  const ReadBarrier* completed = completed_read_barrier(*both);
  ASSERT_NE(completed, nullptr);
  EXPECT_EQ(completed->read_index, 1U);
}

TEST(RaftNodeTest, LeadershipChangeAbandonsPendingReadBarrier) {
  auto leader = RaftNode::create(1U, {1U, 2U, 3U});
  ASSERT_TRUE(leader.has_value());
  ASSERT_TRUE(leader->start_election().has_value());
  ASSERT_TRUE(leader->receive(2U, RequestVoteResponse{1U, true}).has_value());
  ASSERT_TRUE(leader->propose(1U, {std::byte{0x11}}).has_value());
  ASSERT_TRUE(
      leader->receive(2U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U}).has_value());
  auto first = leader->begin_read_barrier();
  ASSERT_TRUE(first.has_value());
  const auto old_context = std::get<ReadBarrierRequest>(first->outbound.front().message).context;

  auto stepped_down = leader->receive(2U, ReadBarrierResponse{2U, old_context, true});
  ASSERT_TRUE(stepped_down.has_value());
  EXPECT_TRUE(stepped_down->persistent_state.has_value());
  EXPECT_EQ(leader->role(), Role::kFollower);
  ASSERT_TRUE(leader->start_election().has_value());
  ASSERT_TRUE(leader->receive(2U, RequestVoteResponse{3U, true}).has_value());
  ASSERT_TRUE(leader->propose(1U, {std::byte{0x22}}).has_value());
  ASSERT_TRUE(
      leader->receive(2U, AppendEntriesResponse{3U, true, 2U, std::nullopt, 0U}).has_value());
  auto second = leader->begin_read_barrier();
  ASSERT_TRUE(second.has_value());
  const auto new_context = std::get<ReadBarrierRequest>(second->outbound.front().message).context;
  EXPECT_NE(new_context, old_context);

  auto stale = leader->receive(2U, ReadBarrierResponse{3U, old_context, true});
  ASSERT_TRUE(stale.has_value());
  EXPECT_FALSE(stale->read_barrier_ready.has_value());
  auto current = leader->receive(2U, ReadBarrierResponse{3U, new_context, true});
  ASSERT_TRUE(current.has_value());
  const ReadBarrier* completed = completed_read_barrier(*current);
  ASSERT_NE(completed, nullptr);
  EXPECT_EQ(completed->term, 3U);
  EXPECT_EQ(completed->read_index, 2U);
}

TEST(RaftNodeTest, ReadBarrierProbeStepsDownReceiverAndSingleVoterCompletesLocally) {
  auto follower = RaftNode::create(2U, {1U, 2U, 3U});
  ASSERT_TRUE(follower.has_value());
  auto response = follower->receive(1U, ReadBarrierRequest{2U, 1U, 7U});
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  ASSERT_TRUE(response->persistent_state.has_value());
  ASSERT_EQ(response->outbound.size(), 1U);
  EXPECT_EQ(std::get<ReadBarrierResponse>(response->outbound.front().message),
            (ReadBarrierResponse{.term = 2U, .context = 7U, .accepted = true}));
  EXPECT_EQ(follower->leader_id(), 1U);

  auto single = RaftNode::create(1U, {1U});
  ASSERT_TRUE(single.has_value());
  ASSERT_TRUE(single->start_election().has_value());
  ASSERT_TRUE(single->propose(1U, {std::byte{0x11}}).has_value());
  ASSERT_EQ(single->commit_index(), 1U);
  auto barrier = single->begin_read_barrier();
  ASSERT_TRUE(barrier.has_value());
  EXPECT_TRUE(barrier->outbound.empty());
  const ReadBarrier* completed = completed_read_barrier(*barrier);
  ASSERT_NE(completed, nullptr);
  EXPECT_EQ(completed->read_index, 1U);
}

TEST(RaftNodeTest, RejectsNonvoterReadBarrierBeforeHigherTermObservation) {
  auto leader = RaftNode::create(1U, {1U, 2U, 3U});
  ASSERT_TRUE(leader.has_value());
  ASSERT_TRUE(leader->start_election().has_value());
  ASSERT_TRUE(leader->receive(2U, RequestVoteResponse{1U, true}).has_value());
  ASSERT_EQ(leader->role(), Role::kLeader);
  const PersistentState before = leader->persistent_state();

  auto rejected = leader->receive(4U, ReadBarrierRequest{2U, 4U, 7U});

  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(leader->current_term(), 1U);
  EXPECT_EQ(leader->role(), Role::kLeader);
  EXPECT_EQ(leader->leader_id(), 1U);
  EXPECT_EQ(leader->persistent_state(), before);
}

} // namespace
} // namespace chronos::raft
