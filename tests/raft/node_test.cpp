#include "chronos/raft/node.hpp"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <gtest/gtest.h>
#include <limits>
#include <map>
#include <set>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

struct Envelope {
  NodeId source{};
  OutboundMessage outbound;
};

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

TEST(RaftNodeTest, RejectsIndexExhaustionBeforeNextIndexCanWrap) {
  PersistentState state{};
  state.current_term = 1U;
  state.snapshot.last_included_index = std::numeric_limits<LogIndex>::max() - 1U;
  state.snapshot.last_included_term = 1U;
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

} // namespace
} // namespace chronos::raft
