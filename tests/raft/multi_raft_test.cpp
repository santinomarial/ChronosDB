#include "chronos/raft/multi_raft.hpp"

#include <cstddef>
#include <deque>
#include <gtest/gtest.h>
#include <limits>
#include <map>
#include <set>
#include <utility>

namespace chronos::raft {
namespace {

[[nodiscard]] GroupId group_id(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return GroupId{bytes};
}

struct Envelope {
  GroupOutboundMessage message;
};

class MultiCluster {
public:
  MultiCluster() {
    for (NodeId id = 1U; id <= 3U; ++id) {
      auto runtime = MultiRaftRuntime::create(id);
      EXPECT_TRUE(runtime.has_value());
      runtimes_.emplace(id, std::move(*runtime));
      EXPECT_TRUE(runtimes_.at(id).add_group(group_a, {1U, 2U, 3U}).is_ok());
      EXPECT_TRUE(runtimes_.at(id).add_group(group_b, {1U, 2U, 3U}).is_ok());
    }
  }

  void enqueue(MultiRaftTransition transition) {
    for (auto& message : transition.outbound)
      queue_.push_back(Envelope{std::move(message)});
    if (transition.persistence.has_value())
      latest_persistence_ = *transition.persistence;
  }

  void drain(const std::set<NodeId>& available = {1U, 2U, 3U}) {
    std::size_t count = 0U;
    while (!queue_.empty()) {
      ASSERT_LT(count++, 20'000U);
      Envelope envelope = std::move(queue_.front());
      queue_.pop_front();
      if (!available.contains(envelope.message.source) ||
          !available.contains(envelope.message.outbound.destination))
        continue;
      auto result = runtimes_.at(envelope.message.outbound.destination)
                        .receive(envelope.message.group_id, envelope.message.source,
                                 std::move(envelope.message.outbound.message));
      ASSERT_TRUE(result.has_value()) << result.error().to_string();
      enqueue(std::move(*result));
    }
  }

  [[nodiscard]] MultiRaftRuntime& runtime(const NodeId id) {
    return runtimes_.at(id);
  }
  [[nodiscard]] const GroupPersistentState& latest_persistence() const {
    return latest_persistence_;
  }

  GroupId group_a{group_id(std::byte{1})};
  GroupId group_b{group_id(std::byte{2})};

private:
  std::map<NodeId, MultiRaftRuntime> runtimes_;
  std::deque<Envelope> queue_;
  GroupPersistentState latest_persistence_{group_id(std::byte{3}), 1U, {}};
};

TEST(MultiRaftTest, IndependentGroupsElectDifferentLeadersAndCommitWithoutInterference) {
  MultiCluster cluster;
  auto election_a = cluster.runtime(1U).start_election(cluster.group_a);
  ASSERT_TRUE(election_a.has_value());
  cluster.enqueue(std::move(*election_a));
  cluster.drain();
  auto election_b = cluster.runtime(2U).start_election(cluster.group_b);
  ASSERT_TRUE(election_b.has_value());
  cluster.enqueue(std::move(*election_b));
  cluster.drain();
  EXPECT_EQ(cluster.runtime(1U).find_group(cluster.group_a)->role(), Role::kLeader);
  EXPECT_EQ(cluster.runtime(2U).find_group(cluster.group_b)->role(), Role::kLeader);

  auto proposal = cluster.runtime(1U).propose(cluster.group_a, 1U, {std::byte{0x44}});
  ASSERT_TRUE(proposal.has_value());
  cluster.enqueue(std::move(*proposal));
  cluster.drain();
  for (NodeId id = 1U; id <= 3U; ++id) {
    EXPECT_EQ(cluster.runtime(id).find_group(cluster.group_a)->commit_index(), 1U);
    EXPECT_EQ(cluster.runtime(id).find_group(cluster.group_b)->commit_index(), 0U);
  }

  auto failover = cluster.runtime(2U).start_election(cluster.group_a);
  ASSERT_TRUE(failover.has_value());
  cluster.enqueue(std::move(*failover));
  cluster.drain({2U, 3U});
  EXPECT_EQ(cluster.runtime(2U).find_group(cluster.group_a)->role(), Role::kLeader);
  EXPECT_EQ(cluster.runtime(2U).find_group(cluster.group_b)->role(), Role::kLeader);
  EXPECT_GT(cluster.latest_persistence().physical_sequence, 0U);
}

TEST(MultiRaftTest, ReopensPersistedGroupStateAtSharedPhysicalSequence) {
  const GroupId group = group_id(std::byte{9});
  auto runtime = MultiRaftRuntime::create(1U);
  ASSERT_TRUE(runtime.has_value());
  ASSERT_TRUE(runtime->add_group(group, {1U}).is_ok());
  auto elected = runtime->start_election(group);
  ASSERT_TRUE(elected.has_value());
  ASSERT_TRUE(elected->persistence.has_value());
  auto proposal = runtime->propose(group, 1U, {std::byte{1U}});
  ASSERT_TRUE(proposal.has_value());
  const auto& persistence = proposal->persistence;
  if (!persistence.has_value()) {
    ADD_FAILURE() << "expected proposal persistence";
    return;
  }

  auto reopened = MultiRaftRuntime::create(1U);
  ASSERT_TRUE(reopened.has_value());
  EXPECT_TRUE(
      reopened->add_group(group, {1U}, persistence->state, persistence->physical_sequence).is_ok());
  EXPECT_EQ(reopened->find_group(group)->commit_index(), 1U);
  EXPECT_EQ(reopened->find_group(group)->last_log_index(), 1U);
}

TEST(MultiRaftTest, OutboundOverflowFailsRuntimeClosed) {
  MultiRaftLimits limits{};
  limits.maximum_queued_outbound = 1U;
  auto runtime = MultiRaftRuntime::create(1U, limits);
  ASSERT_TRUE(runtime.has_value());
  const GroupId group = group_id(std::byte{7});
  ASSERT_TRUE(runtime->add_group(group, {1U, 2U, 3U}).is_ok());

  auto overflow = runtime->start_election(group);

  ASSERT_FALSE(overflow.has_value());
  EXPECT_EQ(overflow.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(runtime->failed());
  auto repeated = runtime->start_election(group);
  ASSERT_FALSE(repeated.has_value());
  EXPECT_EQ(repeated.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(runtime->group_count(), 1U);
}

TEST(MultiRaftTest, RejectsExhaustedPhysicalSequenceBeforeMutatingGroup) {
  const GroupId group = group_id(std::byte{6U});
  auto runtime = MultiRaftRuntime::create(1U);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const PersistentState before{};
  ASSERT_TRUE(
      runtime->add_group(group, {1U}, before, std::numeric_limits<std::uint64_t>::max()).is_ok());

  auto exhausted = runtime->start_election(group);

  ASSERT_FALSE(exhausted.has_value());
  EXPECT_EQ(exhausted.error().code(), common::StatusCode::kOutOfRange);
  EXPECT_TRUE(runtime->failed());
  const RaftNode* node = runtime->find_group(group);
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state(), before);
  EXPECT_EQ(node->current_term(), 0U);
  EXPECT_EQ(node->role(), Role::kFollower);
  EXPECT_EQ(runtime->start_election(group).error().code(), common::StatusCode::kUnavailable);
}

TEST(MultiRaftTest, RoutesReadBarrierProbeAndGroupScopedCompletion) {
  const GroupId group = group_id(std::byte{8});
  auto runtime = MultiRaftRuntime::create(1U);
  ASSERT_TRUE(runtime.has_value());
  ASSERT_TRUE(runtime->add_group(group, {1U, 2U, 3U}).is_ok());
  ASSERT_TRUE(runtime->start_election(group).has_value());
  ASSERT_TRUE(runtime->receive(group, 2U, RequestVoteResponse{1U, true}).has_value());
  ASSERT_TRUE(runtime->propose(group, 1U, {std::byte{0x44}}).has_value());
  ASSERT_TRUE(runtime->receive(group, 2U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U})
                  .has_value());

  auto started = runtime->begin_read_barrier(group);
  ASSERT_TRUE(started.has_value()) << started.error().to_string();
  ASSERT_EQ(started->outbound.size(), 2U);
  EXPECT_FALSE(started->persistence.has_value());
  EXPECT_FALSE(started->read_barrier_ready.has_value());
  for (const GroupOutboundMessage& outbound : started->outbound)
    EXPECT_EQ(outbound.group_id, group);
  const auto context =
      std::get<ReadBarrierRequest>(started->outbound.front().outbound.message).context;

  auto completed = runtime->receive(group, 2U, ReadBarrierResponse{1U, context, true});
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  const auto& ready = completed->read_barrier_ready;
  if (!ready.has_value()) {
    ADD_FAILURE() << "expected group-scoped read-barrier completion";
    return;
  }
  EXPECT_EQ(*ready,
            (GroupReadBarrier{.group_id = group,
                              .barrier = {.term = 1U, .context = context, .read_index = 1U}}));
}

} // namespace
} // namespace chronos::raft
