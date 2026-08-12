#include "chronos/raft/deterministic_simulator.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] RaftSimulationConfig config() {
  return {.node_ids = {1U, 2U, 3U},
          .initial_voters = {1U, 2U, 3U},
          .limits = {.maximum_pending_messages = 4096U,
                     .maximum_trace_actions = 20'000U,
                     .maximum_shrink_replays = 1000U}};
}

void drain(DeterministicRaftSimulator& simulation) {
  for (std::size_t delivered = 0U; delivered < 10'000U; ++delivered) {
    auto messages = simulation.pending_messages();
    ASSERT_TRUE(messages.has_value()) << messages.error().to_string();
    if (messages->empty())
      return;
    ASSERT_TRUE(simulation.step(RaftSimulationDeliver{messages->front().message_id}).is_ok())
        << simulation.status().to_string();
  }
  FAIL() << "simulation network did not drain";
}

TEST(DeterministicRaftSimulatorTest, ReplaysPartitionDuplicateCommitCrashAndPersistenceFailure) {
  auto simulation = DeterministicRaftSimulator::create(config());
  ASSERT_TRUE(simulation.has_value()) << simulation.error().to_string();
  ASSERT_TRUE(simulation->step(RaftSimulationStartElection{1U}).is_ok());
  auto election = simulation->pending_messages();
  ASSERT_TRUE(election.has_value());
  ASSERT_EQ(election->size(), 2U);
  ASSERT_TRUE(simulation->step(RaftSimulationDuplicate{election->front().message_id}).is_ok());
  ASSERT_TRUE(simulation->step(RaftSimulationSetLink{1U, 3U, false}).is_ok());
  drain(*simulation);
  ASSERT_NE(simulation->active_node(1U), nullptr);
  ASSERT_EQ(simulation->active_node(1U)->role(), Role::kLeader);

  ASSERT_TRUE(simulation->step(RaftSimulationPropose{1U, 1U, {std::byte{0x11U}, std::byte{0x22U}}})
                  .is_ok());
  drain(*simulation);
  EXPECT_EQ(simulation->durable_state(1U)->commit_index, 1U);
  EXPECT_EQ(simulation->durable_state(2U)->commit_index, 1U);
  EXPECT_TRUE(simulation->durable_state(3U)->log.empty());
  ASSERT_TRUE(simulation->step(RaftSimulationHeartbeat{1U}).is_ok());
  drain(*simulation);
  EXPECT_EQ(simulation->durable_state(2U)->commit_index, 1U);

  ASSERT_TRUE(simulation->step(RaftSimulationCrash{1U}).is_ok());
  ASSERT_TRUE(simulation->step(RaftSimulationFailNextPersistence{2U}).is_ok());
  ASSERT_TRUE(simulation->step(RaftSimulationStartElection{2U}).is_ok());
  EXPECT_EQ(simulation->active_node(2U), nullptr);
  EXPECT_EQ(simulation->durable_state(2U)->current_term, 1U);
  ASSERT_TRUE(simulation->step(RaftSimulationRestart{2U}).is_ok());
  EXPECT_EQ(simulation->active_node(2U)->current_term(), 1U);

  std::vector<RaftSimulationAction> trace(simulation->trace().begin(), simulation->trace().end());
  auto replay = DeterministicRaftSimulator::create(config());
  ASSERT_TRUE(replay.has_value());
  ASSERT_TRUE(replay->replay(trace).is_ok()) << replay->status().to_string();
  EXPECT_EQ(*replay->durable_state(1U), *simulation->durable_state(1U));
  EXPECT_EQ(*replay->durable_state(2U), *simulation->durable_state(2U));
  EXPECT_EQ(replay->stats().delivered, simulation->stats().delivered);
  EXPECT_EQ(replay->stats().persistence_failures, 1U);
}

TEST(DeterministicRaftSimulatorTest, RunsReproducibleSeededFaultSchedules) {
  for (std::uint64_t seed = 1U; seed <= 8U; ++seed) {
    auto first = DeterministicRaftSimulator::create(config());
    auto second = DeterministicRaftSimulator::create(config());
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(first->run_seeded(seed, 500U).is_ok())
        << "seed=" << seed << " " << first->status().to_string();
    ASSERT_TRUE(second->run_seeded(seed, 500U).is_ok())
        << "seed=" << seed << " " << second->status().to_string();
    EXPECT_TRUE(std::ranges::equal(first->trace(), second->trace()));
    EXPECT_EQ(first->stats().actions, 500U);
    EXPECT_EQ(first->stats(), second->stats());
    for (NodeId node = 1U; node <= 3U; ++node)
      EXPECT_EQ(*first->durable_state(node), *second->durable_state(node));
  }
}

TEST(DeterministicRaftSimulatorTest, DrivesJointMembershipAndLocalSnapshotCompaction) {
  auto membership_config = config();
  membership_config.node_ids.push_back(4U);
  auto simulation = DeterministicRaftSimulator::create(std::move(membership_config));
  ASSERT_TRUE(simulation.has_value());
  ASSERT_TRUE(simulation->step(RaftSimulationStartElection{1U}).is_ok());
  drain(*simulation);
  ASSERT_EQ(simulation->active_node(1U)->role(), Role::kLeader);

  ASSERT_TRUE(simulation->step(RaftSimulationBeginMembershipChange{1U, {1U, 2U, 3U, 4U}}).is_ok());
  drain(*simulation);
  ASSERT_TRUE(simulation->active_node(1U)->joint_membership_can_finalize());
  ASSERT_TRUE(simulation->step(RaftSimulationFinalizeMembershipChange{1U}).is_ok());
  drain(*simulation);
  EXPECT_EQ(simulation->active_node(1U)->commit_index(), 2U);
  EXPECT_EQ(simulation->active_node(4U)->commit_index(), 2U);
  EXPECT_TRUE(std::ranges::equal(simulation->active_node(1U)->voters(),
                                 std::vector<NodeId>{1U, 2U, 3U, 4U}));

  ASSERT_TRUE(simulation->step(RaftSimulationMarkApplied{1U, 2U}).is_ok());
  SnapshotMetadata snapshot{
      .last_included_index = 2U, .last_included_term = 1U, .manifest_generation = 7U};
  snapshot.part_set_checksum.fill(std::byte{0x5aU});
  ASSERT_TRUE(simulation->step(RaftSimulationCompactSnapshot{1U, snapshot}).is_ok())
      << simulation->status().to_string();
  EXPECT_EQ(simulation->durable_state(1U)->snapshot.last_included_index, 2U);
  EXPECT_TRUE(simulation->durable_state(1U)->log.empty());
}

TEST(DeterministicRaftSimulatorTest, ShrinksAReplayFailureToItsEssentialAction) {
  const std::vector<RaftSimulationAction> failing{RaftSimulationSetLink{1U, 2U, false},
                                                  RaftSimulationSetLink{2U, 1U, false},
                                                  RaftSimulationDeliver{999U}};
  auto shrunk = DeterministicRaftSimulator::shrink_failing_trace(config(), failing);
  ASSERT_TRUE(shrunk.has_value()) << shrunk.error().to_string();
  ASSERT_EQ(shrunk->size(), 1U);
  EXPECT_EQ(std::get<RaftSimulationDeliver>(shrunk->front()), RaftSimulationDeliver{999U});
}

TEST(DeterministicRaftSimulatorTest, RejectsUnboundedAndUnknownScheduleInputs) {
  auto invalid = config();
  invalid.node_ids = {2U, 1U};
  EXPECT_EQ(DeterministicRaftSimulator::create(std::move(invalid)).error().code(),
            common::StatusCode::kInvalidArgument);
  auto simulation = DeterministicRaftSimulator::create(config());
  ASSERT_TRUE(simulation.has_value());
  EXPECT_EQ(simulation->step(RaftSimulationCrash{9U}).code(), common::StatusCode::kNotFound);
  EXPECT_EQ(simulation->step(RaftSimulationStartElection{1U}).code(),
            common::StatusCode::kNotFound);
}

} // namespace
} // namespace chronos::raft
