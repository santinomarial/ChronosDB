#include "chronos/raft/deterministic_simulator.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] RaftSimulationConfig config() {
  return {.node_ids = {1U, 2U, 3U},
          .initial_voters = {1U, 2U, 3U},
          .limits = {.maximum_pending_messages = 4096U,
                     .maximum_trace_actions = 20'000U,
                     .maximum_shrink_replays = 1000U,
                     .raft = {}}};
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

[[nodiscard]] std::uint64_t message_id(DeterministicRaftSimulator& simulation, const NodeId source,
                                       const NodeId destination,
                                       const std::uint64_t excluded = 0U) {
  auto messages = simulation.pending_messages();
  EXPECT_TRUE(messages.has_value());
  if (!messages.has_value())
    return 0U;
  const auto found = std::ranges::find_if(*messages, [&](const auto& route) {
    return route.source == source && route.destination == destination &&
           route.message_id != excluded;
  });
  EXPECT_NE(found, messages->end());
  return found == messages->end() ? 0U : found->message_id;
}

void drain_except(DeterministicRaftSimulator& simulation, const std::uint64_t retained) {
  for (std::size_t delivered = 0U; delivered < 10'000U; ++delivered) {
    auto messages = simulation.pending_messages();
    ASSERT_TRUE(messages.has_value()) << messages.error().to_string();
    const auto next = std::ranges::find_if(
        *messages, [&](const auto& route) { return route.message_id != retained; });
    if (next == messages->end())
      return;
    ASSERT_TRUE(simulation.step(RaftSimulationDeliver{next->message_id}).is_ok())
        << simulation.status().to_string();
  }
  FAIL() << "simulation network did not drain around retained message";
}

TEST(DeterministicRaftSimulatorTest, InitializesAndRestartsFromAnExactEmptyDurableImage) {
  auto simulation = DeterministicRaftSimulator::create(config());
  ASSERT_TRUE(simulation.has_value()) << simulation.error().to_string();
  const PersistentState empty;
  for (const NodeId node : config().node_ids) {
    ASSERT_NE(simulation->durable_state(node), nullptr);
    EXPECT_EQ(*simulation->durable_state(node), empty);
  }

  ASSERT_TRUE(simulation->step(RaftSimulationCrash{2U}).is_ok());
  EXPECT_EQ(simulation->active_node(2U), nullptr);
  EXPECT_EQ(*simulation->durable_state(2U), empty);
  ASSERT_TRUE(simulation->step(RaftSimulationRestart{2U}).is_ok());
  ASSERT_NE(simulation->active_node(2U), nullptr);
  EXPECT_EQ(*simulation->durable_state(2U), empty);
}

TEST(DeterministicRaftSimulatorTest, InitializesAndRestartsFromRecoveredTermExhaustion) {
  PersistentState recovered;
  recovered.current_term = std::numeric_limits<Term>::max();
  recovered.voted_for = 1U;
  auto recovered_config = config();
  recovered_config.node_ids = {1U};
  recovered_config.initial_voters = {1U};
  recovered_config.initial_persistent_states = {recovered};

  auto simulation = DeterministicRaftSimulator::create(recovered_config);

  ASSERT_TRUE(simulation.has_value()) << simulation.error().to_string();
  ASSERT_NE(simulation->durable_state(1U), nullptr);
  EXPECT_EQ(*simulation->durable_state(1U), recovered);
  ASSERT_NE(simulation->active_node(1U), nullptr);
  EXPECT_EQ(simulation->active_node(1U)->persistent_state(), recovered);
  ASSERT_TRUE(simulation->step(RaftSimulationCrash{1U}).is_ok());
  ASSERT_TRUE(simulation->step(RaftSimulationRestart{1U}).is_ok());
  ASSERT_NE(simulation->active_node(1U), nullptr);
  EXPECT_EQ(simulation->active_node(1U)->persistent_state(), recovered);

  const common::Status exhausted = simulation->step(RaftSimulationStartElection{1U});

  EXPECT_EQ(exhausted.code(), common::StatusCode::kOutOfRange);
  ASSERT_NE(simulation->active_node(1U), nullptr);
  EXPECT_EQ(simulation->active_node(1U)->role(), Role::kFollower);
  EXPECT_EQ(simulation->active_node(1U)->persistent_state(), recovered);
  EXPECT_EQ(*simulation->durable_state(1U), recovered);

  recovered_config.initial_persistent_states.push_back(recovered);
  auto mismatched = DeterministicRaftSimulator::create(recovered_config);
  ASSERT_FALSE(mismatched.has_value());
  EXPECT_EQ(mismatched.error().code(), common::StatusCode::kInvalidArgument);
  recovered_config.initial_persistent_states.resize(1U);
  recovered_config.initial_persistent_states.front().current_term = 0U;
  auto invalid = DeterministicRaftSimulator::create(std::move(recovered_config));
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(DeterministicRaftSimulatorTest, PreservesRecoveredStateAtTermAndIndexExhaustion) {
  PersistentState recovered;
  recovered.current_term = std::numeric_limits<Term>::max() - 1U;
  recovered.snapshot.last_included_index = std::numeric_limits<LogIndex>::max() - 1U;
  recovered.snapshot.last_included_term = recovered.current_term;
  recovered.snapshot.manifest_generation = 1U;
  recovered.snapshot.voters = {1U};
  recovered.commit_index = recovered.snapshot.last_included_index;
  recovered.applied_index = recovered.snapshot.last_included_index;
  auto recovered_config = config();
  recovered_config.node_ids = {1U};
  recovered_config.initial_voters = {1U};
  recovered_config.initial_persistent_states = {recovered};

  auto proposal = DeterministicRaftSimulator::create(recovered_config);
  ASSERT_TRUE(proposal.has_value()) << proposal.error().to_string();
  ASSERT_TRUE(proposal->step(RaftSimulationStartElection{1U}).is_ok());
  ASSERT_NE(proposal->active_node(1U), nullptr);
  ASSERT_EQ(proposal->active_node(1U)->role(), Role::kLeader);
  ASSERT_NE(proposal->durable_state(1U), nullptr);
  const PersistentState terminal = *proposal->durable_state(1U);
  ASSERT_EQ(terminal.current_term, std::numeric_limits<Term>::max());

  const common::Status proposal_exhausted =
      proposal->step(RaftSimulationPropose{1U, 1U, {std::byte{0x11U}}});

  EXPECT_EQ(proposal_exhausted.code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(proposal->active_node(1U)->persistent_state(), terminal);
  EXPECT_EQ(*proposal->durable_state(1U), terminal);

  auto election = DeterministicRaftSimulator::create(std::move(recovered_config));
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->step(RaftSimulationStartElection{1U}).is_ok());
  ASSERT_TRUE(election->step(RaftSimulationCrash{1U}).is_ok());
  ASSERT_TRUE(election->step(RaftSimulationRestart{1U}).is_ok());
  ASSERT_NE(election->active_node(1U), nullptr);
  ASSERT_EQ(election->active_node(1U)->role(), Role::kFollower);
  ASSERT_NE(election->durable_state(1U), nullptr);
  const PersistentState restarted = *election->durable_state(1U);

  const common::Status election_exhausted = election->step(RaftSimulationStartElection{1U});

  EXPECT_EQ(election_exhausted.code(), common::StatusCode::kOutOfRange);
  EXPECT_EQ(election->active_node(1U)->role(), Role::kFollower);
  EXPECT_EQ(election->active_node(1U)->persistent_state(), restarted);
  EXPECT_EQ(*election->durable_state(1U), restarted);
}

TEST(DeterministicRaftSimulatorTest, RejectsUnsafeRecoveredClusterImages) {
  PersistentState first;
  first.current_term = 1U;
  first.log.push_back(LogEntry{1U, 1U, 1U, {std::byte{0x11U}}});
  PersistentState second = first;
  second.log.front().payload.front() = std::byte{0x22U};
  auto recovered_config = config();
  recovered_config.node_ids = {1U, 2U};
  recovered_config.initial_voters = {1U, 2U};
  recovered_config.initial_persistent_states = {std::move(first), std::move(second)};

  auto simulation = DeterministicRaftSimulator::create(std::move(recovered_config));

  ASSERT_FALSE(simulation.has_value());
  EXPECT_EQ(simulation.error().code(), common::StatusCode::kCorruption);

  PersistentState foreign_voter;
  foreign_voter.current_term = 1U;
  foreign_voter.snapshot.last_included_index = 1U;
  foreign_voter.snapshot.last_included_term = 1U;
  foreign_voter.snapshot.manifest_generation = 1U;
  foreign_voter.snapshot.voters = {1U, 3U};
  foreign_voter.commit_index = 1U;
  foreign_voter.applied_index = 1U;
  auto foreign_config = config();
  foreign_config.node_ids = {1U, 2U};
  foreign_config.initial_voters = {1U, 2U};
  foreign_config.initial_persistent_states = {std::move(foreign_voter), PersistentState{}};

  auto foreign = DeterministicRaftSimulator::create(std::move(foreign_config));

  ASSERT_FALSE(foreign.has_value());
  EXPECT_EQ(foreign.error().code(), common::StatusCode::kInvalidArgument);
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
    ASSERT_TRUE(first->run_seeded({.seed = seed, .actions = 500U}).is_ok())
        << "seed=" << seed << " " << first->status().to_string();
    ASSERT_TRUE(second->run_seeded({.seed = seed, .actions = 500U}).is_ok())
        << "seed=" << seed << " " << second->status().to_string();
    EXPECT_TRUE(std::ranges::equal(first->trace(), second->trace()));
    EXPECT_EQ(first->stats().actions, 500U);
    EXPECT_EQ(first->stats(), second->stats());
    for (NodeId node = 1U; node <= 3U; ++node)
      EXPECT_EQ(*first->durable_state(node), *second->durable_state(node));
  }
}

TEST(DeterministicRaftSimulatorTest, GeneratesReplayableMembershipAndSnapshotChurn) {
  auto churn_config = config();
  churn_config.node_ids.push_back(4U);
  std::size_t membership_starts = 0U;
  std::size_t membership_finalizations = 0U;
  std::size_t snapshot_compactions = 0U;
  std::size_t read_barrier_starts = 0U;
  std::uint64_t completed_read_barriers = 0U;
  for (std::uint64_t seed = 1U; seed <= 32U; ++seed) {
    auto simulation = DeterministicRaftSimulator::create(churn_config);
    ASSERT_TRUE(simulation.has_value()) << simulation.error().to_string();
    ASSERT_TRUE(simulation->run_seeded({.seed = seed, .actions = 2'000U}).is_ok())
        << "seed=" << seed << " " << simulation->status().to_string()
        << " trace=" << simulation->trace().size()
        << " action=" << simulation->trace().back().index();
    for (const RaftSimulationAction& action : simulation->trace()) {
      membership_starts +=
          std::holds_alternative<RaftSimulationBeginMembershipChange>(action) ? 1U : 0U;
      membership_finalizations +=
          std::holds_alternative<RaftSimulationFinalizeMembershipChange>(action) ? 1U : 0U;
      snapshot_compactions +=
          std::holds_alternative<RaftSimulationCompactSnapshot>(action) ? 1U : 0U;
      read_barrier_starts +=
          std::holds_alternative<RaftSimulationBeginReadBarrier>(action) ? 1U : 0U;
    }
    completed_read_barriers += simulation->stats().completed_read_barriers;

    std::vector<RaftSimulationAction> trace(simulation->trace().begin(), simulation->trace().end());
    auto replay = DeterministicRaftSimulator::create(churn_config);
    ASSERT_TRUE(replay.has_value()) << replay.error().to_string();
    ASSERT_TRUE(replay->replay(trace).is_ok())
        << "seed=" << seed << " " << replay->status().to_string();
    EXPECT_EQ(replay->stats(), simulation->stats());
    for (const NodeId node : churn_config.node_ids)
      EXPECT_EQ(*replay->durable_state(node), *simulation->durable_state(node));
  }
  EXPECT_GT(membership_starts, 0U);
  EXPECT_GT(membership_finalizations, 0U);
  EXPECT_GT(snapshot_compactions, 0U);
  EXPECT_GT(read_barrier_starts, 0U);
  EXPECT_GT(completed_read_barriers, 0U);
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
      .last_included_index = 2U, .last_included_term = 1U, .manifest_generation = 7U, .voters = {}};
  snapshot.part_set_checksum.fill(std::byte{0x5aU});
  ASSERT_TRUE(simulation->step(RaftSimulationCompactSnapshot{1U, snapshot}).is_ok())
      << simulation->status().to_string();
  EXPECT_EQ(simulation->durable_state(1U)->snapshot.last_included_index, 2U);
  EXPECT_TRUE(simulation->durable_state(1U)->log.empty());
}

TEST(DeterministicRaftSimulatorTest, GeneratesAndReplaysPendingSnapshotCompletion) {
  auto simulation = DeterministicRaftSimulator::create(config());
  ASSERT_TRUE(simulation.has_value()) << simulation.error().to_string();
  ASSERT_TRUE(simulation->step(RaftSimulationStartElection{1U}).is_ok());
  drain(*simulation);
  ASSERT_EQ(simulation->active_node(1U)->role(), Role::kLeader);

  ASSERT_TRUE(simulation->step(RaftSimulationSetLink{1U, 3U, false}).is_ok());
  ASSERT_TRUE(simulation->step(RaftSimulationPropose{1U, 1U, {std::byte{0x33U}}}).is_ok());
  drain(*simulation);
  ASSERT_EQ(simulation->active_node(1U)->commit_index(), 1U);
  ASSERT_TRUE(simulation->durable_state(3U)->log.empty());
  ASSERT_TRUE(simulation->step(RaftSimulationMarkApplied{1U, 1U}).is_ok());
  SnapshotMetadata snapshot{
      .last_included_index = 1U, .last_included_term = 1U, .manifest_generation = 1U, .voters = {}};
  snapshot.part_set_checksum.fill(std::byte{0x44U});
  ASSERT_TRUE(simulation->step(RaftSimulationCompactSnapshot{1U, snapshot}).is_ok());

  ASSERT_TRUE(simulation->step(RaftSimulationSetLink{1U, 3U, true}).is_ok());
  ASSERT_TRUE(simulation->step(RaftSimulationHeartbeat{1U}).is_ok());
  auto messages = simulation->pending_messages();
  ASSERT_TRUE(messages.has_value()) << messages.error().to_string();
  const auto snapshot_request = std::ranges::find_if(
      *messages, [](const auto& route) { return route.source == 1U && route.destination == 3U; });
  ASSERT_NE(snapshot_request, messages->end());
  ASSERT_TRUE(simulation->step(RaftSimulationDeliver{snapshot_request->message_id}).is_ok());

  const std::size_t generated = simulation->trace().size();
  ASSERT_TRUE(simulation->run_seeded({.seed = 15U, .actions = 1U}).is_ok())
      << simulation->status().to_string();
  ASSERT_EQ(simulation->trace().size(), generated + 1U);
  EXPECT_TRUE(std::holds_alternative<RaftSimulationCompleteSnapshotInstall>(
      simulation->trace()[generated]));

  std::vector<RaftSimulationAction> trace(simulation->trace().begin(), simulation->trace().end());
  auto replay = DeterministicRaftSimulator::create(config());
  ASSERT_TRUE(replay.has_value()) << replay.error().to_string();
  ASSERT_TRUE(replay->replay(trace).is_ok()) << replay->status().to_string();
  EXPECT_EQ(replay->stats(), simulation->stats());
  for (const NodeId node : config().node_ids)
    EXPECT_EQ(*replay->durable_state(node), *simulation->durable_state(node));
}

TEST(DeterministicRaftSimulatorTest, ExhaustivelyExploresBoundedMessageDeliveryAndLoss) {
  auto two_nodes = config();
  two_nodes.node_ids = {1U, 2U};
  two_nodes.initial_voters = {1U, 2U};
  const std::vector<RaftSimulationAction> setup{RaftSimulationStartElection{1U}};

  auto complete = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, setup, {.maximum_depth = 2U, .maximum_replays = 5U});

  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_TRUE(complete->search_complete);
  EXPECT_EQ(complete->replayed_prefixes, 5U);
  EXPECT_FALSE(complete->failure.has_value());
  EXPECT_TRUE(complete->failing_trace.empty());

  auto bounded = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, setup, {.maximum_depth = 2U, .maximum_replays = 4U});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  EXPECT_FALSE(bounded->search_complete);
  EXPECT_EQ(bounded->replayed_prefixes, 4U);
  EXPECT_FALSE(bounded->failure.has_value());

  const std::vector<RaftSimulationAction> invalid_trace{RaftSimulationDeliver{999U}};
  auto invalid_setup = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, invalid_trace, {.maximum_depth = 1U, .maximum_replays = 1U});
  ASSERT_FALSE(invalid_setup.has_value());
  EXPECT_EQ(invalid_setup.error().code(), common::StatusCode::kInvalidArgument);
  auto invalid_bounds = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, {}, {.maximum_depth = 1U, .maximum_replays = 0U});
  ASSERT_FALSE(invalid_bounds.has_value());
  EXPECT_EQ(invalid_bounds.error().code(), common::StatusCode::kInvalidArgument);
  auto tight_trace = two_nodes;
  tight_trace.limits.maximum_trace_actions = setup.size();
  auto invalid_depth = DeterministicRaftSimulator::explore_fault_schedules(
      tight_trace, setup, {.maximum_depth = 1U, .maximum_replays = 1U});
  ASSERT_FALSE(invalid_depth.has_value());
  EXPECT_EQ(invalid_depth.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(DeterministicRaftSimulatorTest, ExhaustivelyExploresOptionalMessageDuplication) {
  auto two_nodes = config();
  two_nodes.node_ids = {1U, 2U};
  two_nodes.initial_voters = {1U, 2U};
  const std::vector<RaftSimulationAction> setup{RaftSimulationStartElection{1U}};

  auto complete = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, setup, {.maximum_depth = 1U, .maximum_replays = 4U, .include_duplication = true});

  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_TRUE(complete->search_complete);
  EXPECT_EQ(complete->replayed_prefixes, 4U);
  EXPECT_FALSE(complete->failure.has_value());

  two_nodes.limits.maximum_pending_messages = 1U;
  auto queue_failure = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, setup, {.maximum_depth = 1U, .maximum_replays = 4U, .include_duplication = true});
  ASSERT_TRUE(queue_failure.has_value()) << queue_failure.error().to_string();
  EXPECT_FALSE(queue_failure->search_complete);
  EXPECT_EQ(queue_failure->replayed_prefixes, 4U);
  ASSERT_TRUE(queue_failure->failure.has_value());
  const common::Status failure = queue_failure->failure.value_or(
      common::Status{common::StatusCode::kInternal, "missing duplication failure"});
  EXPECT_EQ(failure.code(), common::StatusCode::kResourceExhausted);
  ASSERT_EQ(queue_failure->failing_trace.size(), setup.size() + 1U);
  const auto& duplicate = std::get<RaftSimulationDuplicate>(queue_failure->failing_trace.back());
  EXPECT_EQ(duplicate.message_id, 1U);
  auto replay = DeterministicRaftSimulator::create(two_nodes);
  ASSERT_TRUE(replay.has_value()) << replay.error().to_string();
  EXPECT_EQ(replay->replay(queue_failure->failing_trace).code(),
            common::StatusCode::kResourceExhausted);
}

TEST(DeterministicRaftSimulatorTest, ExhaustivelyExploresNodeCrashAndRestart) {
  auto one_node = config();
  one_node.node_ids = {1U};
  one_node.initial_voters = {1U};

  auto complete = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, {}, {.maximum_depth = 2U, .maximum_replays = 3U, .include_node_lifecycle = true});

  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_TRUE(complete->search_complete);
  EXPECT_EQ(complete->replayed_prefixes, 3U);
  EXPECT_FALSE(complete->failure.has_value());

  auto bounded = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, {}, {.maximum_depth = 2U, .maximum_replays = 2U, .include_node_lifecycle = true});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  EXPECT_FALSE(bounded->search_complete);
  EXPECT_EQ(bounded->replayed_prefixes, 2U);
  EXPECT_FALSE(bounded->failure.has_value());
}

TEST(DeterministicRaftSimulatorTest, ExhaustivelyExploresDirectionalLinkChanges) {
  auto two_nodes = config();
  two_nodes.node_ids = {1U, 2U};
  two_nodes.initial_voters = {1U, 2U};

  auto complete = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, {}, {.maximum_depth = 2U, .maximum_replays = 7U, .include_link_changes = true});

  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_TRUE(complete->search_complete);
  EXPECT_EQ(complete->replayed_prefixes, 7U);
  EXPECT_FALSE(complete->failure.has_value());

  auto bounded = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, {}, {.maximum_depth = 2U, .maximum_replays = 6U, .include_link_changes = true});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  EXPECT_FALSE(bounded->search_complete);
  EXPECT_EQ(bounded->replayed_prefixes, 6U);
  EXPECT_FALSE(bounded->failure.has_value());
}

TEST(DeterministicRaftSimulatorTest, ExhaustivelyArmsPersistenceFailureOncePerActiveNode) {
  auto one_node = config();
  one_node.node_ids = {1U};
  one_node.initial_voters = {1U};

  auto complete = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, {},
      {.maximum_depth = 2U, .maximum_replays = 2U, .include_persistence_failures = true});

  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_TRUE(complete->search_complete);
  EXPECT_EQ(complete->replayed_prefixes, 2U);
  EXPECT_FALSE(complete->failure.has_value());

  auto bounded = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, {},
      {.maximum_depth = 2U, .maximum_replays = 1U, .include_persistence_failures = true});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  EXPECT_FALSE(bounded->search_complete);
  EXPECT_EQ(bounded->replayed_prefixes, 1U);
  EXPECT_FALSE(bounded->failure.has_value());

  const std::vector<RaftSimulationAction> crashed{RaftSimulationCrash{1U}};
  auto inactive = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, crashed,
      {.maximum_depth = 1U, .maximum_replays = 1U, .include_persistence_failures = true});
  ASSERT_TRUE(inactive.has_value()) << inactive.error().to_string();
  EXPECT_TRUE(inactive->search_complete);
  EXPECT_EQ(inactive->replayed_prefixes, 1U);
}

TEST(DeterministicRaftSimulatorTest, ExhaustivelyExploresEligibleElectionsAndRetainsExhaustion) {
  auto learner = config();
  learner.node_ids = {1U, 2U};
  learner.initial_voters = {1U};

  auto complete = DeterministicRaftSimulator::explore_fault_schedules(
      learner, {}, {.maximum_depth = 2U, .maximum_replays = 2U, .include_elections = true});

  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_TRUE(complete->search_complete);
  EXPECT_EQ(complete->replayed_prefixes, 2U);
  EXPECT_FALSE(complete->failure.has_value());

  auto bounded = DeterministicRaftSimulator::explore_fault_schedules(
      learner, {}, {.maximum_depth = 2U, .maximum_replays = 1U, .include_elections = true});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  EXPECT_FALSE(bounded->search_complete);
  EXPECT_EQ(bounded->replayed_prefixes, 1U);

  PersistentState terminal;
  terminal.current_term = std::numeric_limits<Term>::max();
  terminal.voted_for = 1U;
  auto exhausted_config = config();
  exhausted_config.node_ids = {1U};
  exhausted_config.initial_voters = {1U};
  exhausted_config.initial_persistent_states = {terminal};
  auto exhausted = DeterministicRaftSimulator::explore_fault_schedules(
      exhausted_config, {},
      {.maximum_depth = 1U, .maximum_replays = 2U, .include_elections = true});
  ASSERT_TRUE(exhausted.has_value()) << exhausted.error().to_string();
  EXPECT_FALSE(exhausted->search_complete);
  EXPECT_EQ(exhausted->replayed_prefixes, 2U);
  ASSERT_TRUE(exhausted->failure.has_value());
  const common::Status failure = exhausted->failure.value_or(
      common::Status{common::StatusCode::kInternal, "missing election exhaustion"});
  EXPECT_EQ(failure.code(), common::StatusCode::kOutOfRange);
  ASSERT_EQ(exhausted->failing_trace.size(), 1U);
  EXPECT_EQ(std::get<RaftSimulationStartElection>(exhausted->failing_trace.front()),
            RaftSimulationStartElection{1U});
  auto replay = DeterministicRaftSimulator::create(exhausted_config);
  ASSERT_TRUE(replay.has_value()) << replay.error().to_string();
  EXPECT_EQ(replay->replay(exhausted->failing_trace).code(), failure.code());
  ASSERT_NE(replay->active_node(1U), nullptr);
  EXPECT_EQ(replay->active_node(1U)->role(), Role::kFollower);
  EXPECT_EQ(replay->active_node(1U)->persistent_state(), terminal);
  ASSERT_NE(replay->durable_state(1U), nullptr);
  EXPECT_EQ(*replay->durable_state(1U), terminal);
}

TEST(DeterministicRaftSimulatorTest, ExhaustivelyExploresMultiMemberLeaderHeartbeats) {
  auto two_nodes = config();
  two_nodes.node_ids = {1U, 2U};
  two_nodes.initial_voters = {1U, 2U};
  auto leader = DeterministicRaftSimulator::create(two_nodes);
  ASSERT_TRUE(leader.has_value()) << leader.error().to_string();
  ASSERT_TRUE(leader->step(RaftSimulationStartElection{1U}).is_ok());
  drain(*leader);
  ASSERT_NE(leader->active_node(1U), nullptr);
  ASSERT_EQ(leader->active_node(1U)->role(), Role::kLeader);
  const std::vector<RaftSimulationAction> setup(leader->trace().begin(), leader->trace().end());

  auto complete = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, setup, {.maximum_depth = 1U, .maximum_replays = 2U, .include_heartbeats = true});

  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_TRUE(complete->search_complete);
  EXPECT_EQ(complete->replayed_prefixes, 2U);
  EXPECT_FALSE(complete->failure.has_value());

  auto bounded = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, setup, {.maximum_depth = 1U, .maximum_replays = 1U, .include_heartbeats = true});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  EXPECT_FALSE(bounded->search_complete);
  EXPECT_EQ(bounded->replayed_prefixes, 1U);

  auto one_node = config();
  one_node.node_ids = {1U};
  one_node.initial_voters = {1U};
  const std::vector<RaftSimulationAction> single_setup{RaftSimulationStartElection{1U}};
  auto no_op_excluded = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, single_setup,
      {.maximum_depth = 1U, .maximum_replays = 1U, .include_heartbeats = true});
  ASSERT_TRUE(no_op_excluded.has_value()) << no_op_excluded.error().to_string();
  EXPECT_TRUE(no_op_excluded->search_complete);
  EXPECT_EQ(no_op_excluded->replayed_prefixes, 1U);
}

TEST(DeterministicRaftSimulatorTest, ExhaustivelyExploresApplicationBoundaries) {
  auto one_node = config();
  one_node.node_ids = {1U};
  one_node.initial_voters = {1U};
  const std::vector<RaftSimulationAction> setup{RaftSimulationStartElection{1U},
                                                RaftSimulationPropose{1U, 1U, {std::byte{0x11U}}},
                                                RaftSimulationPropose{1U, 1U, {std::byte{0x22U}}}};

  auto complete = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, setup,
      {.maximum_depth = 2U, .maximum_replays = 4U, .include_application_advancement = true});

  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_TRUE(complete->search_complete);
  EXPECT_EQ(complete->replayed_prefixes, 4U);
  EXPECT_FALSE(complete->failure.has_value());

  auto bounded = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, setup,
      {.maximum_depth = 2U, .maximum_replays = 3U, .include_application_advancement = true});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  EXPECT_FALSE(bounded->search_complete);
  EXPECT_EQ(bounded->replayed_prefixes, 3U);

  std::vector<RaftSimulationAction> crashed = setup;
  crashed.emplace_back(RaftSimulationCrash{1U});
  auto inactive = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, crashed,
      {.maximum_depth = 1U, .maximum_replays = 1U, .include_application_advancement = true});
  ASSERT_TRUE(inactive.has_value()) << inactive.error().to_string();
  EXPECT_TRUE(inactive->search_complete);
  EXPECT_EQ(inactive->replayed_prefixes, 1U);
}

TEST(DeterministicRaftSimulatorTest, ExhaustivelyExploresReadBarrierCompletionAndLoss) {
  auto two_nodes = config();
  two_nodes.node_ids = {1U, 2U};
  two_nodes.initial_voters = {1U, 2U};
  auto leader = DeterministicRaftSimulator::create(two_nodes);
  ASSERT_TRUE(leader.has_value()) << leader.error().to_string();
  ASSERT_TRUE(leader->step(RaftSimulationStartElection{1U}).is_ok());
  drain(*leader);
  ASSERT_TRUE(leader->step(RaftSimulationPropose{1U, 1U, {std::byte{0x11U}}}).is_ok());
  drain(*leader);
  ASSERT_NE(leader->active_node(1U), nullptr);
  ASSERT_EQ(leader->active_node(1U)->role(), Role::kLeader);
  ASSERT_EQ(leader->active_node(1U)->commit_index(), 1U);
  const std::vector<RaftSimulationAction> setup(leader->trace().begin(), leader->trace().end());

  auto complete = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, setup,
      {.maximum_depth = 3U, .maximum_replays = 6U, .include_read_barriers = true});

  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_TRUE(complete->search_complete);
  EXPECT_EQ(complete->replayed_prefixes, 6U);
  EXPECT_FALSE(complete->failure.has_value());

  auto bounded = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, setup,
      {.maximum_depth = 3U, .maximum_replays = 5U, .include_read_barriers = true});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  EXPECT_FALSE(bounded->search_complete);
  EXPECT_EQ(bounded->replayed_prefixes, 5U);

  auto completed = DeterministicRaftSimulator::create(two_nodes);
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_TRUE(completed->replay(setup).is_ok());
  ASSERT_TRUE(completed->step(RaftSimulationBeginReadBarrier{1U}).is_ok());
  auto request = completed->pending_messages();
  ASSERT_TRUE(request.has_value()) << request.error().to_string();
  ASSERT_EQ(request->size(), 1U);
  ASSERT_TRUE(completed->step(RaftSimulationDeliver{request->front().message_id}).is_ok());
  auto response = completed->pending_messages();
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  ASSERT_EQ(response->size(), 1U);
  ASSERT_TRUE(completed->step(RaftSimulationDeliver{response->front().message_id}).is_ok());
  EXPECT_EQ(completed->stats().completed_read_barriers, 1U);
  ASSERT_NE(completed->active_node(1U), nullptr);
  EXPECT_FALSE(completed->active_node(1U)->read_barrier_pending());

  auto lost = DeterministicRaftSimulator::create(two_nodes);
  ASSERT_TRUE(lost.has_value()) << lost.error().to_string();
  ASSERT_TRUE(lost->replay(setup).is_ok());
  ASSERT_TRUE(lost->step(RaftSimulationBeginReadBarrier{1U}).is_ok());
  auto dropped = lost->pending_messages();
  ASSERT_TRUE(dropped.has_value()) << dropped.error().to_string();
  ASSERT_EQ(dropped->size(), 1U);
  ASSERT_TRUE(lost->step(RaftSimulationDrop{dropped->front().message_id}).is_ok());
  EXPECT_EQ(lost->stats().completed_read_barriers, 0U);
  ASSERT_NE(lost->active_node(1U), nullptr);
  EXPECT_TRUE(lost->active_node(1U)->read_barrier_pending());

  auto one_node = config();
  one_node.node_ids = {1U};
  one_node.initial_voters = {1U};
  const std::vector<RaftSimulationAction> no_commit{RaftSimulationStartElection{1U}};
  auto ineligible = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, no_commit,
      {.maximum_depth = 1U, .maximum_replays = 1U, .include_read_barriers = true});
  ASSERT_TRUE(ineligible.has_value()) << ineligible.error().to_string();
  EXPECT_TRUE(ineligible->search_complete);
  EXPECT_EQ(ineligible->replayed_prefixes, 1U);
}

TEST(DeterministicRaftSimulatorTest, ExhaustivelyExploresMembershipBeginAndFinalize) {
  auto two_nodes = config();
  two_nodes.node_ids = {1U, 2U};
  two_nodes.initial_voters = {1U};
  const std::vector<RaftSimulationAction> stable_setup{RaftSimulationStartElection{1U}};

  auto stable = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, stable_setup,
      {.maximum_depth = 1U, .maximum_replays = 2U, .include_membership_changes = true});

  ASSERT_TRUE(stable.has_value()) << stable.error().to_string();
  EXPECT_TRUE(stable->search_complete);
  EXPECT_EQ(stable->replayed_prefixes, 2U);
  EXPECT_FALSE(stable->failure.has_value());

  auto bounded = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, stable_setup,
      {.maximum_depth = 1U, .maximum_replays = 1U, .include_membership_changes = true});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  EXPECT_FALSE(bounded->search_complete);
  EXPECT_EQ(bounded->replayed_prefixes, 1U);

  auto joint_leader = DeterministicRaftSimulator::create(two_nodes);
  ASSERT_TRUE(joint_leader.has_value()) << joint_leader.error().to_string();
  ASSERT_TRUE(joint_leader->step(RaftSimulationStartElection{1U}).is_ok());
  ASSERT_TRUE(joint_leader->step(RaftSimulationBeginMembershipChange{1U, {1U, 2U}}).is_ok());
  drain(*joint_leader);
  ASSERT_NE(joint_leader->active_node(1U), nullptr);
  ASSERT_TRUE(joint_leader->active_node(1U)->joint_membership_can_finalize());
  const std::vector<RaftSimulationAction> joint_setup(joint_leader->trace().begin(),
                                                      joint_leader->trace().end());
  auto finalization = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, joint_setup,
      {.maximum_depth = 1U, .maximum_replays = 2U, .include_membership_changes = true});
  ASSERT_TRUE(finalization.has_value()) << finalization.error().to_string();
  EXPECT_TRUE(finalization->search_complete);
  EXPECT_EQ(finalization->replayed_prefixes, 2U);
  EXPECT_FALSE(finalization->failure.has_value());

  auto follower = DeterministicRaftSimulator::explore_fault_schedules(
      two_nodes, {},
      {.maximum_depth = 1U, .maximum_replays = 1U, .include_membership_changes = true});
  ASSERT_TRUE(follower.has_value()) << follower.error().to_string();
  EXPECT_TRUE(follower->search_complete);
  EXPECT_EQ(follower->replayed_prefixes, 1U);
}

TEST(DeterministicRaftSimulatorTest, ExhaustivelyExploresLocalSnapshotCompaction) {
  auto one_node = config();
  one_node.node_ids = {1U};
  one_node.initial_voters = {1U};
  const std::vector<RaftSimulationAction> setup{
      RaftSimulationStartElection{1U}, RaftSimulationPropose{1U, 1U, {std::byte{0x11U}}},
      RaftSimulationPropose{1U, 1U, {std::byte{0x22U}}}, RaftSimulationMarkApplied{1U, 2U}};

  auto complete = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, setup,
      {.maximum_depth = 2U, .maximum_replays = 2U, .include_snapshot_actions = true});

  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_TRUE(complete->search_complete);
  EXPECT_EQ(complete->replayed_prefixes, 2U);
  EXPECT_FALSE(complete->failure.has_value());

  auto bounded = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, setup,
      {.maximum_depth = 2U, .maximum_replays = 1U, .include_snapshot_actions = true});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  EXPECT_FALSE(bounded->search_complete);
  EXPECT_EQ(bounded->replayed_prefixes, 1U);

  SnapshotMetadata snapshot{
      .last_included_index = 2U, .last_included_term = 1U, .manifest_generation = 2U, .voters = {}};
  snapshot.part_set_checksum.fill(std::byte{0x03U});
  std::vector<RaftSimulationAction> compacted = setup;
  compacted.emplace_back(RaftSimulationCompactSnapshot{1U, snapshot});
  auto replay = DeterministicRaftSimulator::create(one_node);
  ASSERT_TRUE(replay.has_value()) << replay.error().to_string();
  ASSERT_TRUE(replay->replay(compacted).is_ok()) << replay->status().to_string();
  EXPECT_EQ(replay->durable_state(1U)->snapshot.last_included_index, 2U);
  EXPECT_TRUE(replay->durable_state(1U)->log.empty());

  auto ineligible = DeterministicRaftSimulator::explore_fault_schedules(
      one_node, {}, {.maximum_depth = 1U, .maximum_replays = 1U, .include_snapshot_actions = true});
  ASSERT_TRUE(ineligible.has_value()) << ineligible.error().to_string();
  EXPECT_TRUE(ineligible->search_complete);
  EXPECT_EQ(ineligible->replayed_prefixes, 1U);
}

TEST(DeterministicRaftSimulatorTest, ExhaustivelyExploresPendingSnapshotCompletion) {
  auto source = DeterministicRaftSimulator::create(config());
  ASSERT_TRUE(source.has_value()) << source.error().to_string();
  ASSERT_TRUE(source->step(RaftSimulationStartElection{1U}).is_ok());
  drain(*source);
  ASSERT_TRUE(source->step(RaftSimulationSetLink{1U, 3U, false}).is_ok());
  ASSERT_TRUE(source->step(RaftSimulationPropose{1U, 1U, {std::byte{0x33U}}}).is_ok());
  drain(*source);
  ASSERT_TRUE(source->step(RaftSimulationMarkApplied{1U, 1U}).is_ok());
  SnapshotMetadata snapshot{
      .last_included_index = 1U, .last_included_term = 1U, .manifest_generation = 1U, .voters = {}};
  snapshot.part_set_checksum.fill(std::byte{0x44U});
  ASSERT_TRUE(source->step(RaftSimulationCompactSnapshot{1U, snapshot}).is_ok());
  ASSERT_TRUE(source->step(RaftSimulationSetLink{1U, 3U, true}).is_ok());
  ASSERT_TRUE(source->step(RaftSimulationHeartbeat{1U}).is_ok());
  const std::uint64_t request = message_id(*source, 1U, 3U);
  ASSERT_NE(request, 0U);
  ASSERT_TRUE(source->step(RaftSimulationDeliver{request}).is_ok());
  drain(*source);
  const std::vector<RaftSimulationAction> setup(source->trace().begin(), source->trace().end());

  auto complete = DeterministicRaftSimulator::explore_fault_schedules(
      config(), setup,
      {.maximum_depth = 1U, .maximum_replays = 3U, .include_snapshot_actions = true});

  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_TRUE(complete->search_complete);
  EXPECT_EQ(complete->replayed_prefixes, 3U);
  EXPECT_FALSE(complete->failure.has_value());

  auto bounded = DeterministicRaftSimulator::explore_fault_schedules(
      config(), setup,
      {.maximum_depth = 1U, .maximum_replays = 2U, .include_snapshot_actions = true});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  EXPECT_FALSE(bounded->search_complete);
  EXPECT_EQ(bounded->replayed_prefixes, 2U);

  auto rejected = DeterministicRaftSimulator::create(config());
  ASSERT_TRUE(rejected.has_value()) << rejected.error().to_string();
  ASSERT_TRUE(rejected->replay(setup).is_ok()) << rejected->status().to_string();
  ASSERT_TRUE(rejected->step(RaftSimulationCompleteSnapshotInstall{3U, false}).is_ok());
  EXPECT_EQ(rejected->durable_state(3U)->snapshot.last_included_index, 0U);

  auto installed = DeterministicRaftSimulator::create(config());
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  ASSERT_TRUE(installed->replay(setup).is_ok()) << installed->status().to_string();
  ASSERT_TRUE(installed->step(RaftSimulationCompleteSnapshotInstall{3U, true}).is_ok());
  EXPECT_EQ(installed->durable_state(3U)->snapshot.last_included_index, 1U);
}

TEST(DeterministicRaftSimulatorTest, ExhaustiveExplorationRetainsTheFirstFailingSchedule) {
  auto simulation = DeterministicRaftSimulator::create(config());
  ASSERT_TRUE(simulation.has_value()) << simulation.error().to_string();
  ASSERT_TRUE(simulation->step(RaftSimulationStartElection{1U}).is_ok());
  drain(*simulation);
  ASSERT_EQ(simulation->active_node(1U)->role(), Role::kLeader);

  ASSERT_TRUE(simulation->step(RaftSimulationBeginMembershipChange{1U, {1U, 2U}}).is_ok());
  const std::uint64_t request_three = message_id(*simulation, 1U, 3U);
  ASSERT_NE(request_three, 0U);
  ASSERT_TRUE(simulation->step(RaftSimulationDeliver{request_three}).is_ok());
  const std::uint64_t response_three = message_id(*simulation, 3U, 1U);
  ASSERT_NE(response_three, 0U);
  ASSERT_TRUE(simulation->step(RaftSimulationDuplicate{response_three}).is_ok());
  const std::uint64_t retained_response = message_id(*simulation, 3U, 1U, response_three);
  ASSERT_NE(retained_response, 0U);
  ASSERT_TRUE(simulation->step(RaftSimulationDeliver{response_three}).is_ok());
  const std::uint64_t request_two = message_id(*simulation, 1U, 2U);
  ASSERT_NE(request_two, 0U);
  ASSERT_TRUE(simulation->step(RaftSimulationDeliver{request_two}).is_ok());
  const std::uint64_t response_two = message_id(*simulation, 2U, 1U);
  ASSERT_NE(response_two, 0U);
  ASSERT_TRUE(simulation->step(RaftSimulationDeliver{response_two}).is_ok());
  drain_except(*simulation, retained_response);
  ASSERT_TRUE(simulation->active_node(1U)->joint_membership_can_finalize());

  ASSERT_TRUE(simulation->step(RaftSimulationFinalizeMembershipChange{1U}).is_ok());
  const std::uint64_t final_request_two = message_id(*simulation, 1U, 2U);
  ASSERT_NE(final_request_two, 0U);
  ASSERT_TRUE(simulation->step(RaftSimulationDeliver{final_request_two}).is_ok());
  const std::uint64_t final_response_two = message_id(*simulation, 2U, 1U);
  ASSERT_NE(final_response_two, 0U);
  ASSERT_TRUE(simulation->step(RaftSimulationDeliver{final_response_two}).is_ok());
  EXPECT_TRUE(
      std::ranges::equal(simulation->active_node(1U)->voters(), std::vector<NodeId>{1U, 2U}));

  const std::vector<RaftSimulationAction> setup(simulation->trace().begin(),
                                                simulation->trace().end());
  auto explored = DeterministicRaftSimulator::explore_fault_schedules(
      config(), setup, {.maximum_depth = 1U, .maximum_replays = 2U});

  ASSERT_TRUE(explored.has_value()) << explored.error().to_string();
  EXPECT_FALSE(explored->search_complete);
  ASSERT_TRUE(explored->failure.has_value());
  const common::Status failure = explored->failure.value_or(
      common::Status{common::StatusCode::kInternal, "missing exhaustive failure"});
  EXPECT_EQ(failure.code(), common::StatusCode::kInvalidArgument);
  ASSERT_EQ(explored->failing_trace.size(), setup.size() + 1U);
  EXPECT_EQ(std::get<RaftSimulationDeliver>(explored->failing_trace.back()),
            RaftSimulationDeliver{retained_response});
  auto replay = DeterministicRaftSimulator::create(config());
  ASSERT_TRUE(replay.has_value()) << replay.error().to_string();
  EXPECT_EQ(replay->replay(explored->failing_trace).code(), failure.code());
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

TEST(DeterministicRaftSimulatorTest, ChunkShrinkingUsesABoundedReplayBudgetEffectively) {
  auto limited = config();
  limited.limits.maximum_shrink_replays = 4U;
  std::vector<RaftSimulationAction> failing;
  failing.reserve(65U);
  for (std::size_t index = 0U; index < 64U; ++index) {
    failing.emplace_back(RaftSimulationSetLink{1U, 2U, static_cast<bool>((index & 1U) != 0U)});
  }
  failing.emplace_back(RaftSimulationDeliver{999U});

  auto shrunk = DeterministicRaftSimulator::shrink_failing_trace(limited, failing);
  ASSERT_TRUE(shrunk.has_value()) << shrunk.error().to_string();
  ASSERT_LE(shrunk->size(), 4U);
  ASSERT_FALSE(shrunk->empty());
  EXPECT_EQ(std::get<RaftSimulationDeliver>(shrunk->back()), RaftSimulationDeliver{999U});
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
