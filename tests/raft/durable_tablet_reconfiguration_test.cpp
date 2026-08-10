#include "chronos/common/crc32c.hpp"
#include "chronos/raft/durable_tablet_reconfiguration.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  explicit TemporaryDirectory(const char* name) {
    std::string pattern =
        (std::filesystem::temp_directory_path() / (std::string{name} + "-XXXXXX")).string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

template <typename Identifier> [[nodiscard]] Identifier id(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] GroupId group(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return GroupId{bytes};
}

[[nodiscard]] schema::TableId table_id() {
  return id<schema::TableId>(std::byte{2U});
}

[[nodiscard]] schema::TabletId tablet_id() {
  return id<schema::TabletId>(std::byte{3U});
}

[[nodiscard]] std::vector<std::byte> snapshot_bytes() {
  return {std::byte{1U}, std::byte{2U}};
}

[[nodiscard]] SnapshotTransferMetadata transfer() {
  const auto bytes = snapshot_bytes();
  return {9U, 5U, 1U, bytes.size(), common::crc32c(bytes)};
}

[[nodiscard]] common::Result<TabletMovement>
ready_movement(std::vector<NodeId> voters = {1U, 2U, 3U}, const NodeId source = 1U,
               const NodeId target = 4U) {
  auto movement = TabletMovement::begin(tablet_id(), 7U, source, target, std::move(voters));
  if (!movement.has_value())
    return common::make_unexpected(movement.error());
  const auto bytes = snapshot_bytes();
  common::Status status = movement->begin_snapshot(transfer());
  if (!status.is_ok())
    return common::make_unexpected(std::move(status));
  status = movement->accept_snapshot_chunk(0U, bytes, common::crc32c(bytes));
  if (!status.is_ok())
    return common::make_unexpected(std::move(status));
  status = movement->finish_snapshot();
  if (!status.is_ok())
    return common::make_unexpected(std::move(status));
  status = movement->mark_caught_up(5U);
  if (!status.is_ok())
    return common::make_unexpected(std::move(status));
  return std::move(*movement);
}

[[nodiscard]] TabletMovementCheckpointStorageConfig
checkpoint_config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string(), .tablet_id = tablet_id()};
}

[[nodiscard]] TabletMovementSnapshotChunkStorageConfig
chunk_config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string(),
          .session = TabletMovementSnapshotSession{tablet_id(), 7U, 1U, 4U, transfer()}};
}

[[nodiscard]] TabletReconfigurationActionLedgerConfig
ledger_config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string(), .tablet_id = tablet_id()};
}

[[nodiscard]] common::Result<RaftNode> leader(std::vector<NodeId> voters, const NodeId local = 2U) {
  auto node = RaftNode::create(local, std::move(voters));
  if (!node.has_value())
    return common::make_unexpected(node.error());
  auto election = node->start_election();
  if (!election.has_value())
    return common::make_unexpected(election.error());
  auto vote = node->receive(3U, RequestVoteResponse{1U, true});
  if (!vote.has_value())
    return common::make_unexpected(vote.error());
  vote = node->receive(1U, RequestVoteResponse{1U, true});
  if (!vote.has_value())
    return common::make_unexpected(vote.error());
  return std::move(*node);
}

TEST(DurableTabletReconfigurationTest, CheckpointsPromotionAndCompletionBeforeLiveAdoption) {
  TemporaryDirectory checkpoints{"chronos-durable-reconfiguration-checkpoints"};
  TemporaryDirectory chunks{"chronos-durable-reconfiguration-chunks"};
  auto checkpoint_storage = TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints));
  auto chunk_storage = TabletMovementSnapshotChunkStorage::create(chunk_config(chunks));
  auto movement = ready_movement();
  ASSERT_TRUE(checkpoint_storage.has_value());
  ASSERT_TRUE(chunk_storage.has_value());
  ASSERT_TRUE(movement.has_value());
  const auto bytes = snapshot_bytes();
  ASSERT_TRUE(
      chunk_storage->install(TabletMovementSnapshotChunk{chunk_config(chunks).session, 0U, bytes})
          .has_value());
  ASSERT_TRUE(install_verified_tablet_movement_reference(
                  *checkpoint_storage, *chunk_storage,
                  {1U, TabletMovementCheckpointReference{movement->record(), 7U}})
                  .has_value());
  auto first = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->has_value());
  auto recovered = recover_tablet_movement_generation(**first, *chunk_storage);
  ASSERT_TRUE(recovered.has_value());

  auto source_metadata = MetadataStateMachine::create();
  ASSERT_TRUE(source_metadata.has_value());
  ASSERT_TRUE(source_metadata
                  ->apply_committed(
                      1U, TabletPlacementMetadata{table_id(), tablet_id(), 7U, {1U, 2U, 3U}, 2U})
                  .is_ok());
  auto source_node = leader({1U, 2U, 3U});
  ASSERT_TRUE(source_node.has_value());
  auto promotion_action = reconcile_durable_tablet_reconfiguration(
      *recovered, group(std::byte{10U}), group(std::byte{11U}), table_id(), *source_node,
      *source_metadata, *checkpoint_storage, 2U);
  ASSERT_TRUE(promotion_action.has_value()) << promotion_action.error().to_string();
  EXPECT_FALSE(promotion_action->installed_checkpoint.has_value());
  ASSERT_TRUE(promotion_action->action.has_value());
  EXPECT_EQ(promotion_action->action->kind, TabletReconfigurationActionKind::kBeginJointMembership);
  EXPECT_EQ(recovered->checkpoint_generation, 1U);
  EXPECT_EQ(recovered->movement.record().phase, TabletMovementPhase::kReady);

  auto promoted_metadata = MetadataStateMachine::create();
  ASSERT_TRUE(promoted_metadata.has_value());
  ASSERT_TRUE(
      promoted_metadata
          ->apply_committed(
              1U, TabletPlacementMetadata{table_id(), tablet_id(), 8U, {1U, 2U, 3U, 4U}, 2U})
          .is_ok());
  auto promoted_node = leader({1U, 2U, 3U, 4U});
  ASSERT_TRUE(promoted_node.has_value());
  auto missing_chunks = reconcile_durable_tablet_reconfiguration(
      *recovered, group(std::byte{10U}), group(std::byte{11U}), table_id(), *promoted_node,
      *promoted_metadata, *checkpoint_storage, 2U);
  ASSERT_FALSE(missing_chunks.has_value());
  EXPECT_EQ(missing_chunks.error().code(), common::StatusCode::kNotSupported);
  EXPECT_EQ(recovered->movement.record().phase, TabletMovementPhase::kReady);

  auto promoted = reconcile_durable_tablet_reconfiguration(
      *recovered, group(std::byte{10U}), group(std::byte{11U}), table_id(), *promoted_node,
      *promoted_metadata, *checkpoint_storage, *chunk_storage, 2U);
  ASSERT_TRUE(promoted.has_value()) << promoted.error().to_string();
  ASSERT_TRUE(promoted->installed_checkpoint.has_value());
  EXPECT_EQ(promoted->installed_checkpoint->checkpoint_generation, 2U);
  ASSERT_TRUE(promoted->action.has_value());
  EXPECT_EQ(promoted->action->kind, TabletReconfigurationActionKind::kBeginJointMembership);
  EXPECT_EQ(recovered->movement.record().phase, TabletMovementPhase::kTargetPromoted);
  EXPECT_EQ(recovered->checkpoint_generation, 2U);

  auto complete_metadata = MetadataStateMachine::create();
  ASSERT_TRUE(complete_metadata.has_value());
  ASSERT_TRUE(complete_metadata
                  ->apply_committed(
                      1U, TabletPlacementMetadata{table_id(), tablet_id(), 9U, {2U, 3U, 4U}, 2U})
                  .is_ok());
  auto complete_node = RaftNode::create(2U, {2U, 3U, 4U});
  ASSERT_TRUE(complete_node.has_value());
  auto completed = reconcile_durable_tablet_reconfiguration(
      *recovered, group(std::byte{10U}), group(std::byte{11U}), table_id(), *complete_node,
      *complete_metadata, *checkpoint_storage, *chunk_storage, 2U);
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_TRUE(completed->installed_checkpoint.has_value());
  EXPECT_EQ(completed->installed_checkpoint->checkpoint_generation, 3U);
  EXPECT_FALSE(completed->action.has_value());
  EXPECT_EQ(recovered->movement.record().phase, TabletMovementPhase::kComplete);
  EXPECT_EQ(recovered->checkpoint_generation, 3U);
  auto latest = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(latest.has_value());
  ASSERT_TRUE(latest->has_value());
  auto reopened = recover_tablet_movement_generation(**latest, *chunk_storage);
  ASSERT_TRUE(reopened.has_value());
  EXPECT_EQ(reopened->movement.record().phase, TabletMovementPhase::kComplete);
}

TEST(DurableTabletReconfigurationTest, PreservesSelfContainedRepresentation) {
  TemporaryDirectory checkpoints{"chronos-durable-reconfiguration-self-contained"};
  auto checkpoint_storage = TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints));
  auto movement = ready_movement();
  ASSERT_TRUE(checkpoint_storage.has_value());
  ASSERT_TRUE(movement.has_value());
  ASSERT_TRUE(checkpoint_storage
                  ->install({1U, TabletMovementCheckpoint{movement->record(), snapshot_bytes()}})
                  .has_value());
  auto first = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->has_value());
  auto recovered = recover_tablet_movement_generation(**first);
  ASSERT_TRUE(recovered.has_value());
  auto metadata = MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(
      metadata
          ->apply_committed(
              1U, TabletPlacementMetadata{table_id(), tablet_id(), 8U, {1U, 2U, 3U, 4U}, 2U})
          .is_ok());
  auto node = leader({1U, 2U, 3U, 4U});
  ASSERT_TRUE(node.has_value());

  auto promoted = reconcile_durable_tablet_reconfiguration(*recovered, group(std::byte{10U}),
                                                           group(std::byte{11U}), table_id(), *node,
                                                           *metadata, *checkpoint_storage, 2U);
  ASSERT_TRUE(promoted.has_value()) << promoted.error().to_string();
  auto latest = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(latest.has_value());
  ASSERT_TRUE(latest->has_value());
  EXPECT_TRUE(std::holds_alternative<TabletMovementCheckpointGeneration>((**latest).generation));
}

TEST(DurableTabletReconfigurationTest, CheckpointConflictLeavesLiveMovementUnchanged) {
  TemporaryDirectory checkpoints{"chronos-durable-reconfiguration-conflict-checkpoints"};
  TemporaryDirectory chunks{"chronos-durable-reconfiguration-conflict-chunks"};
  auto checkpoint_storage = TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints));
  auto chunk_storage = TabletMovementSnapshotChunkStorage::create(chunk_config(chunks));
  auto movement = ready_movement();
  ASSERT_TRUE(checkpoint_storage.has_value());
  ASSERT_TRUE(chunk_storage.has_value());
  ASSERT_TRUE(movement.has_value());
  const auto bytes = snapshot_bytes();
  ASSERT_TRUE(
      chunk_storage->install(TabletMovementSnapshotChunk{chunk_config(chunks).session, 0U, bytes})
          .has_value());
  const TabletMovementCheckpointReference ready_reference{movement->record(), 7U};
  ASSERT_TRUE(install_verified_tablet_movement_reference(*checkpoint_storage, *chunk_storage,
                                                         {1U, ready_reference})
                  .has_value());
  auto first = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->has_value());
  auto recovered = recover_tablet_movement_generation(**first, *chunk_storage);
  ASSERT_TRUE(recovered.has_value());
  ASSERT_TRUE(checkpoint_storage->install_reference({2U, ready_reference}).has_value());
  auto metadata = MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(
      metadata
          ->apply_committed(
              1U, TabletPlacementMetadata{table_id(), tablet_id(), 8U, {1U, 2U, 3U, 4U}, 2U})
          .is_ok());
  auto node = leader({1U, 2U, 3U, 4U});
  ASSERT_TRUE(node.has_value());

  auto rejected = reconcile_durable_tablet_reconfiguration(
      *recovered, group(std::byte{10U}), group(std::byte{11U}), table_id(), *node, *metadata,
      *checkpoint_storage, *chunk_storage, 2U);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(recovered->checkpoint_generation, 1U);
  EXPECT_EQ(recovered->movement.record().phase, TabletMovementPhase::kReady);
}

TEST(DurableTabletReconfigurationTest, ReturnsOnlyLedgerPreparedDispatchAndRetriesExactly) {
  TemporaryDirectory checkpoints{"chronos-prepared-reconfiguration-checkpoints"};
  TemporaryDirectory actions{"chronos-prepared-reconfiguration-actions"};
  auto checkpoint_storage = TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints));
  auto action_ledger = TabletReconfigurationActionLedger::create(ledger_config(actions));
  auto movement = ready_movement();
  ASSERT_TRUE(checkpoint_storage.has_value());
  ASSERT_TRUE(action_ledger.has_value());
  ASSERT_TRUE(movement.has_value());
  ASSERT_TRUE(checkpoint_storage
                  ->install({1U, TabletMovementCheckpoint{movement->record(), snapshot_bytes()}})
                  .has_value());
  auto first = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->has_value());
  auto recovered = recover_tablet_movement_generation(**first);
  ASSERT_TRUE(recovered.has_value());
  auto metadata = MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(metadata
                  ->apply_committed(
                      1U, TabletPlacementMetadata{table_id(), tablet_id(), 7U, {1U, 2U, 3U}, 2U})
                  .is_ok());
  auto node = leader({1U, 2U, 3U});
  ASSERT_TRUE(node.has_value());

  auto prepared = reconcile_and_prepare_durable_tablet_reconfiguration(
      *recovered, group(std::byte{10U}), group(std::byte{11U}), table_id(), *node, *metadata,
      *checkpoint_storage, *action_ledger, 2U);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  EXPECT_FALSE(prepared->installed_checkpoint.has_value());
  ASSERT_TRUE(prepared->dispatch.has_value());
  EXPECT_FALSE(prepared->dispatch->preparation().already_present);
  EXPECT_EQ(prepared->dispatch->action().id, prepared->dispatch->preparation().id);
  auto loaded = action_ledger->load(prepared->dispatch->action().id);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  auto expected_bytes = encode_tablet_reconfiguration_action_v1(prepared->dispatch->action());
  ASSERT_TRUE(expected_bytes.has_value());
  EXPECT_EQ(loaded->bytes, *expected_bytes);

  auto retry = reconcile_and_prepare_durable_tablet_reconfiguration(
      *recovered, group(std::byte{10U}), group(std::byte{11U}), table_id(), *node, *metadata,
      *checkpoint_storage, *action_ledger, 2U);
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  ASSERT_TRUE(retry->dispatch.has_value());
  EXPECT_TRUE(retry->dispatch->preparation().already_present);
  EXPECT_EQ(retry->dispatch->action().id, prepared->dispatch->action().id);
  EXPECT_EQ(recovered->checkpoint_generation, 1U);
  EXPECT_EQ(recovered->movement.record().phase, TabletMovementPhase::kReady);

  auto promoted_metadata = MetadataStateMachine::create();
  ASSERT_TRUE(promoted_metadata.has_value());
  ASSERT_TRUE(
      promoted_metadata
          ->apply_committed(
              1U, TabletPlacementMetadata{table_id(), tablet_id(), 8U, {1U, 2U, 3U, 4U}, 2U})
          .is_ok());
  auto promoted_node = leader({1U, 2U, 3U, 4U});
  ASSERT_TRUE(promoted_node.has_value());
  auto promoted = reconcile_and_prepare_durable_tablet_reconfiguration(
      *recovered, group(std::byte{10U}), group(std::byte{11U}), table_id(), *promoted_node,
      *promoted_metadata, *checkpoint_storage, *action_ledger, 2U);
  ASSERT_TRUE(promoted.has_value()) << promoted.error().to_string();
  ASSERT_TRUE(promoted->installed_checkpoint.has_value());
  EXPECT_EQ(promoted->installed_checkpoint->checkpoint_generation, 2U);
  ASSERT_TRUE(promoted->dispatch.has_value());
  EXPECT_FALSE(promoted->dispatch->preparation().already_present);
  EXPECT_EQ(promoted->dispatch->action().id.movement_epoch, 8U);
  EXPECT_EQ(promoted->dispatch->action().id, promoted->dispatch->preparation().id);
  EXPECT_EQ(recovered->checkpoint_generation, 2U);
  EXPECT_EQ(recovered->movement.record().phase, TabletMovementPhase::kTargetPromoted);
}

TEST(DurableTabletReconfigurationTest, LedgerConflictKeepsInstalledPhaseCheckpoint) {
  TemporaryDirectory checkpoints{"chronos-prepared-reconfiguration-conflict-checkpoints"};
  TemporaryDirectory actions{"chronos-prepared-reconfiguration-conflict-actions"};
  auto checkpoint_storage = TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints));
  auto action_ledger = TabletReconfigurationActionLedger::create(ledger_config(actions));
  auto movement = ready_movement();
  ASSERT_TRUE(checkpoint_storage.has_value());
  ASSERT_TRUE(action_ledger.has_value());
  ASSERT_TRUE(movement.has_value());
  ASSERT_TRUE(checkpoint_storage
                  ->install({1U, TabletMovementCheckpoint{movement->record(), snapshot_bytes()}})
                  .has_value());
  auto first = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->has_value());
  auto recovered = recover_tablet_movement_generation(**first);
  ASSERT_TRUE(recovered.has_value());
  TabletReconfigurationAction conflict{
      {tablet_id(), 8U, TabletReconfigurationActionKind::kBeginJointMembership},
      TabletReconfigurationActionKind::kBeginJointMembership,
      {group(std::byte{10U}), BeginMembershipChangeOperation{{1U, 2U, 3U, 4U}}}};
  ASSERT_TRUE(action_ledger->prepare(conflict).has_value());
  auto metadata = MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(
      metadata
          ->apply_committed(
              1U, TabletPlacementMetadata{table_id(), tablet_id(), 8U, {1U, 2U, 3U, 4U}, 2U})
          .is_ok());
  auto node = leader({1U, 2U, 3U, 4U});
  ASSERT_TRUE(node.has_value());

  auto rejected = reconcile_and_prepare_durable_tablet_reconfiguration(
      *recovered, group(std::byte{10U}), group(std::byte{11U}), table_id(), *node, *metadata,
      *checkpoint_storage, *action_ledger, 2U);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(recovered->checkpoint_generation, 2U);
  EXPECT_EQ(recovered->movement.record().phase, TabletMovementPhase::kTargetPromoted);
  auto latest = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(latest.has_value());
  ASSERT_TRUE(latest->has_value());
  EXPECT_EQ(std::visit([](const auto& value) { return value.checkpoint_generation; },
                       (**latest).generation),
            2U);
}

TEST(DurableTabletReconfigurationTest, ExecutesOnlySealedPreparedDispatchAfterRaftSync) {
  TemporaryDirectory checkpoints{"chronos-local-reconfiguration-checkpoints"};
  TemporaryDirectory actions{"chronos-local-reconfiguration-actions"};
  TemporaryDirectory raft_log{"chronos-local-reconfiguration-raft"};
  auto checkpoint_storage = TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints));
  auto action_ledger = TabletReconfigurationActionLedger::create(ledger_config(actions));
  auto movement = ready_movement({2U}, 2U, 4U);
  ASSERT_TRUE(checkpoint_storage.has_value());
  ASSERT_TRUE(action_ledger.has_value());
  ASSERT_TRUE(movement.has_value());
  ASSERT_TRUE(checkpoint_storage
                  ->install({1U, TabletMovementCheckpoint{movement->record(), snapshot_bytes()}})
                  .has_value());
  auto first = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->has_value());
  auto recovered = recover_tablet_movement_generation(**first);
  ASSERT_TRUE(recovered.has_value());
  auto metadata = MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(
      metadata->apply_committed(1U, TabletPlacementMetadata{table_id(), tablet_id(), 7U, {2U}, 2U})
          .is_ok());
  const GroupId tablet_group = group(std::byte{10U});
  const std::vector<RaftGroupConfiguration> groups{{tablet_group, {2U}}};
  const RaftPersistentLogConfig raft_config{.directory_path = raft_log.path().string()};
  auto runtime = DurableMultiRaftRuntime::create_new(2U, raft_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->execute_batch({{tablet_group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_EQ(runtime->find_group(tablet_group)->role(), Role::kLeader);
  auto prepared = reconcile_and_prepare_durable_tablet_reconfiguration(
      *recovered, tablet_group, group(std::byte{11U}), table_id(),
      *runtime->find_group(tablet_group), *metadata, *checkpoint_storage, *action_ledger, 2U);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  ASSERT_TRUE(prepared->dispatch.has_value());
  PreparedTabletReconfigurationDispatch dispatch = std::move(*prepared->dispatch);
  EXPECT_FALSE(prepared->dispatch->is_valid());
  auto moved_from = execute_local_prepared_tablet_reconfiguration(*prepared->dispatch, *runtime);
  ASSERT_FALSE(moved_from.has_value());
  EXPECT_EQ(moved_from.error().code(), common::StatusCode::kInvalidArgument);
  const std::uint64_t durable_before = runtime->durable_physical_sequence();

  auto executed = execute_local_prepared_tablet_reconfiguration(dispatch, *runtime);

  ASSERT_TRUE(executed.has_value()) << executed.error().to_string();
  EXPECT_TRUE(executed->status.is_ok()) << executed->status.to_string();
  ASSERT_TRUE(executed->transition.has_value());
  ASSERT_TRUE(executed->transition->persistence.has_value());
  EXPECT_EQ(executed->transition->persistence->group_id, tablet_group);
  EXPECT_GT(runtime->durable_physical_sequence(), durable_before);
  ASSERT_TRUE(runtime->close().is_ok());
  auto reopened = DurableMultiRaftRuntime::open_existing(2U, raft_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  ASSERT_NE(reopened->find_group(tablet_group), nullptr);
  EXPECT_EQ(reopened->find_group(tablet_group)->persistent_state().log.size(), 1U);
}

TEST(DurableTabletReconfigurationTest, AdmitsPreparedDispatchToBoundedAsyncOwner) {
  TemporaryDirectory checkpoints{"chronos-async-reconfiguration-checkpoints"};
  TemporaryDirectory actions{"chronos-async-reconfiguration-actions"};
  TemporaryDirectory raft_log{"chronos-async-reconfiguration-raft"};
  auto checkpoint_storage = TabletMovementCheckpointStorage::create(checkpoint_config(checkpoints));
  auto action_ledger = TabletReconfigurationActionLedger::create(ledger_config(actions));
  auto movement = ready_movement({2U}, 2U, 4U);
  ASSERT_TRUE(checkpoint_storage.has_value());
  ASSERT_TRUE(action_ledger.has_value());
  ASSERT_TRUE(movement.has_value());
  ASSERT_TRUE(checkpoint_storage
                  ->install({1U, TabletMovementCheckpoint{movement->record(), snapshot_bytes()}})
                  .has_value());
  auto first = checkpoint_storage->load_latest_any();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->has_value());
  auto recovered = recover_tablet_movement_generation(**first);
  ASSERT_TRUE(recovered.has_value());
  auto metadata = MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(
      metadata->apply_committed(1U, TabletPlacementMetadata{table_id(), tablet_id(), 7U, {2U}, 2U})
          .is_ok());
  auto observed_node = RaftNode::create(2U, {2U});
  ASSERT_TRUE(observed_node.has_value());
  ASSERT_TRUE(observed_node->start_election().has_value());
  ASSERT_EQ(observed_node->role(), Role::kLeader);
  const GroupId tablet_group = group(std::byte{10U});
  const std::vector<RaftGroupConfiguration> groups{{tablet_group, {2U}}};
  const RaftPersistentLogConfig raft_config{.directory_path = raft_log.path().string()};
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(2U, raft_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{tablet_group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  auto election_result = election->wait();
  ASSERT_TRUE(election_result.has_value()) << election_result.error().to_string();
  ASSERT_TRUE(election_result->front().status.is_ok());
  auto prepared = reconcile_and_prepare_durable_tablet_reconfiguration(
      *recovered, tablet_group, group(std::byte{11U}), table_id(), *observed_node, *metadata,
      *checkpoint_storage, *action_ledger, 2U);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  ASSERT_TRUE(prepared->dispatch.has_value());

  auto completion = try_submit_local_prepared_tablet_reconfiguration(*prepared->dispatch, *runtime);
  ASSERT_TRUE(completion.has_value()) << completion.error().to_string();
  auto executed = completion->wait();
  ASSERT_TRUE(executed.has_value()) << executed.error().to_string();
  ASSERT_EQ(executed->size(), 1U);
  EXPECT_TRUE(executed->front().status.is_ok()) << executed->front().status.to_string();
  ASSERT_TRUE(executed->front().transition.has_value());
  EXPECT_TRUE(executed->front().transition->persistence.has_value());
  EXPECT_TRUE(runtime->shutdown().is_ok());
  auto rejected = try_submit_local_prepared_tablet_reconfiguration(*prepared->dispatch, *runtime);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(prepared->dispatch->is_valid());
  auto reopened = DurableMultiRaftRuntime::open_existing(2U, raft_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  ASSERT_NE(reopened->find_group(tablet_group), nullptr);
  EXPECT_EQ(reopened->find_group(tablet_group)->persistent_state().log.size(), 1U);
}

} // namespace
} // namespace chronos::raft
