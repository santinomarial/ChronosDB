#include "chronos/ingest/columnar_append_executor.hpp"
#include "chronos/ingest/columnar_append_recovery.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"
#include "wal/wal_writer_test_support.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

[[nodiscard]] schema::TabletId tablet_id(const std::uint16_t value) {
  return columnar::test::id<schema::TabletId>(value);
}

[[nodiscard]] RetryIdentity retry_identity(const std::uint8_t seed) {
  return RetryIdentity{.client_id = test::request_id<ClientId>(seed),
                       .client_batch_id =
                           test::request_id<ClientBatchId>(static_cast<std::uint8_t>(seed + 32U))};
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
batch(const std::byte timestamp_tail = std::byte{0U}) {
  std::shared_ptr<const schema::TableSchema> schema = columnar::test::batch_schema();
  std::vector<columnar::OwnedColumnVector> columns = columnar::test::batch_columns();
  std::vector<std::byte> timestamps(16U);
  timestamps.back() = timestamp_tail;
  columns[0] =
      columnar::test::fixed_vector(1U, columnar::test::type(schema::LogicalTypeKind::kTimestampNs),
                                   false, 2U, {}, 0U, std::move(timestamps));
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(std::move(schema), std::move(columns)).value());
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> successor_batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::successor_batch_schema(),
                                           columnar::test::successor_batch_columns())
          .value());
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema> deduplication_schema() {
  const std::shared_ptr<const schema::TableSchema> base = columnar::test::batch_schema();
  std::vector<schema::ColumnDefinition> columns{base->columns().begin(), base->columns().end()};
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(
          columnar::test::id<schema::TableId>(90U), columnar::test::id<schema::SchemaId>(91U),
          schema::SchemaVersion::initial(), std::nullopt, std::move(columns),
          schema::TableSchemaRoles{
              .event_time_column = columnar::test::id<schema::ColumnId>(1U),
              .physical_ordering_key = {columnar::test::id<schema::ColumnId>(1U)},
              .partition_columns = {columnar::test::id<schema::ColumnId>(1U)},
              .shard_key = {columnar::test::id<schema::ColumnId>(1U)},
              .deduplication_key = {columnar::test::id<schema::ColumnId>(1U),
                                    columnar::test::id<schema::ColumnId>(3U)}})
          .value());
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> deduplication_batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(deduplication_schema(), columnar::test::batch_columns())
          .value());
}

[[nodiscard]] wal::EncodedApplicationPayload
command(const std::uint8_t seed, const schema::TabletId target,
        const std::shared_ptr<const columnar::OwnedColumnarBatch>& input_batch) {
  const RetryIdentity identity = retry_identity(seed);
  columnar::EncodedColumnarBatch encoded = columnar::encode_columnar_batch_v1(*input_batch).value();
  return encode_columnar_append_v1(
             ColumnarAppendEncodeInput{.client_id = identity.client_id,
                                       .client_batch_id = identity.client_batch_id,
                                       .tablet_id = target},
             encoded)
      .value();
}

[[nodiscard]] ColumnarRecoveryRetrySeed retry_seed(const wal::EncodedApplicationPayload& payload,
                                                   const wal::WalId& wal_id,
                                                   const std::uint64_t record_sequence) {
  const ColumnarAppendDecodeResult decoded = decode_columnar_append_v1_exact(payload.bytes());
  EXPECT_TRUE(decoded.has_value());
  return ColumnarRecoveryRetrySeed{
      .identity = RetryIdentity{.client_id = decoded->client_id(),
                                .client_batch_id = decoded->client_batch_id()},
      .outcome = ColumnarAppendRetryOutcome{
          .mutation = ColumnarAppendMutationIdentity{.table_id = decoded->table_id(),
                                                     .tablet_id = decoded->tablet_id(),
                                                     .request_digest = decoded->request_digest()},
          .wal_id = wal_id,
          .record_sequence = record_sequence,
          .applied_row_count = decoded->row_count()}};
}

[[nodiscard]] TabletStateConfig tablet_config(const std::size_t schema_versions = 1U) {
  return TabletStateConfig{
      .head_capacity =
          head::MutableHeadCapacity{.row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
      .maximum_schema_versions = schema_versions,
      .maximum_sealed_generations = 4U,
      .maximum_retry_entries = 8U,
  };
}

[[nodiscard]] ColumnarAppendRecoveryConfig
recovery_config(const std::vector<schema::TabletId>& tablet_ids,
                const bool retain_successor = false) {
  std::vector<ColumnarRecoveryTabletConfig> tablets;
  tablets.reserve(tablet_ids.size());
  for (const schema::TabletId& target : tablet_ids) {
    std::vector<ColumnarRecoverySuccessorSchemaConfig> successors;
    if (retain_successor) {
      successors.push_back(ColumnarRecoverySuccessorSchemaConfig{
          .schema = columnar::test::successor_batch_schema(),
          .head_capacity = head::MutableHeadCapacity{.row_capacity = 8U,
                                                     .variable_value_bytes = {0U, 8U, 0U, 8U}}});
    }
    tablets.push_back(
        ColumnarRecoveryTabletConfig{.schema = columnar::test::batch_schema(),
                                     .tablet_id = target,
                                     .state = tablet_config(retain_successor ? 2U : 1U),
                                     .successors = std::move(successors)});
  }
  return ColumnarAppendRecoveryConfig{.retry_directory = {.maximum_entries = 16U},
                                      .tablets = std::move(tablets),
                                      .decode_limits = {}};
}

struct ReplayTargets {
  schema::TabletId first;
  schema::TabletId second;
};

[[nodiscard]] wal::WalId write_replay_history(const wal::WalWriterConfig& config,
                                              const ReplayTargets& targets) {
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(config);
  EXPECT_TRUE(created.has_value())
      << (created.has_value() ? std::string{} : created.error().to_string());
  if (!created.has_value()) {
    return {};
  }
  wal::WalWriter writer = std::move(*created);
  const wal::WalId wal_id = writer.wal_id();
  const std::shared_ptr<const columnar::OwnedColumnarBatch> input_batch = batch();
  const wal::EncodedApplicationPayload first = command(1U, targets.first, input_batch);
  const wal::EncodedApplicationPayload second = command(2U, targets.second, input_batch);
  const auto first_append = writer.append_application_entry(first.bytes());
  const auto duplicate_append = writer.append_application_entry(first.bytes());
  const auto second_append = writer.append_application_entry(second.bytes());
  EXPECT_TRUE(first_append.has_value());
  EXPECT_TRUE(duplicate_append.has_value());
  EXPECT_TRUE(second_append.has_value());
  EXPECT_EQ(first_append.value_or(wal::WalAppendResult{}).record_sequence, 1U);
  EXPECT_EQ(duplicate_append.value_or(wal::WalAppendResult{}).record_sequence, 2U);
  EXPECT_EQ(second_append.value_or(wal::WalAppendResult{}).record_sequence, 3U);
  EXPECT_TRUE(writer.synchronize().has_value());
  EXPECT_TRUE(writer.close().is_ok());
  return wal_id;
}

TEST(ColumnarAppendRecoveryTest, RebuildsFreshStateDeterministicallyAndContinuesAtNextSequence) {
  wal::test::TemporaryDirectory directory{"chronos-columnar-recovery"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const schema::TabletId first_tablet = tablet_id(70U);
  const schema::TabletId second_tablet = tablet_id(71U);
  const wal::WalId expected_wal_id = write_replay_history(
      writer_config, ReplayTargets{.first = first_tablet, .second = second_tablet});
  ASSERT_TRUE(expected_wal_id.is_valid());

  common::Result<RecoveredColumnarAppendState> first = recover_columnar_append_wal(
      writer_config, {}, recovery_config({first_tablet, second_tablet}));
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_EQ(first->tablet_count(), 2U);
  TabletState* const first_state = first->tablet(first_tablet);
  TabletState* const second_state = first->tablet(second_tablet);
  ASSERT_NE(first_state, nullptr);
  ASSERT_NE(second_state, nullptr);
  const TabletSnapshot first_snapshot = first_state->snapshot().value();
  const TabletSnapshot second_snapshot = second_state->snapshot().value();
  ASSERT_TRUE(first_snapshot.applied_position().has_value());
  ASSERT_TRUE(second_snapshot.applied_position().has_value());
  const head::HeadCommitPosition first_position =
      first_snapshot.applied_position().value_or(head::HeadCommitPosition{});
  const head::HeadCommitPosition second_position =
      second_snapshot.applied_position().value_or(head::HeadCommitPosition{});
  EXPECT_EQ(first_snapshot.visible_row_count(), 2U);
  EXPECT_EQ(first_snapshot.retry_entry_count(), 1U);
  EXPECT_EQ(first_position.wal_id, expected_wal_id);
  EXPECT_EQ(first_position.record_sequence, 2U);
  ASSERT_TRUE(first_snapshot.active_generation().applied_position().has_value());
  const head::HeadCommitPosition active_position =
      first_snapshot.active_generation().applied_position().value_or(head::HeadCommitPosition{});
  EXPECT_EQ(active_position.record_sequence, 1U);
  EXPECT_EQ(second_snapshot.visible_row_count(), 2U);
  EXPECT_EQ(second_snapshot.retry_entry_count(), 1U);
  EXPECT_EQ(second_position.record_sequence, 3U);
  EXPECT_EQ(first->retry_directory().metrics().committed_entries, 2U);

  const std::shared_ptr<const ColumnarAppendRetryOutcome> first_outcome =
      first_snapshot.retry_outcome(retry_identity(1U));
  ASSERT_NE(first_outcome, nullptr);
  common::Result<RetryDecision> matching =
      first->retry_directory().try_reserve(retry_identity(1U), first_outcome->mutation);
  ASSERT_TRUE(matching.has_value());
  EXPECT_EQ(matching->kind(), RetryDecisionKind::kMatchingCommitted);
  EXPECT_EQ(matching->committed_outcome().get(), first_outcome.get());

  common::Result<wal::WalWriter> first_writer = first->release_writer();
  ASSERT_TRUE(first_writer.has_value()) << first_writer.error().to_string();
  EXPECT_FALSE(first->release_writer().has_value());
  const common::Result<wal::WalSegmentReclamationReport> no_writer_reclamation =
      first->reclaim_checkpointed_segments({.wal_id = expected_wal_id,
                                            .record_sequence = 0U,
                                            .segment_number = wal::kFirstSegmentNumber,
                                            .byte_offset = wal::kSegmentHeaderSize});
  ASSERT_FALSE(no_writer_reclamation.has_value());
  EXPECT_EQ(no_writer_reclamation.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(first_writer->close().is_ok());

  common::Result<RecoveredColumnarAppendState> second = recover_columnar_append_wal(
      writer_config, {}, recovery_config({first_tablet, second_tablet}));
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  const TabletSnapshot repeated_first = second->tablet(first_tablet)->snapshot().value();
  const TabletSnapshot repeated_second = second->tablet(second_tablet)->snapshot().value();
  EXPECT_EQ(repeated_first.visible_row_count(), first_snapshot.visible_row_count());
  EXPECT_EQ(repeated_first.retry_entry_count(), first_snapshot.retry_entry_count());
  EXPECT_EQ(repeated_first.applied_position(), first_snapshot.applied_position());
  EXPECT_EQ(repeated_second.visible_row_count(), second_snapshot.visible_row_count());
  EXPECT_EQ(repeated_second.retry_entry_count(), second_snapshot.retry_entry_count());
  EXPECT_EQ(repeated_second.applied_position(), second_snapshot.applied_position());

  common::Result<wal::WalWriter> continued_writer = second->release_writer();
  ASSERT_TRUE(continued_writer.has_value()) << continued_writer.error().to_string();
  common::Result<wal::WalCommitCoordinator> started = wal::WalCommitCoordinator::start(
      std::move(*continued_writer), {.maximum_sync_batch_delay = std::chrono::microseconds{0}});
  ASSERT_TRUE(started.has_value()) << started.error().to_string();
  wal::WalCommitCoordinator coordinator = std::move(*started);
  const RetryIdentity next_identity = retry_identity(3U);
  const auto applied = execute_columnar_append(
      ColumnarAppendExecutionInput{.client_id = next_identity.client_id,
                                   .client_batch_id = next_identity.client_batch_id,
                                   .batch = batch(),
                                   .durability = wal::WalDurabilityMode::kLocalSync},
      second->retry_directory(), *second->tablet(first_tablet), coordinator);
  ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
  ASSERT_TRUE(applied->wal_commit.has_value());
  const wal::WalCommitResult continued_commit =
      applied->wal_commit.value_or(wal::WalCommitResult{});
  EXPECT_EQ(continued_commit.append.record_sequence, 4U);
  EXPECT_EQ(second->tablet(first_tablet)->snapshot()->visible_row_count(), 4U);
  EXPECT_TRUE(coordinator.shutdown().is_ok());
}

TEST(ColumnarAppendRecoveryTest, ReplaysRegisteredSchemaSwitchAndAllowsAnExactAncestorRetryNoOp) {
  wal::test::TemporaryDirectory directory{"chronos-columnar-recovery-schema-switch"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const schema::TabletId target = tablet_id(70U);
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const auto ancestor = batch();
  const auto successor = successor_batch();
  const wal::EncodedApplicationPayload first = command(1U, target, ancestor);
  const wal::EncodedApplicationPayload second = command(2U, target, successor);
  ASSERT_TRUE(writer.append_application_entry(first.bytes()).has_value());
  ASSERT_TRUE(writer.append_application_entry(second.bytes()).has_value());
  ASSERT_TRUE(writer.append_application_entry(first.bytes()).has_value());
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  common::Result<RecoveredColumnarAppendState> recovered =
      recover_columnar_append_wal(writer_config, {}, recovery_config({target}, true));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  const TabletSnapshot snapshot = recovered->tablet(target)->snapshot().value();
  ASSERT_EQ(snapshot.sealed_generations().size(), 1U);
  EXPECT_EQ(snapshot.sealed_generations()[0].schema_ptr()->version().value(), 1U);
  EXPECT_EQ(snapshot.sealed_generations()[0].row_count(), 2U);
  EXPECT_EQ(snapshot.schema_ptr()->version().value(), 2U);
  EXPECT_EQ(snapshot.active_generation().column_count(), 4U);
  EXPECT_EQ(snapshot.active_generation().row_count(), 2U);
  EXPECT_EQ(snapshot.visible_row_count(), 4U);
  EXPECT_EQ(snapshot.retry_entry_count(), 2U);
  ASSERT_TRUE(snapshot.applied_position().has_value());
  EXPECT_EQ(snapshot.applied_position().value_or(head::HeadCommitPosition{}).record_sequence, 3U);
  ASSERT_TRUE(snapshot.active_generation().applied_position().has_value());
  EXPECT_EQ(snapshot.active_generation()
                .applied_position()
                .value_or(head::HeadCommitPosition{})
                .record_sequence,
            2U);
  const auto original = snapshot.retry_outcome(retry_identity(1U));
  ASSERT_NE(original, nullptr);
  EXPECT_EQ(original->record_sequence, 1U);
  EXPECT_TRUE(recovered->release_writer()->close().is_ok());
}

TEST(ColumnarAppendRecoveryTest,
     RestoresManifestPrefixSkipsCoveredCommandsAndAppliesOnlyTheUncoveredSuffix) {
  wal::test::TemporaryDirectory directory{"chronos-columnar-recovery-durable-prefix"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const schema::TabletId target = tablet_id(70U);
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const wal::WalId wal_id = writer.wal_id();
  const wal::EncodedApplicationPayload ancestor = command(1U, target, batch());
  const wal::EncodedApplicationPayload successor = command(2U, target, successor_batch());
  const wal::EncodedApplicationPayload next = command(3U, target, successor_batch());
  const common::Result<wal::WalAppendResult> first =
      writer.append_application_entry(ancestor.bytes());
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(writer.append_application_entry(successor.bytes()).has_value());
  ASSERT_TRUE(writer.append_application_entry(ancestor.bytes()).has_value());
  ASSERT_TRUE(writer.append_application_entry(next.bytes()).has_value());
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  const wal::WalReplayCheckpoint checkpoint{.wal_id = wal_id,
                                            .record_sequence = 1U,
                                            .segment_number = first->record_end.segment_number,
                                            .byte_offset = first->record_end.byte_offset};
  const auto make_config = [&]() {
    ColumnarAppendRecoveryConfig config = recovery_config({target}, true);
    config.checkpoint = checkpoint;
    config.tablets.front().durable_seed = ColumnarRecoveryTabletSeed{
        .recovery_schema_id = successor_batch()->schema().schema_id(),
        .recovery_schema_version = successor_batch()->schema().version(),
        .durable_record_sequence = 2U,
        .retries = {retry_seed(ancestor, wal_id, 1U), retry_seed(successor, wal_id, 2U)}};
    return config;
  };

  for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
    common::Result<RecoveredColumnarAppendState> recovered =
        recover_columnar_append_wal(writer_config, {}, make_config());
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    const TabletSnapshot snapshot = recovered->tablet(target)->snapshot().value();
    EXPECT_EQ(snapshot.schema_ptr()->schema_id(), successor_batch()->schema().schema_id());
    EXPECT_EQ(snapshot.visible_row_count(), 2U);
    EXPECT_EQ(snapshot.sealed_generations().size(), 0U);
    EXPECT_EQ(snapshot.retry_entry_count(), 3U);
    ASSERT_TRUE(snapshot.applied_position().has_value());
    const head::HeadCommitPosition applied_position =
        snapshot.applied_position().value_or(head::HeadCommitPosition{});
    ASSERT_TRUE(snapshot.active_generation().applied_position().has_value());
    const head::HeadCommitPosition active_position =
        snapshot.active_generation().applied_position().value_or(head::HeadCommitPosition{});
    EXPECT_EQ(applied_position.record_sequence, 4U);
    EXPECT_EQ(active_position.record_sequence, 4U);
    EXPECT_EQ(recovered->retry_directory().metrics().committed_entries, 3U);
    EXPECT_EQ(snapshot.retry_outcome(retry_identity(1U))->record_sequence, 1U);
    EXPECT_EQ(snapshot.retry_outcome(retry_identity(2U))->record_sequence, 2U);

    common::Result<wal::WalWriter> reopened = recovered->release_writer();
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_EQ(reopened->next_record_sequence().value(), 5U);
    EXPECT_TRUE(reopened->close().is_ok());
  }
}

TEST(ColumnarAppendRecoveryTest, RejectsAnUnprotectedCommandInsideATabletDurableBoundary) {
  wal::test::TemporaryDirectory directory{"chronos-columnar-recovery-missing-durable-retry"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const schema::TabletId target = tablet_id(70U);
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const wal::WalId wal_id = writer.wal_id();
  const wal::EncodedApplicationPayload first_command = command(1U, target, batch());
  const wal::EncodedApplicationPayload second_command = command(2U, target, successor_batch());
  const common::Result<wal::WalAppendResult> first =
      writer.append_application_entry(first_command.bytes());
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(writer.append_application_entry(second_command.bytes()).has_value());
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  ColumnarAppendRecoveryConfig config = recovery_config({target}, true);
  config.checkpoint = wal::WalReplayCheckpoint{.wal_id = wal_id,
                                               .record_sequence = 1U,
                                               .segment_number = first->record_end.segment_number,
                                               .byte_offset = first->record_end.byte_offset};
  config.tablets.front().durable_seed =
      ColumnarRecoveryTabletSeed{.recovery_schema_id = successor_batch()->schema().schema_id(),
                                 .recovery_schema_version = successor_batch()->schema().version(),
                                 .durable_record_sequence = 2U,
                                 .retries = {retry_seed(first_command, wal_id, 1U)}};

  const common::Result<RecoveredColumnarAppendState> recovered =
      recover_columnar_append_wal(writer_config, {}, std::move(config));
  ASSERT_FALSE(recovered.has_value());
  EXPECT_EQ(recovered.error().code(), common::StatusCode::kCorruption);
}

TEST(ColumnarAppendRecoveryTest, RejectsMalformedDurableSeedsBeforeReturningState) {
  wal::test::TemporaryDirectory directory{"chronos-columnar-recovery-invalid-durable-seed"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const schema::TabletId target = tablet_id(70U);
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const wal::WalId wal_id = writer.wal_id();
  const wal::EncodedApplicationPayload payload = command(1U, target, batch());
  const common::Result<wal::WalAppendResult> appended =
      writer.append_application_entry(payload.bytes());
  ASSERT_TRUE(appended.has_value());
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  ColumnarAppendRecoveryConfig missing_checkpoint = recovery_config({target});
  missing_checkpoint.tablets.front().durable_seed =
      ColumnarRecoveryTabletSeed{.recovery_schema_id = batch()->schema().schema_id(),
                                 .recovery_schema_version = batch()->schema().version(),
                                 .durable_record_sequence = 1U,
                                 .retries = {retry_seed(payload, wal_id, 1U)}};
  common::Result<RecoveredColumnarAppendState> rejected =
      recover_columnar_append_wal(writer_config, {}, std::move(missing_checkpoint));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);

  ColumnarAppendRecoveryConfig unknown_schema = recovery_config({target});
  unknown_schema.checkpoint =
      wal::WalReplayCheckpoint{.wal_id = wal_id,
                               .record_sequence = 1U,
                               .segment_number = appended->record_end.segment_number,
                               .byte_offset = appended->record_end.byte_offset};
  unknown_schema.tablets.front().durable_seed =
      ColumnarRecoveryTabletSeed{.recovery_schema_id = columnar::test::id<schema::SchemaId>(250U),
                                 .recovery_schema_version = batch()->schema().version(),
                                 .durable_record_sequence = 1U,
                                 .retries = {retry_seed(payload, wal_id, 1U)}};
  rejected = recover_columnar_append_wal(writer_config, {}, std::move(unknown_schema));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);

  ColumnarAppendRecoveryConfig repeated_retry = recovery_config({target});
  repeated_retry.checkpoint =
      wal::WalReplayCheckpoint{.wal_id = wal_id,
                               .record_sequence = 1U,
                               .segment_number = appended->record_end.segment_number,
                               .byte_offset = appended->record_end.byte_offset};
  const ColumnarRecoveryRetrySeed seed = retry_seed(payload, wal_id, 1U);
  repeated_retry.tablets.front().durable_seed =
      ColumnarRecoveryTabletSeed{.recovery_schema_id = batch()->schema().schema_id(),
                                 .recovery_schema_version = batch()->schema().version(),
                                 .durable_record_sequence = 1U,
                                 .retries = {seed, seed}};
  rejected = recover_columnar_append_wal(writer_config, {}, std::move(repeated_retry));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);

  ColumnarAppendRecoveryConfig missing_required_suffix = recovery_config({target});
  missing_required_suffix.checkpoint =
      wal::WalReplayCheckpoint{.wal_id = wal_id,
                               .record_sequence = 1U,
                               .segment_number = appended->record_end.segment_number,
                               .byte_offset = appended->record_end.byte_offset};
  ColumnarRecoveryRetrySeed missing = retry_seed(payload, wal_id, 1U);
  missing.outcome.record_sequence = 2U;
  missing_required_suffix.tablets.front().durable_seed =
      ColumnarRecoveryTabletSeed{.recovery_schema_id = batch()->schema().schema_id(),
                                 .recovery_schema_version = batch()->schema().version(),
                                 .durable_record_sequence = 2U,
                                 .retries = {missing}};
  rejected = recover_columnar_append_wal(writer_config, {}, std::move(missing_required_suffix));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
}

TEST(ColumnarAppendRecoveryTest, RejectsAFirstTimeAncestorAppendAfterSchemaActivation) {
  wal::test::TemporaryDirectory directory{"chronos-columnar-recovery-schema-regression"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const schema::TabletId target = tablet_id(70U);
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const wal::EncodedApplicationPayload successor = command(1U, target, successor_batch());
  const wal::EncodedApplicationPayload ancestor = command(2U, target, batch());
  ASSERT_TRUE(writer.append_application_entry(successor.bytes()).has_value());
  ASSERT_TRUE(writer.append_application_entry(ancestor.bytes()).has_value());
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  const auto recovered =
      recover_columnar_append_wal(writer_config, {}, recovery_config({target}, true));
  ASSERT_FALSE(recovered.has_value());
  EXPECT_EQ(recovered.error().code(), common::StatusCode::kCorruption);
}

TEST(ColumnarAppendRecoveryTest, PreflightRequiresEveryReferencedRetainedSchema) {
  wal::test::TemporaryDirectory directory{"chronos-columnar-recovery-missing-schema"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const schema::TabletId target = tablet_id(70U);
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const wal::EncodedApplicationPayload successor = command(1U, target, successor_batch());
  ASSERT_TRUE(writer.append_application_entry(successor.bytes()).has_value());
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  const auto recovered = recover_columnar_append_wal(writer_config, {}, recovery_config({target}));
  ASSERT_FALSE(recovered.has_value());
  EXPECT_EQ(recovered.error().code(), common::StatusCode::kNotFound);
}

TEST(ColumnarAppendRecoveryTest, ExactRetryPrecedesRowDedupButNewIdentityConflictIsCorruption) {
  const schema::TabletId target = tablet_id(70U);
  const auto input = deduplication_batch();
  const auto recovery = [&target, &input](const wal::test::TemporaryDirectory& directory,
                                          const bool matching_retry) {
    const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
    common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
    if (!created.has_value()) {
      return common::Result<RecoveredColumnarAppendState>{common::make_unexpected(created.error())};
    }
    wal::WalWriter writer = std::move(*created);
    const wal::EncodedApplicationPayload first = command(1U, target, input);
    const wal::EncodedApplicationPayload second = command(matching_retry ? 1U : 2U, target, input);
    if (!writer.append_application_entry(first.bytes()).has_value() ||
        !writer.append_application_entry(second.bytes()).has_value() ||
        !writer.synchronize().has_value() || !writer.close().is_ok()) {
      return common::Result<RecoveredColumnarAppendState>{common::make_unexpected(common::Status{
          common::StatusCode::kIoError, "deduplication recovery history write failed"})};
    }
    ColumnarAppendRecoveryConfig config{
        .retry_directory = {.maximum_entries = 4U},
        .tablets = {ColumnarRecoveryTabletConfig{
            .schema = input->schema_ptr(),
            .tablet_id = target,
            .state =
                TabletStateConfig{.head_capacity =
                                      head::MutableHeadCapacity{
                                          .row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
                                  .maximum_schema_versions = 1U,
                                  .maximum_sealed_generations = 2U,
                                  .maximum_retry_entries = 4U},
            .successors = {}}},
        .decode_limits = {}};
    return recover_columnar_append_wal(writer_config, {}, std::move(config));
  };

  wal::test::TemporaryDirectory matching_directory{"chronos-columnar-recovery-dedup-retry"};
  ASSERT_TRUE(matching_directory.valid());
  auto matching = recovery(matching_directory, true);
  ASSERT_TRUE(matching.has_value()) << matching.error().to_string();
  EXPECT_EQ(matching->tablet(target)->snapshot()->visible_row_count(), 2U);
  EXPECT_EQ(matching->tablet(target)->snapshot()->retry_entry_count(), 1U);
  EXPECT_TRUE(matching->release_writer()->close().is_ok());

  wal::test::TemporaryDirectory conflict_directory{"chronos-columnar-recovery-dedup-conflict"};
  ASSERT_TRUE(conflict_directory.valid());
  const auto conflict = recovery(conflict_directory, false);
  ASSERT_FALSE(conflict.has_value());
  EXPECT_EQ(conflict.error().code(), common::StatusCode::kCorruption);
}

TEST(ColumnarAppendRecoveryTest, RejectsConflictingIdentityRepeatablyWithoutReturningPartialState) {
  wal::test::TemporaryDirectory directory{"chronos-columnar-recovery-conflict"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const schema::TabletId target = tablet_id(70U);
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const wal::EncodedApplicationPayload first = command(1U, target, batch());
  const wal::EncodedApplicationPayload conflict = command(1U, target, batch(std::byte{1U}));
  ASSERT_TRUE(writer.append_application_entry(first.bytes()).has_value());
  ASSERT_TRUE(writer.append_application_entry(conflict.bytes()).has_value());
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
    const auto recovered =
        recover_columnar_append_wal(writer_config, {}, recovery_config({target}));
    ASSERT_FALSE(recovered.has_value());
    EXPECT_EQ(recovered.error().code(), common::StatusCode::kCorruption);
  }
}

TEST(ColumnarAppendRecoveryTest, PreflightRejectsAnUnconfiguredTabletBeforeReplay) {
  wal::test::TemporaryDirectory directory{"chronos-columnar-recovery-target"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const schema::TabletId recorded = tablet_id(70U);
  const schema::TabletId configured = tablet_id(71U);
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const wal::EncodedApplicationPayload payload = command(1U, recorded, batch());
  ASSERT_TRUE(writer.append_application_entry(payload.bytes()).has_value());
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  const auto recovered =
      recover_columnar_append_wal(writer_config, {}, recovery_config({configured}));
  ASSERT_FALSE(recovered.has_value());
  EXPECT_EQ(recovered.error().code(), common::StatusCode::kNotFound);
}

TEST(ColumnarAppendRecoveryTest, ClassifiesCompleteWalRecordsWithIncompleteCommandsAsCorruption) {
  wal::test::TemporaryDirectory directory{"chronos-columnar-recovery-incomplete-command"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const schema::TabletId target = tablet_id(70U);
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const wal::EncodedApplicationPayload complete = command(1U, target, batch());
  ASSERT_GT(complete.size(), wal::kApplicationEnvelopeSize);
  ASSERT_TRUE(writer
                  .append_application_entry(
                      complete.bytes().first(complete.size() - static_cast<std::size_t>(1U)))
                  .has_value());
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  const auto recovered = recover_columnar_append_wal(writer_config, {}, recovery_config({target}));
  ASSERT_FALSE(recovered.has_value());
  EXPECT_EQ(recovered.error().code(), common::StatusCode::kCorruption);
}

TEST(ColumnarAppendRecoveryTest, PreservesUnsupportedApplicationClassification) {
  wal::test::TemporaryDirectory directory{"chronos-columnar-recovery-unsupported"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const schema::TabletId target = tablet_id(70U);
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const std::vector<std::byte> body{std::byte{0U}};
  const auto unsupported = wal::encode_application_payload(
      {.application_format = columnar_append_v1::kApplicationFormat,
       .application_kind = columnar_append_v1::kApplicationKind + 1U,
       .application_flags = 0U,
       .application_body = body});
  ASSERT_TRUE(unsupported.has_value()) << unsupported.error().to_string();
  ASSERT_TRUE(writer.append_application_entry(unsupported->bytes()).has_value());
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  const auto recovered = recover_columnar_append_wal(writer_config, {}, recovery_config({target}));
  ASSERT_FALSE(recovered.has_value());
  EXPECT_EQ(recovered.error().code(), common::StatusCode::kNotSupported);
}

} // namespace
} // namespace chronos::ingest
