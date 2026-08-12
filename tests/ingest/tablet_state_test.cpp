#include "chronos/ingest/tablet_state.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"
#include "ingest/tablet_state_internal.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <latch>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(70U);
}

[[nodiscard]] wal::WalId wal_id(const std::uint8_t seed = 1U) {
  wal::WalId id;
  id.bytes.back() = static_cast<std::byte>(seed);
  return id;
}

[[nodiscard]] common::Uuid raft_group_id(const std::uint8_t seed = 9U) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] head::HeadCommitPosition position(const std::uint64_t sequence,
                                                const std::uint8_t wal_seed = 1U) {
  return head::HeadCommitPosition{.wal_id = wal_id(wal_seed), .record_sequence = sequence};
}

[[nodiscard]] RetryIdentity retry_identity(const std::uint8_t seed) {
  return RetryIdentity{.client_id = test::request_id<ClientId>(seed),
                       .client_batch_id =
                           test::request_id<ClientBatchId>(static_cast<std::uint8_t>(seed + 32U))};
}

[[nodiscard]] Sha256Digest digest(const std::uint8_t seed) {
  Sha256Digest::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    bytes[index] = static_cast<std::byte>(seed + index);
  }
  return Sha256Digest{bytes};
}

[[nodiscard]] ColumnarAppendMutationIdentity mutation(
    const std::uint8_t seed,
    const std::shared_ptr<const schema::TableSchema>& schema = columnar::test::batch_schema()) {
  return ColumnarAppendMutationIdentity{
      .table_id = schema->table_id(), .tablet_id = tablet_id(), .request_digest = digest(seed)};
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
batch(std::shared_ptr<const schema::TableSchema> schema = columnar::test::batch_schema()) {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(std::move(schema), columnar::test::batch_columns())
          .value());
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> successor_batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::successor_batch_schema(),
                                           columnar::test::successor_batch_columns())
          .value());
}

[[nodiscard]] head::MutableHeadCapacity successor_capacity(const std::uint32_t rows = 4U) {
  return head::MutableHeadCapacity{.row_capacity = rows, .variable_value_bytes = {0U, 2U, 0U, 2U}};
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema>
renamed_successor(const std::shared_ptr<const schema::TableSchema>& predecessor,
                  const std::uint16_t schema_seed, std::string name) {
  std::vector<schema::ColumnDefinition> columns{predecessor->columns().begin(),
                                                predecessor->columns().end()};
  columns[1] = schema::ColumnDefinition::create(columns[1].id(), std::move(name), columns[1].type(),
                                                columns[1].nullable())
                   .value();
  schema::TableSchemaRoles roles{
      .event_time_column = predecessor->event_time_column(),
      .physical_ordering_key =
          std::vector<schema::ColumnId>{predecessor->physical_ordering_key().begin(),
                                        predecessor->physical_ordering_key().end()},
      .partition_columns = std::vector<schema::ColumnId>{predecessor->partition_columns().begin(),
                                                         predecessor->partition_columns().end()},
      .shard_key = std::vector<schema::ColumnId>{predecessor->shard_key().begin(),
                                                 predecessor->shard_key().end()},
      .deduplication_key = std::vector<schema::ColumnId>{predecessor->deduplication_key().begin(),
                                                         predecessor->deduplication_key().end()},
  };
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(predecessor->table_id(),
                                  columnar::test::id<schema::SchemaId>(schema_seed),
                                  predecessor->version().next().value(), predecessor->schema_id(),
                                  std::move(columns), std::move(roles))
          .value());
}

[[nodiscard]] TabletStateConfig config(const std::uint32_t rows = 4U,
                                       const std::size_t string_bytes = 2U,
                                       const std::size_t sealed = 2U,
                                       const std::size_t retries = 8U,
                                       const std::size_t schema_versions = 1U) {
  return TabletStateConfig{
      .head_capacity = head::MutableHeadCapacity{.row_capacity = rows,
                                                 .variable_value_bytes = {0U, string_bytes, 0U}},
      .maximum_schema_versions = schema_versions,
      .maximum_sealed_generations = sealed,
      .maximum_retry_entries = retries,
      .flush_queue = nullptr};
}

[[nodiscard]] TabletState tablet(const TabletStateConfig& limits = config()) {
  return TabletState::create(columnar::test::batch_schema(), tablet_id(), limits).value();
}

[[nodiscard]] PreparedTabletAppend
prepare(TabletState& target, const std::uint8_t seed,
        const std::shared_ptr<const columnar::OwnedColumnarBatch>& input) {
  auto prepared = target.prepare_append(retry_identity(seed), mutation(seed), input);
  EXPECT_TRUE(prepared.has_value()) << prepared.error().to_string();
  return std::move(*prepared);
}

[[nodiscard]] TabletAppendResult publish(PreparedTabletAppend& prepared,
                                         const std::uint64_t sequence) {
  EXPECT_TRUE(prepared.mark_wal_started().is_ok());
  auto published = prepared.publish(position(sequence));
  EXPECT_TRUE(published.has_value()) << published.error().to_string();
  return std::move(*published);
}

TEST(TabletStateTest, RequiresExplicitBoundsAndStartsAtOneExactEmptyEpoch) {
  const auto schema = columnar::test::batch_schema();
  EXPECT_EQ(TabletState::create(nullptr, tablet_id(), config()).error().code(),
            common::StatusCode::kInvalidArgument);
  TabletStateConfig zero_schemas = config();
  zero_schemas.maximum_schema_versions = 0U;
  EXPECT_EQ(TabletState::create(schema, tablet_id(), zero_schemas).error().code(),
            common::StatusCode::kInvalidArgument);
  TabletStateConfig zero_sealed = config();
  zero_sealed.maximum_sealed_generations = 0U;
  EXPECT_EQ(TabletState::create(schema, tablet_id(), zero_sealed).error().code(),
            common::StatusCode::kInvalidArgument);
  TabletStateConfig zero_retries = config();
  zero_retries.maximum_retry_entries = 0U;
  EXPECT_EQ(TabletState::create(schema, tablet_id(), zero_retries).error().code(),
            common::StatusCode::kInvalidArgument);

  TabletState target = TabletState::create(schema, tablet_id(), config()).value();
  const TabletSnapshot empty = target.snapshot().value();
  EXPECT_EQ(empty.table_id(), schema->table_id());
  EXPECT_EQ(empty.tablet_id(), tablet_id());
  EXPECT_EQ(empty.schema_ptr().get(), schema.get());
  EXPECT_FALSE(empty.applied_position().has_value());
  EXPECT_TRUE(empty.sealed_generations().empty());
  EXPECT_EQ(empty.active_generation().generation(), 1U);
  EXPECT_EQ(empty.active_generation().row_count(), 0U);
  EXPECT_EQ(empty.visible_row_count(), 0U);
  EXPECT_EQ(empty.retry_entry_count(), 0U);
  EXPECT_EQ(empty.retry_outcome(retry_identity(1U)), nullptr);

  const TabletStateMetrics metrics = target.metrics();
  EXPECT_EQ(metrics.maximum_schema_versions, 1U);
  EXPECT_EQ(metrics.maximum_sealed_generations, 2U);
  EXPECT_EQ(metrics.maximum_retry_entries, 8U);
  EXPECT_EQ(metrics.active_generation, 1U);
  EXPECT_EQ(metrics.active_schema_version, 1U);
  EXPECT_EQ(metrics.visible_rows, 0U);
  EXPECT_FALSE(metrics.failed);
}

TEST(TabletStateTest, SwitchesOnlyAcrossRegisteredSuccessorsAndPinsAncestorSnapshots) {
  const auto initial_schema = columnar::test::batch_schema();
  const auto next_schema = columnar::test::successor_batch_schema();
  TabletState target =
      TabletState::create(initial_schema, tablet_id(), config(4U, 2U, 2U, 8U, 2U)).value();
  EXPECT_TRUE(target.register_schema(next_schema, successor_capacity()).is_ok());

  const auto initial_batch = batch(initial_schema);
  PreparedTabletAppend first_prepared = prepare(target, 1U, initial_batch);
  const TabletAppendResult first = publish(first_prepared, 1U);
  const TabletSnapshot ancestor_epoch = first.snapshot;

  const auto next_batch = successor_batch();
  PreparedTabletAppend second_prepared = prepare(target, 2U, next_batch);
  const TabletAppendResult second = publish(second_prepared, 2U);
  ASSERT_EQ(second.snapshot.sealed_generations().size(), 1U);
  EXPECT_EQ(second.snapshot.sealed_generations()[0].schema_ptr().get(), initial_schema.get());
  EXPECT_EQ(second.snapshot.sealed_generations()[0].row_count(), 2U);
  EXPECT_EQ(second.snapshot.schema_ptr().get(), next_schema.get());
  EXPECT_EQ(second.snapshot.active_generation().row_count(), 2U);
  EXPECT_EQ(second.snapshot.active_generation().column_count(), 4U);
  EXPECT_EQ(second.snapshot.visible_row_count(), 4U);
  EXPECT_EQ(second.snapshot.retry_entry_count(), 2U);
  const auto retries = second.snapshot.retry_entries();
  ASSERT_TRUE(retries.has_value()) << retries.error().to_string();
  ASSERT_EQ(retries->size(), 2U);
  EXPECT_EQ(retries->at(0U).identity, retry_identity(1U));
  EXPECT_EQ(retries->at(0U).outcome.get(), first.outcome.get());
  EXPECT_EQ(retries->at(1U).identity, retry_identity(2U));
  EXPECT_EQ(retries->at(1U).outcome.get(), second.outcome.get());
  EXPECT_EQ(target.metrics().active_schema_version, 2U);

  EXPECT_EQ(ancestor_epoch.schema_ptr().get(), initial_schema.get());
  EXPECT_TRUE(ancestor_epoch.sealed_generations().empty());
  EXPECT_EQ(ancestor_epoch.visible_row_count(), 2U);
  EXPECT_EQ(target.prepare_append(retry_identity(3U), mutation(3U), initial_batch).error().code(),
            common::StatusCode::kInvalidArgument);

  const auto advanced =
      target.advance_recovered_retry(retry_identity(1U), mutation(1U), first.outcome, position(3U));
  ASSERT_TRUE(advanced.has_value()) << advanced.error().to_string();
  EXPECT_EQ(advanced->applied_position().value_or(head::HeadCommitPosition{}).record_sequence, 3U);
  EXPECT_EQ(advanced->schema_ptr().get(), next_schema.get());
  EXPECT_EQ(advanced->visible_row_count(), 4U);
  EXPECT_EQ(advanced->retry_outcome(retry_identity(1U)).get(), first.outcome.get());
}

TEST(TabletStateTest, BoundsAndValidatesRegisteredSchemaLineageBeforeWal) {
  const auto successor = columnar::test::successor_batch_schema();
  TabletState bounded = tablet();
  EXPECT_EQ(bounded.register_schema(successor, successor_capacity()).code(),
            common::StatusCode::kResourceExhausted);

  TabletState target =
      TabletState::create(columnar::test::batch_schema(), tablet_id(), config(4U, 2U, 2U, 8U, 2U))
          .value();
  EXPECT_EQ(target.register_schema(nullptr, successor_capacity()).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(target.register_schema(columnar::test::batch_schema(), successor_capacity()).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(target.register_schema(successor, successor_capacity()).is_ok());
  EXPECT_EQ(target.register_schema(successor, successor_capacity()).code(),
            common::StatusCode::kResourceExhausted);
}

TEST(TabletStateTest, EmptyAncestorIsSealedButNotRetainedDuringSchemaActivation) {
  const auto successor = successor_batch();
  TabletState target =
      TabletState::create(columnar::test::batch_schema(), tablet_id(), config(4U, 2U, 1U, 8U, 2U))
          .value();
  ASSERT_TRUE(target.register_schema(successor->schema_ptr(), successor_capacity()).is_ok());
  auto prepared = target.prepare_append(retry_identity(1U), mutation(1U), successor);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();

  const TabletSnapshot activated = target.snapshot().value();
  EXPECT_EQ(activated.schema_ptr()->version().value(), 2U);
  EXPECT_EQ(activated.active_generation().generation(), 2U);
  EXPECT_EQ(activated.active_generation().row_count(), 0U);
  EXPECT_TRUE(activated.sealed_generations().empty());
  EXPECT_TRUE(prepared->cancel_before_wal().is_ok());
  EXPECT_EQ(target.snapshot()->schema_ptr()->version().value(), 2U);
  EXPECT_TRUE(target.snapshot()->sealed_generations().empty());
}

TEST(TabletStateTest, AdvancesAcrossRegisteredVersionsThatHaveNoRows) {
  const auto initial = columnar::test::batch_schema();
  const auto second = columnar::test::successor_batch_schema();
  const auto third = renamed_successor(second, 53U, "label_v3");
  const auto third_batch = std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(third, columnar::test::successor_batch_columns())
          .value());
  TabletState target =
      TabletState::create(initial, tablet_id(), config(4U, 2U, 1U, 8U, 3U)).value();
  ASSERT_TRUE(target.register_schema(second, successor_capacity()).is_ok());
  ASSERT_TRUE(target.register_schema(third, successor_capacity()).is_ok());

  PreparedTabletAppend prepared = prepare(target, 1U, third_batch);
  const TabletAppendResult published = publish(prepared, 1U);
  EXPECT_EQ(published.snapshot.schema_ptr().get(), third.get());
  EXPECT_EQ(published.snapshot.active_generation().generation(), 2U);
  EXPECT_TRUE(published.snapshot.sealed_generations().empty());
  EXPECT_EQ(published.snapshot.visible_row_count(), 2U);
}

TEST(TabletStatePropertyTest, GeneratedLinearRenamesPreserveEveryPublishedGeneration) {
  constexpr std::size_t kSchemaCount = 4U;
  std::vector<std::shared_ptr<const schema::TableSchema>> schemas;
  schemas.push_back(columnar::test::batch_schema());
  for (std::size_t index = 1U; index < kSchemaCount; ++index) {
    schemas.push_back(renamed_successor(schemas.back(), static_cast<std::uint16_t>(60U + index),
                                        "tag_v" + std::to_string(index)));
  }

  TabletState target =
      TabletState::create(schemas.front(), tablet_id(), config(4U, 2U, 4U, 8U, kSchemaCount))
          .value();
  for (std::size_t index = 1U; index < schemas.size(); ++index) {
    ASSERT_TRUE(target
                    .register_schema(schemas[index],
                                     head::MutableHeadCapacity{
                                         .row_capacity = 4U, .variable_value_bytes = {0U, 2U, 0U}})
                    .is_ok());
  }

  std::vector<TabletSnapshot> epochs;
  std::shared_ptr<const ColumnarAppendRetryOutcome> first_outcome;
  for (std::size_t index = 0U; index < schemas.size(); ++index) {
    const auto input = batch(schemas[index]);
    PreparedTabletAppend prepared = prepare(target, static_cast<std::uint8_t>(index + 1U), input);
    TabletAppendResult result = publish(prepared, index + 1U);
    if (index == 0U) {
      first_outcome = result.outcome;
    }
    epochs.push_back(result.snapshot);
    EXPECT_EQ(result.snapshot.schema_ptr().get(), schemas[index].get());
    EXPECT_EQ(result.snapshot.sealed_generations().size(), index);
    EXPECT_EQ(result.snapshot.visible_row_count(), (index + 1U) * 2U);
    EXPECT_EQ(result.snapshot.retry_entry_count(), index + 1U);
  }

  ASSERT_NE(first_outcome, nullptr);
  for (std::size_t index = 0U; index < epochs.size(); ++index) {
    EXPECT_EQ(epochs[index].schema_ptr().get(), schemas[index].get());
    EXPECT_EQ(epochs[index].visible_row_count(), (index + 1U) * 2U);
  }
  const auto advanced =
      target.advance_recovered_retry(retry_identity(1U), mutation(1U), first_outcome, position(5U));
  ASSERT_TRUE(advanced.has_value()) << advanced.error().to_string();
  EXPECT_EQ(advanced->schema_ptr().get(), schemas.back().get());
  EXPECT_EQ(advanced->visible_row_count(), schemas.size() * 2U);
  EXPECT_EQ(advanced->applied_position().value_or(head::HeadCommitPosition{}).record_sequence, 5U);
}

TEST(TabletStateTest, PreparesWithoutVisibilityAndCancelsBeforeWal) {
  TabletState target = tablet();
  const auto input = batch();
  PreparedTabletAppend prepared = prepare(target, 1U, input);
  EXPECT_FALSE(prepared.wal_started());
  EXPECT_EQ(target.snapshot()->visible_row_count(), 0U);
  EXPECT_EQ(target.snapshot()->retry_entry_count(), 0U);
  EXPECT_EQ(target.prepare_append(retry_identity(2U), mutation(2U), input).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(prepared.publish(position(1U)).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(prepared.cancel_before_wal().is_ok());
  EXPECT_FALSE(prepared.is_valid());
  EXPECT_EQ(target.snapshot()->visible_row_count(), 0U);
}

TEST(TabletStateTest, PublishesRowsPositionAndExactRetryOutcomeTogether) {
  TabletState target = tablet();
  const auto input = batch();
  PreparedTabletAppend prepared = prepare(target, 1U, input);
  const TabletAppendResult result = publish(prepared, 7U);
  ASSERT_TRUE(result.snapshot.applied_position().has_value());
  EXPECT_EQ(result.snapshot.applied_position().value_or(head::HeadCommitPosition{}), position(7U));
  EXPECT_EQ(result.snapshot.active_generation().row_count(), 2U);
  EXPECT_EQ(result.snapshot.visible_row_count(), 2U);
  EXPECT_EQ(result.snapshot.retry_entry_count(), 1U);
  EXPECT_EQ(result.snapshot.retry_outcome(retry_identity(1U)).get(), result.outcome.get());
  EXPECT_EQ(result.outcome->mutation, mutation(1U));
  EXPECT_EQ(result.outcome->wal_id, wal_id());
  EXPECT_EQ(result.outcome->record_sequence, 7U);
  EXPECT_EQ(result.outcome->applied_row_count, input->row_count());
  EXPECT_EQ(result.snapshot.active_generation().row_metadata(1U)->commit_position, position(7U));

  const TabletSnapshot reacquired = target.snapshot().value();
  EXPECT_EQ(reacquired.retry_outcome(retry_identity(1U)).get(), result.outcome.get());
  EXPECT_EQ(reacquired.active_generation().row_count(), 2U);
}

TEST(TabletStateTest, PublishesRaftCommitIdentityIntoRowsAndRetryOutcome) {
  TabletState target = tablet();
  const auto input = batch();
  PreparedTabletAppend prepared = prepare(target, 1U, input);
  ASSERT_TRUE(prepared.mark_wal_started().is_ok());
  const head::HeadCommitPosition raft = head::HeadCommitPosition::raft(raft_group_id(), 13U);
  const common::Result<TabletAppendResult> result = prepared.publish(raft);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->snapshot.applied_position(), raft);
  EXPECT_EQ(result->outcome->commit_source, head::CommitSource::kRaft);
  EXPECT_FALSE(result->outcome->wal_id.is_valid());
  EXPECT_EQ(result->outcome->raft_group_id, raft_group_id());
  EXPECT_EQ(result->outcome->record_sequence, 13U);
  EXPECT_EQ(result->snapshot.active_generation().row_metadata(1U)->commit_position, raft);
}

TEST(TabletStateTest, MatchingRecoveredRetryAdvancesOnlyTheOuterAppliedPosition) {
  TabletState target = tablet();
  const auto input = batch();
  PreparedTabletAppend prepared = prepare(target, 1U, input);
  const TabletAppendResult first = publish(prepared, 1U);
  const TabletSnapshot old_epoch = first.snapshot;

  const auto advanced =
      target.advance_recovered_retry(retry_identity(1U), mutation(1U), first.outcome, position(3U));
  ASSERT_TRUE(advanced.has_value()) << advanced.error().to_string();
  EXPECT_EQ(advanced->applied_position().value_or(head::HeadCommitPosition{}), position(3U));
  EXPECT_EQ(advanced->visible_row_count(), 2U);
  EXPECT_EQ(advanced->retry_entry_count(), 1U);
  EXPECT_EQ(advanced->retry_outcome(retry_identity(1U)).get(), first.outcome.get());
  EXPECT_EQ(advanced->active_generation().applied_position().value_or(head::HeadCommitPosition{}),
            position(1U));

  EXPECT_EQ(old_epoch.applied_position().value_or(head::HeadCommitPosition{}), position(1U));
  EXPECT_EQ(old_epoch.visible_row_count(), 2U);
  EXPECT_EQ(
      target.advance_recovered_retry(retry_identity(2U), mutation(1U), first.outcome, position(4U))
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      target.advance_recovered_retry(retry_identity(1U), mutation(1U), first.outcome, position(1U))
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
}

TEST(TabletStateTest, HandsTheExactPublishedOutcomeToTheGlobalRetryDirectory) {
  TabletState target = tablet();
  RetryDirectory directory = RetryDirectory::create({.maximum_entries = 2U}).value();
  const RetryIdentity identity = retry_identity(1U);
  const ColumnarAppendMutationIdentity request = mutation(1U);
  auto decision = directory.try_reserve(identity, request);
  ASSERT_TRUE(decision.has_value());
  ASSERT_EQ(decision->kind(), RetryDecisionKind::kReserved);
  RetryReservation reservation = std::move(*decision->reservation());
  PreparedTabletAppend prepared = target.prepare_append(identity, request, batch()).value();

  EXPECT_TRUE(reservation.mark_wal_started().is_ok());
  EXPECT_TRUE(prepared.mark_wal_started().is_ok());
  const TabletAppendResult published = prepared.publish(position(9U)).value();
  const auto committed = reservation.commit_published(published.outcome);
  ASSERT_TRUE(committed.has_value()) << committed.error().to_string();
  EXPECT_EQ(committed->get(), published.outcome.get());

  auto retry = directory.try_reserve(identity, request);
  ASSERT_TRUE(retry.has_value());
  EXPECT_EQ(retry->kind(), RetryDecisionKind::kMatchingCommitted);
  EXPECT_EQ(retry->committed_outcome().get(), published.outcome.get());
}

TEST(TabletStateTest, RotatesWholeBatchesAndBackpressuresAtTheSealedBound) {
  TabletState target = tablet(config(2U, 1U, 1U, 8U));
  const auto input = batch();
  PreparedTabletAppend first = prepare(target, 1U, input);
  const TabletSnapshot first_snapshot = publish(first, 1U).snapshot;

  PreparedTabletAppend cancelled_rotation = prepare(target, 2U, input);
  const TabletSnapshot topology = target.snapshot().value();
  ASSERT_EQ(topology.sealed_generations().size(), 1U);
  EXPECT_EQ(topology.sealed_generations()[0].generation(), 1U);
  EXPECT_EQ(topology.sealed_generations()[0].row_count(), 2U);
  EXPECT_EQ(topology.active_generation().generation(), 2U);
  EXPECT_EQ(topology.active_generation().row_count(), 0U);
  EXPECT_EQ(topology.applied_position(), first_snapshot.applied_position());
  EXPECT_EQ(topology.retry_entry_count(), 1U);
  EXPECT_TRUE(cancelled_rotation.cancel_before_wal().is_ok());

  PreparedTabletAppend second = prepare(target, 3U, input);
  const TabletSnapshot second_snapshot = publish(second, 3U).snapshot;
  EXPECT_EQ(second_snapshot.visible_row_count(), 4U);
  EXPECT_EQ(second_snapshot.active_generation().generation(), 2U);
  EXPECT_EQ(second_snapshot.active_generation().row_count(), 2U);
  EXPECT_EQ(second_snapshot.retry_entry_count(), 2U);

  const auto blocked = target.prepare_append(retry_identity(4U), mutation(4U), input);
  ASSERT_FALSE(blocked.has_value());
  EXPECT_EQ(blocked.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 4U);
  EXPECT_EQ(target.snapshot()->sealed_generations().size(), 1U);

  EXPECT_EQ(first_snapshot.visible_row_count(), 2U);
  EXPECT_TRUE(first_snapshot.sealed_generations().empty());
  EXPECT_EQ(first_snapshot.active_generation().generation(), 1U);
}

TEST(TabletStateTest, RejectsOversizedBatchAndRetryBoundBeforeWal) {
  const auto input = batch();
  TabletState oversized = tablet(config(1U, 1U, 1U, 4U));
  EXPECT_EQ(oversized.prepare_append(retry_identity(1U), mutation(1U), input).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(oversized.snapshot()->sealed_generations().empty());
  EXPECT_EQ(oversized.snapshot()->active_generation().generation(), 1U);

  TabletState retry_limited = tablet(config(4U, 2U, 1U, 1U));
  PreparedTabletAppend first = prepare(retry_limited, 1U, input);
  static_cast<void>(publish(first, 1U));
  EXPECT_EQ(retry_limited.prepare_append(retry_identity(2U), mutation(2U), input).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(retry_limited.snapshot()->retry_entry_count(), 1U);
  EXPECT_EQ(retry_limited.prepare_append(retry_identity(1U), mutation(1U), input).error().code(),
            common::StatusCode::kAlreadyExists);
  EXPECT_EQ(retry_limited.prepare_append(retry_identity(1U), mutation(9U), input).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(TabletStateTest, InvalidPostWalPositionFailsClosedAtTheOldOuterEpoch) {
  TabletState target = tablet(config(6U, 3U));
  const auto input = batch();
  PreparedTabletAppend first = prepare(target, 1U, input);
  static_cast<void>(publish(first, 2U));
  PreparedTabletAppend invalid_append = prepare(target, 2U, input);
  EXPECT_TRUE(invalid_append.mark_wal_started().is_ok());
  EXPECT_EQ(invalid_append.publish(position(2U)).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(target.metrics().failed);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 2U);
  EXPECT_EQ(target.snapshot()->retry_entry_count(), 1U);
  EXPECT_EQ(target.prepare_append(retry_identity(3U), mutation(3U), input).error().code(),
            common::StatusCode::kUnavailable);
}

class OuterPublicationGate {
public:
  static void pause(void* const context) noexcept {
    auto& gate = *static_cast<OuterPublicationGate*>(context);
    gate.reached_.store(true, std::memory_order_release);
    while (!gate.released_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }

  [[nodiscard]] bool wait_until_reached() const {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (!reached_.load(std::memory_order_acquire)) {
      if (std::chrono::steady_clock::now() >= deadline) {
        return false;
      }
      std::this_thread::yield();
    }
    return true;
  }

  void release() noexcept {
    released_.store(true, std::memory_order_release);
  }

private:
  std::atomic<bool> reached_{false};
  std::atomic<bool> released_{false};
};

TEST(TabletStateConcurrencyTest, InnerHeadPublicationRemainsHiddenUntilOuterPublication) {
  const auto input = batch();
  OuterPublicationGate gate;
  TabletState target =
      detail::TabletStateTestAccess::create(columnar::test::batch_schema(), tablet_id(), config(),
                                            &OuterPublicationGate::pause, &gate)
          .value();
  std::atomic<bool> failed{false};
  std::thread writer{[&] {
    auto prepared = target.prepare_append(retry_identity(1U), mutation(1U), input);
    if (!prepared.has_value() || !prepared->mark_wal_started().is_ok() ||
        !prepared->publish(position(1U)).has_value()) {
      failed.store(true, std::memory_order_release);
      gate.release();
    }
  }};

  if (!gate.wait_until_reached()) {
    gate.release();
    writer.join();
    FAIL() << "writer did not reach the outer-publication gate";
    return;
  }
  const TabletSnapshot old_epoch = target.snapshot().value();
  EXPECT_FALSE(old_epoch.applied_position().has_value());
  EXPECT_EQ(old_epoch.visible_row_count(), 0U);
  EXPECT_EQ(old_epoch.retry_entry_count(), 0U);
  EXPECT_EQ(old_epoch.active_generation().row_count(), 0U);
  gate.release();
  writer.join();

  EXPECT_FALSE(failed.load(std::memory_order_acquire));
  const TabletSnapshot new_epoch = target.snapshot().value();
  EXPECT_EQ(new_epoch.visible_row_count(), 2U);
  EXPECT_EQ(new_epoch.retry_entry_count(), 1U);
  ASSERT_TRUE(new_epoch.applied_position().has_value());
  EXPECT_EQ(new_epoch.applied_position().value_or(head::HeadCommitPosition{}), position(1U));
}

class RetirementPublicationGate {
public:
  static void pause(void* context) noexcept {
    auto* const gate = static_cast<RetirementPublicationGate*>(context);
    if (!gate->armed_.load(std::memory_order_acquire)) {
      return;
    }
    gate->reached_.store(true, std::memory_order_release);
    while (!gate->released_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }

  void arm() noexcept {
    armed_.store(true, std::memory_order_release);
  }
  [[nodiscard]] bool wait_until_reached() const noexcept {
    for (std::size_t attempt = 0U; attempt < 100'000U; ++attempt) {
      if (reached_.load(std::memory_order_acquire)) {
        return true;
      }
      std::this_thread::yield();
    }
    return false;
  }
  void release() noexcept {
    released_.store(true, std::memory_order_release);
  }

private:
  std::atomic<bool> armed_{false};
  std::atomic<bool> reached_{false};
  std::atomic<bool> released_{false};
};

TEST(TabletStateConcurrencyTest, AuthorizedRetirementPublishesOneCompleteOuterEpoch) {
  RetirementPublicationGate gate;
  TabletState target = detail::TabletStateTestAccess::create(
                           columnar::test::batch_schema(), tablet_id(), config(2U, 2U, 1U, 8U),
                           &RetirementPublicationGate::pause, &gate)
                           .value();
  const auto input = batch();
  PreparedTabletAppend first = prepare(target, 1U, input);
  static_cast<void>(publish(first, 1U));
  PreparedTabletAppend second = prepare(target, 2U, input);
  static_cast<void>(publish(second, 2U));
  const TabletSnapshot before = target.snapshot().value();
  ASSERT_EQ(before.sealed_generations().size(), 1U);
  const auto receipt = detail::TabletStateTestAccess::retirement_receipt(
      before.table_id(), before.tablet_id(),
      before.sealed_generations().front().schema_ptr()->schema_id(),
      before.sealed_generations().front().schema_ptr()->version(),
      before.sealed_generations().front().generation(),
      before.sealed_generations().front().row_count(), wal_id(), 1U, 1U);

  gate.arm();
  std::atomic<bool> failed{false};
  std::thread writer{[&] {
    if (!target.retire_sealed_generation(receipt).has_value()) {
      failed.store(true, std::memory_order_release);
      gate.release();
    }
  }};
  if (!gate.wait_until_reached()) {
    gate.release();
    writer.join();
    FAIL() << "writer did not reach the retirement-publication gate";
    return;
  }
  const TabletSnapshot during = target.snapshot().value();
  EXPECT_EQ(during.sealed_generations().size(), 1U);
  EXPECT_EQ(during.visible_row_count(), 4U);
  gate.release();
  writer.join();

  EXPECT_FALSE(failed.load(std::memory_order_acquire));
  const TabletSnapshot after = target.snapshot().value();
  EXPECT_TRUE(after.sealed_generations().empty());
  EXPECT_EQ(after.visible_row_count(), 2U);
  EXPECT_EQ(after.retry_entry_count(), 2U);
  EXPECT_EQ(during.sealed_generations().size(), 1U);
  EXPECT_EQ(during.visible_row_count(), 4U);
}

TEST(TabletStateTest, RetirementReceiptMustMatchExactSealedIdentityAndWalBounds) {
  TabletState target = tablet(config(2U, 2U, 1U, 8U));
  const auto input = batch();
  PreparedTabletAppend first = prepare(target, 1U, input);
  static_cast<void>(publish(first, 1U));
  PreparedTabletAppend second = prepare(target, 2U, input);
  static_cast<void>(publish(second, 2U));
  const TabletSnapshot before = target.snapshot().value();
  ASSERT_EQ(before.sealed_generations().size(), 1U);
  const head::HeadSnapshot& sealed = before.sealed_generations().front();

  const auto wrong_bounds = detail::TabletStateTestAccess::retirement_receipt(
      before.table_id(), before.tablet_id(), sealed.schema_ptr()->schema_id(),
      sealed.schema_ptr()->version(), sealed.generation(), sealed.row_count(), wal_id(), 2U, 2U);
  EXPECT_EQ(target.retire_sealed_generation(wrong_bounds).error().code(),
            common::StatusCode::kInvalidArgument);
  const auto active_generation = detail::TabletStateTestAccess::retirement_receipt(
      before.table_id(), before.tablet_id(), before.active_generation().schema_ptr()->schema_id(),
      before.active_generation().schema_ptr()->version(), before.active_generation().generation(),
      before.active_generation().row_count(), wal_id(), 2U, 2U);
  EXPECT_EQ(target.retire_sealed_generation(active_generation).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(target.snapshot()->sealed_generations().size(), 1U);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 4U);
}

TEST(TabletStateConcurrencyTest, AcquireReadersObserveOnlyCompleteOuterEpochs) {
  constexpr std::size_t kReaders = 4U;
  constexpr std::uint64_t kBatches = 48U;
  TabletState target = tablet(config(static_cast<std::uint32_t>(kBatches * 2U), kBatches, 1U,
                                     static_cast<std::size_t>(kBatches)));
  const auto input = batch();
  std::latch start{static_cast<std::ptrdiff_t>(kReaders + 1U)};
  std::atomic<bool> done{false};
  std::atomic<std::size_t> failures{0U};
  std::atomic<std::size_t> observations{0U};
  std::vector<std::thread> readers;
  readers.reserve(kReaders);
  for (std::size_t index = 0U; index < kReaders; ++index) {
    static_cast<void>(index);
    readers.emplace_back([&] {
      start.arrive_and_wait();
      while (!done.load(std::memory_order_acquire)) {
        const TabletSnapshot observed = target.snapshot().value();
        const std::size_t rows = observed.visible_row_count();
        ++observations;
        if (rows % 2U != 0U || observed.retry_entry_count() != rows / 2U ||
            (rows == 0U) != !observed.applied_position().has_value()) {
          ++failures;
          continue;
        }
        if (rows != 0U &&
            observed.applied_position()->record_sequence != observed.retry_entry_count()) {
          ++failures;
        }
      }
    });
  }

  start.arrive_and_wait();
  for (std::uint64_t sequence = 1U; sequence <= kBatches; ++sequence) {
    const auto seed = static_cast<std::uint8_t>(sequence);
    PreparedTabletAppend prepared = prepare(target, seed, input);
    static_cast<void>(publish(prepared, sequence));
  }
  done.store(true, std::memory_order_release);
  for (auto& reader : readers) {
    reader.join();
  }

  EXPECT_EQ(failures.load(), 0U);
  EXPECT_GT(observations.load(), 0U);
  EXPECT_EQ(target.snapshot()->visible_row_count(), kBatches * 2U);
  EXPECT_EQ(target.snapshot()->retry_entry_count(), kBatches);
}

} // namespace
} // namespace chronos::ingest
