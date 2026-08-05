#include "chronos/head/mutable_head.hpp"
#include "columnar/columnar_test_support.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <latch>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::head {
namespace {

[[nodiscard]] schema::TabletId tablet_id(const std::uint16_t value = 70U) {
  return columnar::test::id<schema::TabletId>(value);
}

[[nodiscard]] wal::WalId wal_id(const std::uint8_t seed = 1U) {
  wal::WalId id;
  id.bytes.back() = static_cast<std::byte>(seed);
  return id;
}

[[nodiscard]] HeadCommitPosition position(const std::uint64_t sequence,
                                          const std::uint8_t wal_seed = 1U) {
  return HeadCommitPosition{.wal_id = wal_id(wal_seed), .record_sequence = sequence};
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
batch(std::shared_ptr<const schema::TableSchema> schema = columnar::test::batch_schema()) {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(std::move(schema), columnar::test::batch_columns())
          .value());
}

[[nodiscard]] MutableHead head(const std::uint32_t rows = 8U,
                               const std::size_t string_bytes = 8U) {
  return MutableHead::create(columnar::test::batch_schema(), tablet_id(), 1U,
                             MutableHeadCapacity{.row_capacity = rows,
                                                 .variable_value_bytes = {0U, string_bytes, 0U}})
      .value();
}

[[nodiscard]] PreparedHeadAppend prepare(MutableHead& target,
                                         const std::shared_ptr<const columnar::OwnedColumnarBatch>&
                                             input,
                                         const std::uint64_t sequence) {
  auto prepared = target.prepare_append(input, position(sequence));
  EXPECT_TRUE(prepared.has_value()) << prepared.error().to_string();
  return std::move(*prepared);
}

[[nodiscard]] HeadSnapshot publish(PreparedHeadAppend& prepared) {
  EXPECT_TRUE(prepared.mark_wal_started().is_ok());
  auto snapshot = prepared.publish();
  EXPECT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  return std::move(*snapshot);
}

TEST(MutableHeadTest, ValidatesConfigurationAndStartsAtOneExactEmptyPublication) {
  const std::shared_ptr<const schema::TableSchema> schema = columnar::test::batch_schema();
  EXPECT_EQ(MutableHead::create(nullptr, tablet_id(), 1U,
                                MutableHeadCapacity{.row_capacity = 4U,
                                                    .variable_value_bytes = {0U, 8U, 0U}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(MutableHead::create(schema, tablet_id(), 0U,
                                MutableHeadCapacity{.row_capacity = 4U,
                                                    .variable_value_bytes = {0U, 8U, 0U}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(MutableHead::create(schema, tablet_id(), 1U,
                                MutableHeadCapacity{.row_capacity = 0U,
                                                    .variable_value_bytes = {0U, 8U, 0U}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(MutableHead::create(schema, tablet_id(), 1U,
                                MutableHeadCapacity{.row_capacity = 4U,
                                                    .variable_value_bytes = {0U, 8U}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(MutableHead::create(schema, tablet_id(), 1U,
                                MutableHeadCapacity{.row_capacity = 4U,
                                                    .variable_value_bytes = {1U, 8U, 0U}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  MutableHead target =
      MutableHead::create(schema, tablet_id(), 1U,
                          MutableHeadCapacity{.row_capacity = 4U,
                                              .variable_value_bytes = {0U, 8U, 0U}})
          .value();
  const MutableHeadMetrics metrics = target.metrics();
  EXPECT_EQ(metrics.row_capacity, 4U);
  EXPECT_EQ(metrics.published_rows, 0U);
  EXPECT_EQ(metrics.variable_byte_capacity, 8U);
  EXPECT_EQ(metrics.published_variable_bytes, 0U);
  EXPECT_GT(metrics.retained_storage_bytes, 8U);
  EXPECT_FALSE(metrics.sealed);
  EXPECT_FALSE(metrics.failed);

  const HeadSnapshot empty = target.snapshot().value();
  EXPECT_EQ(empty.table_id(), schema->table_id());
  EXPECT_EQ(empty.tablet_id(), tablet_id());
  EXPECT_EQ(empty.schema_ptr().get(), schema.get());
  EXPECT_EQ(empty.generation(), 1U);
  EXPECT_EQ(empty.row_count(), 0U);
  EXPECT_EQ(empty.column_count(), 3U);
  EXPECT_FALSE(empty.applied_position().has_value());
  const HeadColumnView strings = empty.column(1U).value();
  EXPECT_TRUE(strings.validity().empty());
  ASSERT_EQ(strings.variable_offsets().size(), 1U);
  EXPECT_EQ(strings.variable_offsets().front(), 0U);
  EXPECT_TRUE(strings.variable_values().empty());
  EXPECT_EQ(empty.column(3U).error().code(), common::StatusCode::kOutOfRange);
  EXPECT_EQ(empty.row_metadata(0U).error().code(), common::StatusCode::kOutOfRange);
}

TEST(MutableHeadTest, PreparationAllocatesAndReservesWithoutChangingVisibility) {
  MutableHead target = head(4U, 2U);
  const auto input = batch();
  PreparedHeadAppend prepared = prepare(target, input, 1U);
  EXPECT_TRUE(prepared.is_valid());
  EXPECT_FALSE(prepared.wal_started());
  EXPECT_EQ(target.snapshot()->row_count(), 0U);
  EXPECT_EQ(target.prepare_append(input, position(2U)).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(prepared.publish().error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(target.snapshot()->row_count(), 0U);
  EXPECT_EQ(target.seal().error().code(), common::StatusCode::kUnavailable);

  EXPECT_TRUE(prepared.cancel_before_wal().is_ok());
  EXPECT_FALSE(prepared.is_valid());
  EXPECT_EQ(target.snapshot()->row_count(), 0U);
  EXPECT_TRUE(target.prepare_append(input, position(1U)).has_value());
}

TEST(MutableHeadTest, PublishesTheCompleteBatchAndHiddenMetadataAtOneBoundary) {
  MutableHead target = head(4U, 2U);
  const auto input = batch();
  PreparedHeadAppend prepared = prepare(target, input, 7U);
  const HeadSnapshot published = publish(prepared);
  EXPECT_FALSE(prepared.is_valid());
  ASSERT_TRUE(published.applied_position().has_value());
  EXPECT_EQ(*published.applied_position(), position(7U));
  EXPECT_EQ(published.row_count(), 2U);

  const HeadColumnView timestamps = published.column(0U).value();
  EXPECT_TRUE(timestamps.validity().empty());
  EXPECT_EQ(timestamps.fixed_values().size(), 16U);
  EXPECT_TRUE(std::ranges::all_of(timestamps.fixed_values(),
                                  [](const std::byte value) { return value == std::byte{0U}; }));

  const HeadColumnView strings = published.column(1U).value();
  ASSERT_EQ(strings.validity().size(), 2U);
  EXPECT_EQ(strings.validity()[0], 1U);
  EXPECT_EQ(strings.validity()[1], 0U);
  ASSERT_EQ(strings.variable_offsets().size(), 3U);
  EXPECT_EQ(strings.variable_offsets()[0], 0U);
  EXPECT_EQ(strings.variable_offsets()[1], 1U);
  EXPECT_EQ(strings.variable_offsets()[2], 1U);
  ASSERT_EQ(strings.variable_values().size(), 1U);
  EXPECT_EQ(strings.variable_values()[0], std::byte{'x'});
  EXPECT_EQ(published.cell(1U, 0U)->bytes()->front(), std::byte{'x'});
  EXPECT_TRUE(published.cell(1U, 1U)->is_null());

  const HeadColumnView booleans = published.column(2U).value();
  ASSERT_EQ(booleans.boolean_values().size(), 2U);
  EXPECT_EQ(booleans.boolean_values()[0], 1U);
  EXPECT_EQ(booleans.boolean_values()[1], 0U);
  EXPECT_TRUE(published.cell(2U, 0U)->boolean().value());
  EXPECT_FALSE(published.cell(2U, 1U)->boolean().value());

  EXPECT_EQ(published.row_metadata(0U).value(),
            (HeadRowMetadata{.commit_position = position(7U),
                             .row_ordinal = 0U,
                             .operation = HeadOperationKind::kAppendRows}));
  EXPECT_EQ(published.row_metadata(1U)->row_ordinal, 1U);
  EXPECT_EQ(published.row_version_identity(1U).value(),
            (RowVersionIdentity{.table_id = input->schema().table_id(),
                                .tablet_id = tablet_id(),
                                .wal_id = wal_id(),
                                .record_sequence = 7U,
                                .row_ordinal = 1U}));

  const MutableHeadMetrics metrics = target.metrics();
  EXPECT_EQ(metrics.published_rows, 2U);
  EXPECT_EQ(metrics.published_variable_bytes, 1U);
  EXPECT_FALSE(metrics.failed);
}

TEST(MutableHeadTest, OldSnapshotsKeepExactBoundariesAndStableStorageAcrossLaterAppends) {
  MutableHead target = head(4U, 2U);
  const auto input = batch();
  PreparedHeadAppend first_prepared = prepare(target, input, 1U);
  const HeadSnapshot first = publish(first_prepared);
  const HeadColumnView first_timestamps = first.column(0U).value();
  const HeadColumnView first_strings = first.column(1U).value();
  const std::byte* const fixed_address = first_timestamps.fixed_values().data();
  const std::byte* const variable_address = first_strings.variable_values().data();

  PreparedHeadAppend second_prepared = prepare(target, input, 3U);
  EXPECT_EQ(first.row_count(), 2U);
  EXPECT_EQ(first_strings.variable_offsets().back(), 1U);
  const HeadSnapshot second = publish(second_prepared);

  EXPECT_EQ(first.row_count(), 2U);
  EXPECT_EQ(first.applied_position()->record_sequence, 1U);
  EXPECT_EQ(first.column(0U)->fixed_values().data(), fixed_address);
  EXPECT_EQ(first.column(1U)->variable_values().data(), variable_address);
  EXPECT_EQ(first.column(1U)->variable_offsets().size(), 3U);
  EXPECT_EQ(first.column(1U)->variable_offsets().back(), 1U);

  EXPECT_EQ(second.row_count(), 4U);
  EXPECT_EQ(second.applied_position()->record_sequence, 3U);
  EXPECT_EQ(second.column(0U)->fixed_values().data(), fixed_address);
  EXPECT_EQ(second.column(1U)->variable_values().data(), variable_address);
  EXPECT_EQ(second.column(1U)->variable_offsets().size(), 5U);
  EXPECT_EQ(second.column(1U)->variable_offsets()[2], 1U);
  EXPECT_EQ(second.column(1U)->variable_offsets()[3], 2U);
  EXPECT_EQ(second.column(1U)->variable_offsets()[4], 2U);
  EXPECT_EQ(second.row_metadata(2U)->row_ordinal, 0U);
  EXPECT_EQ(second.row_metadata(3U)->row_ordinal, 1U);

  EXPECT_EQ(target.prepare_append(input, position(3U)).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(target.prepare_append(input, position(4U, 2U)).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(MutableHeadTest, RejectsSchemaRowAndVariableCapacityBeforeWal) {
  const auto input = batch();
  MutableHead row_limited = head(3U, 8U);
  PreparedHeadAppend first = prepare(row_limited, input, 1U);
  static_cast<void>(publish(first));
  EXPECT_EQ(row_limited.prepare_append(input, position(2U)).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(row_limited.snapshot()->row_count(), 2U);

  MutableHead byte_limited = head(4U, 1U);
  PreparedHeadAppend byte_first = prepare(byte_limited, input, 1U);
  static_cast<void>(publish(byte_first));
  EXPECT_EQ(byte_limited.prepare_append(input, position(2U)).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(byte_limited.snapshot()->row_count(), 2U);

  const auto other_schema = std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(columnar::test::id<schema::TableId>(150U),
                                  columnar::test::id<schema::SchemaId>(151U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::vector<schema::ColumnDefinition>{
                                      schema::ColumnDefinition::create(
                                          columnar::test::id<schema::ColumnId>(152U), "ts",
                                          columnar::test::type(
                                              schema::LogicalTypeKind::kTimestampNs),
                                          false)
                                          .value()},
                                  schema::TableSchemaRoles{
                                      .event_time_column =
                                          columnar::test::id<schema::ColumnId>(152U),
                                      .physical_ordering_key = {
                                          columnar::test::id<schema::ColumnId>(152U)},
                                      .partition_columns = {
                                          columnar::test::id<schema::ColumnId>(152U)},
                                      .shard_key = {
                                          columnar::test::id<schema::ColumnId>(152U)},
                                      .deduplication_key = {}})
          .value());
  std::vector<columnar::OwnedColumnVector> other_columns;
  other_columns.push_back(columnar::test::fixed_vector(
      152U, columnar::test::type(schema::LogicalTypeKind::kTimestampNs), false, 1U, {}, 0U,
      std::vector<std::byte>(8U)));
  const auto other_batch = std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(other_schema, std::move(other_columns)).value());
  MutableHead target = head();
  EXPECT_EQ(target.prepare_append(other_batch, position(1U)).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(target.snapshot()->row_count(), 0U);
}

TEST(MutableHeadTest, DroppedPostWalAppendFailsClosedWithoutPublishingPartialRows) {
  MutableHead target = head();
  const auto input = batch();
  {
    PreparedHeadAppend prepared = prepare(target, input, 1U);
    EXPECT_TRUE(prepared.mark_wal_started().is_ok());
    EXPECT_TRUE(prepared.wal_started());
    EXPECT_EQ(prepared.cancel_before_wal().code(), common::StatusCode::kInvalidArgument);
  }

  EXPECT_TRUE(target.metrics().failed);
  EXPECT_EQ(target.metrics().published_rows, 0U);
  EXPECT_EQ(target.snapshot()->row_count(), 0U);
  EXPECT_EQ(target.prepare_append(input, position(2U)).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(target.seal().error().code(), common::StatusCode::kUnavailable);
}

TEST(MutableHeadTest, SealingIsIdempotentPinsStorageAndRejectsNewAppends) {
  std::optional<HeadSnapshot> sealed;
  const auto input = batch();
  {
    MutableHead target = head();
    PreparedHeadAppend prepared = prepare(target, input, 1U);
    static_cast<void>(publish(prepared));
    sealed.emplace(target.seal().value());
    EXPECT_TRUE(target.metrics().sealed);
    EXPECT_EQ(target.seal()->row_count(), 2U);
    EXPECT_EQ(target.prepare_append(input, position(2U)).error().code(),
              common::StatusCode::kUnavailable);
  }

  ASSERT_TRUE(sealed.has_value());
  EXPECT_EQ(sealed->row_count(), 2U);
  EXPECT_EQ(sealed->cell(1U, 0U)->bytes()->front(), std::byte{'x'});
}

[[nodiscard]] std::size_t property_fixed_width(const schema::LogicalTypeKind kind) {
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kUInt8:
    return 1U;
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kUInt16:
    return 2U;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kDate:
    return 4U;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kTimestampNs:
    return 8U;
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kUuid:
    return 16U;
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return 0U;
  }
  return 0U;
}

TEST(MutableHeadPropertyTest, EveryFrozenLogicalTypeAndNullableShapeMaterializesExactly) {
  constexpr std::uint32_t kRows = 3U;
  std::vector<schema::ColumnDefinition> definitions;
  std::vector<columnar::OwnedColumnVector> vectors;
  std::vector<std::size_t> variable_capacities;
  for (std::uint16_t code = 1U; code <= 18U; ++code) {
    const schema::LogicalTypeKind kind = schema::logical_type_kind_from_code(code).value();
    const schema::LogicalType logical_type = kind == schema::LogicalTypeKind::kDecimal
                                                 ? schema::LogicalType::decimal(38U, 9U).value()
                                                 : schema::LogicalType::create(kind).value();
    const bool nullable = kind != schema::LogicalTypeKind::kTimestampNs;
    definitions.push_back(schema::ColumnDefinition::create(
                              columnar::test::id<schema::ColumnId>(code),
                              std::string{"c"} + std::to_string(code), logical_type, nullable)
                              .value());
    variable_capacities.push_back(logical_type.is_variable_width() ? 4U : 0U);

    std::vector<std::byte> validity = nullable ? std::vector<std::byte>{std::byte{0x05}}
                                               : std::vector<std::byte>{};
    const std::uint32_t null_count = nullable ? 1U : 0U;
    if (logical_type.is_variable_width()) {
      std::vector<std::byte> offsets;
      columnar::test::append_u32(offsets, 0U);
      columnar::test::append_u32(offsets, 1U);
      columnar::test::append_u32(offsets, 1U);
      columnar::test::append_u32(offsets, 2U);
      vectors.push_back(
          columnar::OwnedColumnVector::create(
              columnar::ColumnVectorMetadata{
                  .column_id = columnar::test::id<schema::ColumnId>(code),
                  .type = logical_type,
                  .nullable = nullable,
                  .row_count = kRows,
                  .null_count = null_count},
              columnar::ColumnVectorBuffers{.validity = std::move(validity),
                                            .offsets = std::move(offsets),
                                            .values = {std::byte{'a'}, std::byte{'b'}}})
              .value());
    } else {
      const std::size_t width = kind == schema::LogicalTypeKind::kBool
                                    ? columnar::bitmap_size(kRows)
                                    : property_fixed_width(kind) * kRows;
      std::vector<std::byte> values(width);
      if (kind == schema::LogicalTypeKind::kBool) {
        values[0] = std::byte{0x05};
      }
      vectors.push_back(columnar::test::fixed_vector(code, logical_type, nullable, kRows,
                                                      std::move(validity), null_count,
                                                      std::move(values)));
    }
  }

  const schema::ColumnId event_time = columnar::test::id<schema::ColumnId>(13U);
  const auto schema = std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(
          columnar::test::id<schema::TableId>(200U),
          columnar::test::id<schema::SchemaId>(201U), schema::SchemaVersion::initial(),
          std::nullopt, std::move(definitions),
          schema::TableSchemaRoles{.event_time_column = event_time,
                                   .physical_ordering_key = {event_time},
                                   .partition_columns = {event_time},
                                   .shard_key = {event_time},
                                   .deduplication_key = {}})
          .value());
  const auto input = std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(schema, std::move(vectors)).value());
  MutableHead target =
      MutableHead::create(schema, tablet_id(), 1U,
                          MutableHeadCapacity{.row_capacity = kRows,
                                              .variable_value_bytes = variable_capacities})
          .value();
  PreparedHeadAppend prepared = prepare(target, input, 1U);
  const HeadSnapshot snapshot = publish(prepared);

  for (std::size_t ordinal = 0U; ordinal < input->columns().size(); ++ordinal) {
    SCOPED_TRACE(ordinal);
    const columnar::OwnedColumnVector& source = input->columns()[ordinal];
    const HeadColumnView materialized = snapshot.column(ordinal).value();
    EXPECT_EQ(materialized.validity().size(), source.nullable() ? kRows : 0U);
    for (std::uint32_t row = 0U; row < kRows; ++row) {
      const columnar::ColumnCellView expected = source.cell(row).value();
      const HeadCellView actual = materialized.cell(row).value();
      EXPECT_EQ(actual.is_null(), expected.is_null());
      if (expected.is_null()) {
        continue;
      }
      if (source.type().kind() == schema::LogicalTypeKind::kBool) {
        EXPECT_EQ(actual.boolean().value(), expected.boolean().value());
      } else {
        EXPECT_TRUE(std::ranges::equal(actual.bytes().value(), expected.bytes().value()));
      }
    }
  }
}

TEST(MutableHeadConcurrencyTest, AcquireSnapshotsObserveOnlyCompleteBatchBoundaries) {
  constexpr std::size_t kReaders = 4U;
  constexpr std::uint64_t kBatches = 64U;
  MutableHead target = head(static_cast<std::uint32_t>(kBatches * 2U), kBatches);
  const auto input = batch();
  std::latch start{static_cast<std::ptrdiff_t>(kReaders + 1U)};
  std::atomic<bool> done{false};
  std::atomic<std::size_t> failures{0U};
  std::atomic<std::size_t> observations{0U};
  std::vector<std::jthread> readers;
  readers.reserve(kReaders);
  for (std::size_t index = 0U; index < kReaders; ++index) {
    static_cast<void>(index);
    readers.emplace_back([&] {
      start.arrive_and_wait();
      while (!done.load(std::memory_order_acquire)) {
        const auto observed = target.snapshot();
        if (!observed.has_value() || observed->row_count() % 2U != 0U ||
            observed->row_count() > kBatches * 2U) {
          ++failures;
          continue;
        }
        ++observations;
        if (observed->row_count() == 0U) {
          continue;
        }
        const std::uint32_t final_row = observed->row_count() - 1U;
        const auto metadata = observed->row_metadata(final_row);
        const auto final_boolean = observed->cell(2U, final_row);
        const auto first_boolean = observed->cell(2U, final_row - 1U);
        if (!metadata.has_value() || metadata->row_ordinal != 1U ||
            metadata->commit_position.record_sequence != observed->row_count() / 2U ||
            !final_boolean.has_value() || !first_boolean.has_value() ||
            final_boolean->boolean().value_or(true) ||
            !first_boolean->boolean().value_or(false)) {
          ++failures;
        }
      }
    });
  }

  start.arrive_and_wait();
  for (std::uint64_t sequence = 1U; sequence <= kBatches; ++sequence) {
    PreparedHeadAppend prepared = prepare(target, input, sequence);
    static_cast<void>(publish(prepared));
  }
  done.store(true, std::memory_order_release);
  readers.clear();

  EXPECT_EQ(failures.load(), 0U);
  EXPECT_GT(observations.load(), 0U);
  EXPECT_EQ(target.snapshot()->row_count(), kBatches * 2U);
}

} // namespace
} // namespace chronos::head
