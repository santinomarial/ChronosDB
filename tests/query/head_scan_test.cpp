#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/head/mutable_head.hpp"
#include "chronos/query/head_scan.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "columnar/columnar_test_support.hpp"
#include "query/head_scan_test_fixture.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] std::uint32_t u32_at(const common::ByteView bytes, const std::size_t index) {
  common::ByteReader reader{bytes.subspan(index * sizeof(std::uint32_t))};
  return reader.read_u32_le().value();
}

[[nodiscard]] std::int64_t i64_cell(const VectorChunk& chunk, const std::size_t column,
                                    const std::size_t row) {
  const common::ByteView bytes =
      chunk.cell({.column_ordinal = column, .selected_row = row})->bytes().value();
  common::ByteReader reader{bytes};
  return reader.read_i64_le().value();
}

TEST(HeadScanOperatorTest, MaterializesCanonicalChunksAndSynthesizesNullableTailInCallerOrder) {
  test::HeadFixture fixture{6U};
  fixture.publish({.range = {.first_row = 0U, .row_count = 6U}, .record_sequence = 7U});
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{64U} * 1024U * 1024U).value();
  HeadScanLimits limits;
  limits.chunk.maximum_rows = 3U;
  auto source = HeadScanOperator::create(
      resources, fixture.snapshot(), fixture.schemas(),
      columnar::test::id<schema::SchemaId>(test::kSuccessorSchemaId),
      columnar::test::id<schema::TabletId>(test::kTabletId), {4U, 1U, 2U, 3U, 0U}, limits);
  ASSERT_TRUE(source.has_value()) << source.error().to_string();

  {
    common::Result<PhysicalOperatorStep> step = (*source)->next(resources);
    ASSERT_TRUE(step.has_value()) << step.error().to_string();
    ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
    const VectorChunk& chunk = step->chunk()->chunk();
    ASSERT_EQ(chunk.physical_row_count(), 3U);
    ASSERT_EQ(chunk.column_count(), 5U);
    for (std::size_t row = 0U; row < 3U; ++row)
      EXPECT_TRUE(chunk.cell({.column_ordinal = 0U, .selected_row = row})->is_null());

    const columnar::PhysicalColumnView* labels = chunk.column(1U);
    ASSERT_NE(labels, nullptr);
    ASSERT_EQ(labels->validity().size(), 1U);
    EXPECT_EQ(labels->validity()[0], std::byte{0x05});
    EXPECT_EQ(u32_at(labels->offsets(), 0U), 0U);
    EXPECT_EQ(u32_at(labels->offsets(), 1U), 1U);
    EXPECT_EQ(u32_at(labels->offsets(), 2U), 1U);
    EXPECT_EQ(u32_at(labels->offsets(), 3U), 2U);
    EXPECT_TRUE(std::ranges::equal(labels->values(), std::array{std::byte{'a'}, std::byte{'c'}}));

    const columnar::PhysicalColumnView* enabled = chunk.column(2U);
    ASSERT_NE(enabled, nullptr);
    EXPECT_EQ(enabled->validity()[0], std::byte{0x05});
    EXPECT_EQ(enabled->values()[0], std::byte{0x01});
    const columnar::PhysicalColumnView* reading = chunk.column(3U);
    ASSERT_NE(reading, nullptr);
    EXPECT_EQ(reading->validity()[0], std::byte{0x05});
    EXPECT_EQ(i64_cell(chunk, 3U, 0U), -7);
    EXPECT_TRUE(chunk.cell({.column_ordinal = 3U, .selected_row = 1U})->is_null());
    EXPECT_EQ(i64_cell(chunk, 3U, 2U), 193);
    EXPECT_EQ(i64_cell(chunk, 4U, 0U), 0);
    EXPECT_EQ(i64_cell(chunk, 4U, 2U), 20);
  }

  common::Result<PhysicalOperatorStep> final = (*source)->next(resources);
  ASSERT_TRUE(final.has_value()) << final.error().to_string();
  ASSERT_EQ(final->kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& chunk = final->chunk()->chunk();
  ASSERT_EQ(chunk.physical_row_count(), 3U);
  const columnar::PhysicalColumnView* labels = chunk.column(1U);
  ASSERT_NE(labels, nullptr);
  EXPECT_EQ(labels->validity()[0], std::byte{0x02});
  EXPECT_EQ(u32_at(labels->offsets(), 0U), 0U);
  EXPECT_EQ(u32_at(labels->offsets(), 1U), 0U);
  EXPECT_EQ(u32_at(labels->offsets(), 2U), 1U);
  EXPECT_EQ(u32_at(labels->offsets(), 3U), 1U);
  ASSERT_EQ(labels->values().size(), 1U);
  EXPECT_EQ(labels->values()[0], std::byte{'e'});
  EXPECT_EQ(chunk.column(2U)->validity()[0], std::byte{0x03});
  EXPECT_EQ(chunk.column(2U)->values()[0], std::byte{0x01});
  EXPECT_EQ(resources.reserved_memory_bytes(), final->chunk()->charged_memory_bytes());
  final = common::make_unexpected(common::Status{common::StatusCode::kInternal, "drop chunk"});
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_EQ((*source)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ((*source)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(HeadScanOperatorTest, SupportsEmptyHeadAndZeroColumnCardinality) {
  test::HeadFixture empty_fixture{1U};
  QueryResourceContext empty_resources =
      QueryResourceContext::create(std::size_t{1024U} * 1024U).value();
  auto empty =
      HeadScanOperator::create(empty_resources, empty_fixture.snapshot(), empty_fixture.schemas(),
                               columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                               columnar::test::id<schema::TabletId>(test::kTabletId), {});
  ASSERT_TRUE(empty.has_value()) << empty.error().to_string();
  EXPECT_GT(empty_resources.reserved_memory_bytes(), 0U);
  EXPECT_EQ((*empty)->next(empty_resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(empty_resources.reserved_memory_bytes(), 0U);

  test::HeadFixture rows_fixture{5U};
  rows_fixture.publish({.range = {.first_row = 0U, .row_count = 5U}, .record_sequence = 2U});
  QueryResourceContext rows_resources =
      QueryResourceContext::create(std::size_t{1024U} * 1024U).value();
  HeadScanLimits limits;
  limits.chunk.maximum_rows = 2U;
  auto rows =
      HeadScanOperator::create(rows_resources, rows_fixture.snapshot(), rows_fixture.schemas(),
                               columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                               columnar::test::id<schema::TabletId>(test::kTabletId), {}, limits);
  ASSERT_TRUE(rows.has_value()) << rows.error().to_string();
  const std::array<std::size_t, 3> expected{2U, 2U, 1U};
  for (const std::size_t count : expected) {
    const auto step = (*rows)->next(rows_resources);
    ASSERT_TRUE(step.has_value()) << step.error().to_string();
    ASSERT_NE(step->chunk(), nullptr);
    EXPECT_EQ(step->chunk()->chunk().column_count(), 0U);
    EXPECT_EQ(step->chunk()->chunk().selected_row_count(), count);
  }
  EXPECT_EQ((*rows)->next(rows_resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(rows_resources.reserved_memory_bytes(), 0U);
}

TEST(HeadScanOperatorTest, PinsTheExactOldPublicationAcrossLaterAppends) {
  test::HeadFixture fixture{4U};
  fixture.publish({.range = {.first_row = 0U, .row_count = 2U}, .record_sequence = 1U});
  const head::HeadSnapshot old = fixture.snapshot();
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  auto source =
      HeadScanOperator::create(resources, old, fixture.schemas(),
                               columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                               columnar::test::id<schema::TabletId>(test::kTabletId), {0U});
  ASSERT_TRUE(source.has_value()) << source.error().to_string();
  fixture.publish({.range = {.first_row = 2U, .row_count = 2U}, .record_sequence = 3U});
  ASSERT_EQ(fixture.snapshot().row_count(), 4U);

  const auto step = (*source)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_NE(step->chunk(), nullptr);
  EXPECT_EQ(step->chunk()->chunk().selected_row_count(), 2U);
  EXPECT_EQ(i64_cell(step->chunk()->chunk(), 0U, 0U), 0);
  EXPECT_EQ(i64_cell(step->chunk()->chunk(), 0U, 1U), 10);
  EXPECT_EQ((*source)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(HeadScanOperatorTest, RejectsInvalidIdentityProjectionLimitsAndQueryOwnership) {
  test::HeadFixture fixture{2U};
  fixture.publish({.range = {.first_row = 0U, .row_count = 2U}, .record_sequence = 1U});
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  EXPECT_EQ(HeadScanOperator::create(resources, fixture.snapshot(), fixture.schemas(),
                                     columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                                     columnar::test::id<schema::TabletId>(999U), {0U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(HeadScanOperator::create(resources, fixture.snapshot(), fixture.schemas(),
                                     columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                                     columnar::test::id<schema::TabletId>(test::kTabletId),
                                     {0U, 0U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(HeadScanOperator::create(resources, fixture.snapshot(), fixture.schemas(),
                                     columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                                     columnar::test::id<schema::TabletId>(test::kTabletId), {9U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  HeadScanLimits zero;
  zero.chunk.maximum_rows = 0U;
  EXPECT_EQ(HeadScanOperator::create(resources, fixture.snapshot(), fixture.schemas(),
                                     columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                                     columnar::test::id<schema::TabletId>(test::kTabletId), {0U},
                                     zero)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  schema::SchemaLineage foreign =
      schema::SchemaLineage::create(*columnar::test::batch_schema()).value();
  EXPECT_EQ(HeadScanOperator::create(resources, fixture.snapshot(), foreign,
                                     columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                                     columnar::test::id<schema::TabletId>(test::kTabletId), {0U})
                .error()
                .code(),
            common::StatusCode::kNotFound);

  auto source =
      HeadScanOperator::create(resources, fixture.snapshot(), fixture.schemas(),
                               columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                               columnar::test::id<schema::TabletId>(test::kTabletId), {0U});
  ASSERT_TRUE(source.has_value()) << source.error().to_string();
  QueryResourceContext foreign_resources =
      QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  const auto wrong_query = (*source)->next(foreign_resources);
  ASSERT_FALSE(wrong_query.has_value());
  EXPECT_EQ(wrong_query.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(foreign_resources.is_cancelled());
  EXPECT_GT(resources.reserved_memory_bytes(), 0U);
  source->reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(HeadScanOperatorTest, PreCancelledPullRetainsItsExactSourceUntilDestruction) {
  test::HeadFixture fixture{2U};
  fixture.publish({.range = {.first_row = 0U, .row_count = 2U}, .record_sequence = 1U});
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  auto source =
      HeadScanOperator::create(resources, fixture.snapshot(), fixture.schemas(),
                               columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
                               columnar::test::id<schema::TabletId>(test::kTabletId), {0U});
  ASSERT_TRUE(source.has_value()) << source.error().to_string();
  const std::size_t source_charge = resources.reserved_memory_bytes();

  EXPECT_TRUE(resources.request_cancel());
  const auto failed = (*source)->next(resources);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(resources.reserved_memory_bytes(), source_charge);
  source->reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(HeadScanOperatorTest, EnforcesLogicalAndRetainedChunkLimitsBeforeMaterialization) {
  for (const bool constrain_retained : {false, true}) {
    SCOPED_TRACE(constrain_retained);
    test::HeadFixture fixture{4U};
    fixture.publish({.range = {.first_row = 0U, .row_count = 4U}, .record_sequence = 1U});
    QueryResourceContext resources =
        QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
    HeadScanLimits limits;
    if (constrain_retained)
      limits.chunk.maximum_retained_buffer_bytes = 1U;
    else
      limits.chunk.maximum_buffer_bytes = 1U;
    auto source = HeadScanOperator::create(
        resources, fixture.snapshot(), fixture.schemas(),
        columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
        columnar::test::id<schema::TabletId>(test::kTabletId), {0U}, limits);
    ASSERT_TRUE(source.has_value()) << source.error().to_string();
    const std::size_t source_charge = resources.reserved_memory_bytes();

    const auto failed = (*source)->next(resources);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), source_charge);
  }
}

TEST(HeadScanOperatorTest, ReservesBeforeOutputAndUnwindsBudgetFailure) {
  test::HeadFixture fixture{8U};
  fixture.publish({.range = {.first_row = 0U, .row_count = 8U}, .record_sequence = 1U});
  HeadScanLimits limits;
  limits.chunk.maximum_rows = 8U;
  QueryResourceContext too_small = QueryResourceContext::create(1U).value();
  const auto rejected = HeadScanOperator::create(
      too_small, fixture.snapshot(), fixture.schemas(),
      columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
      columnar::test::id<schema::TabletId>(test::kTabletId), {0U, 1U, 2U, 3U}, limits);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(too_small.reserved_memory_bytes(), 0U);

  QueryResourceContext ample =
      QueryResourceContext::create(std::size_t{64U} * 1024U * 1024U).value();
  auto measured = HeadScanOperator::create(
      ample, fixture.snapshot(), fixture.schemas(),
      columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
      columnar::test::id<schema::TabletId>(test::kTabletId), {0U, 1U, 2U, 3U}, limits);
  ASSERT_TRUE(measured.has_value()) << measured.error().to_string();
  const auto step = (*measured)->next(ample);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  const std::size_t required = ample.peak_reserved_memory_bytes();
  ASSERT_GT(required, 1U);
  measured->reset();

  QueryResourceContext constrained = QueryResourceContext::create(required - 1U).value();
  auto source = HeadScanOperator::create(
      constrained, fixture.snapshot(), fixture.schemas(),
      columnar::test::id<schema::SchemaId>(test::kInitialSchemaId),
      columnar::test::id<schema::TabletId>(test::kTabletId), {0U, 1U, 2U, 3U}, limits);
  ASSERT_TRUE(source.has_value()) << source.error().to_string();
  const std::size_t source_charge = constrained.reserved_memory_bytes();
  const auto failed = (*source)->next(constrained);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(constrained.is_cancelled());
  EXPECT_EQ(constrained.reserved_memory_bytes(), source_charge);
  source->reset();
  EXPECT_EQ(constrained.reserved_memory_bytes(), 0U);
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

TEST(HeadScanOperatorPropertyTest, EveryFrozenTypeMatchesItsPublishedHeadCellsAcrossChunks) {
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
    std::vector<std::byte> validity =
        nullable ? std::vector<std::byte>{std::byte{0x05}} : std::vector<std::byte>{};
    if (logical_type.is_variable_width()) {
      std::vector<std::byte> offsets;
      for (const std::uint32_t offset : {0U, 1U, 1U, 2U})
        columnar::test::append_u32(offsets, offset);
      vectors.push_back(columnar::OwnedColumnVector::create(
                            {.column_id = columnar::test::id<schema::ColumnId>(code),
                             .type = logical_type,
                             .nullable = nullable,
                             .row_count = kRows,
                             .null_count = nullable ? 1U : 0U},
                            {.validity = std::move(validity),
                             .offsets = std::move(offsets),
                             .values = {std::byte{'a'}, std::byte{'b'}}})
                            .value());
    } else {
      const std::size_t bytes = kind == schema::LogicalTypeKind::kBool
                                    ? columnar::bitmap_size(kRows)
                                    : property_fixed_width(kind) * kRows;
      std::vector<std::byte> values(bytes);
      if (kind == schema::LogicalTypeKind::kBool)
        values[0] = std::byte{0x05};
      vectors.push_back(columnar::test::fixed_vector(code, logical_type, nullable, kRows,
                                                     std::move(validity), nullable ? 1U : 0U,
                                                     std::move(values)));
    }
  }
  const schema::ColumnId event = columnar::test::id<schema::ColumnId>(13U);
  auto table = std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(
          columnar::test::id<schema::TableId>(700U), columnar::test::id<schema::SchemaId>(701U),
          schema::SchemaVersion::initial(), std::nullopt, std::move(definitions),
          {.event_time_column = event,
           .physical_ordering_key = {event},
           .partition_columns = {event},
           .shard_key = {event},
           .deduplication_key = {}})
          .value());
  auto input = std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(table, std::move(vectors)).value());
  schema::SchemaLineage schemas = schema::SchemaLineage::create(*table).value();
  head::MutableHead head =
      head::MutableHead::create(
          schemas.current(), columnar::test::id<schema::TabletId>(702U), 1U,
          {.row_capacity = kRows, .variable_value_bytes = std::move(variable_capacities)})
          .value();
  auto prepared = head.prepare_append(input).value();
  ASSERT_TRUE(prepared.mark_wal_started().is_ok());
  head::HeadSnapshot snapshot =
      prepared.publish({.wal_id = test::wal_id(), .record_sequence = 1U}).value();
  std::vector<std::uint32_t> ordinals(18U);
  for (std::uint32_t ordinal = 0U; ordinal < ordinals.size(); ++ordinal)
    ordinals[ordinal] = ordinal;
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{256U} * 1024U * 1024U).value();
  HeadScanLimits limits;
  limits.chunk.maximum_rows = 2U;
  auto source = HeadScanOperator::create(resources, snapshot, schemas, table->schema_id(),
                                         snapshot.tablet_id(), ordinals, limits);
  ASSERT_TRUE(source.has_value()) << source.error().to_string();

  std::uint32_t global_row = 0U;
  for (;;) {
    common::Result<PhysicalOperatorStep> step = (*source)->next(resources);
    ASSERT_TRUE(step.has_value()) << step.error().to_string();
    if (step->kind() == PhysicalOperatorStepKind::kEnd)
      break;
    const VectorChunk& chunk = step->chunk()->chunk();
    for (std::size_t local = 0U; local < chunk.selected_row_count(); ++local, ++global_row) {
      for (std::size_t column = 0U; column < input->columns().size(); ++column) {
        const columnar::ColumnCellView expected = input->columns()[column].cell(global_row).value();
        const columnar::ColumnCellView actual =
            chunk.cell({.column_ordinal = column, .selected_row = local}).value();
        ASSERT_EQ(actual.kind(), expected.kind());
        if (actual.kind() == columnar::ColumnCellView::Kind::kBoolean) {
          EXPECT_EQ(actual.boolean().value(), expected.boolean().value());
        } else if (actual.kind() == columnar::ColumnCellView::Kind::kBytes) {
          EXPECT_TRUE(std::ranges::equal(actual.bytes().value(), expected.bytes().value()));
        }
      }
    }
  }
  EXPECT_EQ(global_row, kRows);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(HeadScanOperatorPropertyTest, DeterministicChunkBoundariesPreserveEveryPublishedCell) {
  constexpr std::array<std::uint32_t, 4> projected_source_ordinals{3U, 1U, 2U, 0U};
  for (const std::uint32_t rows : {1U, 2U, 3U, 7U, 17U, 31U}) {
    for (const std::uint32_t chunk_rows : {1U, 2U, 5U, 32U}) {
      SCOPED_TRACE(rows);
      SCOPED_TRACE(chunk_rows);
      test::HeadFixture fixture{rows};
      fixture.publish({.range = {.first_row = 0U, .row_count = rows}, .record_sequence = 1U});
      const head::HeadSnapshot snapshot = fixture.snapshot();
      QueryResourceContext resources =
          QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
      HeadScanLimits limits;
      limits.chunk.maximum_rows = chunk_rows;
      auto source = HeadScanOperator::create(
          resources, snapshot, fixture.schemas(),
          columnar::test::id<schema::SchemaId>(test::kInitialSchemaId), snapshot.tablet_id(),
          std::vector<std::uint32_t>(projected_source_ordinals.begin(),
                                     projected_source_ordinals.end()),
          limits);
      ASSERT_TRUE(source.has_value()) << source.error().to_string();

      std::uint32_t global_row = 0U;
      for (;;) {
        common::Result<PhysicalOperatorStep> step = (*source)->next(resources);
        ASSERT_TRUE(step.has_value()) << step.error().to_string();
        if (step->kind() == PhysicalOperatorStepKind::kEnd)
          break;
        const VectorChunk& chunk = step->chunk()->chunk();
        for (std::size_t local = 0U; local < chunk.selected_row_count(); ++local, ++global_row) {
          for (std::size_t output = 0U; output < projected_source_ordinals.size(); ++output) {
            const head::HeadCellView expected =
                snapshot
                    .cell({.column_ordinal = projected_source_ordinals[output], .row = global_row})
                    .value();
            const columnar::ColumnCellView actual =
                chunk.cell({.column_ordinal = output, .selected_row = local}).value();
            EXPECT_EQ(actual.is_null(), expected.is_null());
            if (actual.is_null())
              continue;
            if (actual.kind() == columnar::ColumnCellView::Kind::kBoolean) {
              EXPECT_EQ(actual.boolean().value(), expected.boolean().value());
            } else {
              EXPECT_TRUE(std::ranges::equal(actual.bytes().value(), expected.bytes().value()));
            }
          }
        }
      }
      EXPECT_EQ(global_row, rows);
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    }
  }
}

} // namespace
} // namespace chronos::query
