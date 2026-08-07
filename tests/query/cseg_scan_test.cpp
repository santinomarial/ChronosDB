#include "chronos/common/byte_reader.hpp"
#include "chronos/cseg/metadata_codec.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/query/cseg_scan.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

struct PartOwner {
  explicit PartOwner(cseg::EncodedCsegPart part_value,
                     std::shared_ptr<bool> destroyed_value = {}) noexcept
      : part(std::move(part_value)), destroyed(std::move(destroyed_value)) {}

  ~PartOwner() {
    if (destroyed != nullptr)
      *destroyed = true;
  }

  cseg::EncodedCsegPart part;
  std::shared_ptr<bool> destroyed;
};

[[nodiscard]] schema::SchemaLineage valid_lineage() {
  const schema::ColumnId event_time = cseg::test::identifier<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(
      schema::ColumnDefinition::create(
          event_time, "event_time", cseg::test::type(schema::LogicalTypeKind::kTimestampNs), false)
          .value());
  schema::TableSchema table =
      schema::TableSchema::create(
          cseg::test::identifier<schema::TableId>(2U), cseg::test::identifier<schema::SchemaId>(4U),
          schema::SchemaVersion::initial(), std::nullopt, std::move(columns),
          {.event_time_column = event_time,
           .physical_ordering_key = {event_time},
           .partition_columns = {event_time},
           .shard_key = {event_time},
           .deduplication_key = {}})
          .value();
  return schema::SchemaLineage::create(std::move(table)).value();
}

[[nodiscard]] schema::SchemaLineage evolved_lineage() {
  schema::SchemaLineage lineage = valid_lineage();
  const std::shared_ptr<const schema::TableSchema> source = lineage.current();
  const schema::ColumnId added = cseg::test::identifier<schema::ColumnId>(7U);
  std::vector<schema::ColumnDefinition> columns{source->columns().begin(), source->columns().end()};
  columns.push_back(schema::ColumnDefinition::create(
                        added, "added", cseg::test::type(schema::LogicalTypeKind::kString), true)
                        .value());
  schema::TableSchema successor =
      schema::TableSchema::create(source->table_id(), cseg::test::identifier<schema::SchemaId>(6U),
                                  schema::SchemaVersion::from_value(2U).value(),
                                  source->schema_id(), std::move(columns),
                                  {.event_time_column = source->event_time_column(),
                                   .physical_ordering_key = {source->event_time_column()},
                                   .partition_columns = {source->event_time_column()},
                                   .shard_key = {source->event_time_column()},
                                   .deduplication_key = {}})
          .value();
  EXPECT_TRUE(lineage.append(std::move(successor)).is_ok());
  return lineage;
}

[[nodiscard]] CsegPartPin pin(cseg::EncodedCsegPart encoded,
                              const std::shared_ptr<bool>& destroyed = {}) {
  auto owner = std::make_shared<const PartOwner>(std::move(encoded), destroyed);
  const std::size_t retained = owner->part.retained_buffer_bytes() + sizeof(PartOwner) + 64U;
  return CsegPartPin::create(owner, owner->part.bytes(), retained).value();
}

[[nodiscard]] std::int64_t int64_cell(const VectorChunk& chunk, const std::size_t selected_row,
                                      const std::size_t column_ordinal = 0U) {
  const common::Result<columnar::ColumnCellView> cell =
      chunk.cell({.column_ordinal = column_ordinal, .selected_row = selected_row});
  EXPECT_TRUE(cell.has_value());
  const common::Result<common::ByteView> bytes = cell->bytes();
  EXPECT_TRUE(bytes.has_value());
  common::ByteReader reader{*bytes};
  const common::Result<std::int64_t> value = reader.read_i64_le();
  EXPECT_TRUE(value.has_value());
  EXPECT_TRUE(reader.empty());
  return value.value_or(0);
}

[[nodiscard]] common::ByteView bytes_cell(const VectorChunk& chunk, const std::size_t column,
                                          const std::size_t row) {
  return chunk.cell({.column_ordinal = column, .selected_row = row})->bytes().value();
}

[[nodiscard]] std::unique_ptr<PhysicalOperator>
scan(const QueryResourceContext& resources, CsegPartPin part,
     const cseg::CsegProjectedReaderLimits reader_limits = {}) {
  const schema::SchemaLineage lineage = valid_lineage();
  const std::array<std::uint32_t, 1> requested{0U};
  CsegScanLimits limits;
  limits.reader = reader_limits;
  common::Result<std::unique_ptr<PhysicalOperator>> result = CsegScanOperator::create(
      resources, std::move(part), lineage, cseg::test::identifier<schema::SchemaId>(4U),
      cseg::test::identifier<schema::TabletId>(3U),
      std::vector<std::uint32_t>{requested.begin(), requested.end()}, limits);
  return std::move(*result);
}

TEST(CsegPartPinTest, RejectsMissingEmptyAndUnderreportedOwners) {
  const auto owner = std::make_shared<const std::array<std::byte, 1>>();
  EXPECT_EQ(CsegPartPin::create({}, *owner, owner->size()).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(CsegPartPin::create(owner, {}, owner->size()).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(CsegPartPin::create(owner, *owner, 0U).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(CsegScanOperatorTest, ReturnsPinnedRawGranuleAndReleasesSourceAtLastOutput) {
  auto resources = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  const auto destroyed = std::make_shared<bool>(false);
  std::unique_ptr<PhysicalOperator> source =
      scan(resources, pin(cseg::test::make_valid_part(cseg::PageCompression::kNone), destroyed));
  EXPECT_GT(resources.reserved_memory_bytes(), 0U);

  common::Result<PhysicalOperatorStep> step = source->next(resources);
  ASSERT_TRUE(step.has_value());
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  ASSERT_NE(step->chunk(), nullptr);
  EXPECT_EQ(step->chunk()->chunk().physical_row_count(), 2U);
  EXPECT_EQ(step->chunk()->chunk().selected_row_count(), 2U);
  EXPECT_EQ(step->chunk()->chunk().column_count(), 1U);
  EXPECT_EQ(int64_cell(step->chunk()->chunk(), 0U), -5);
  EXPECT_EQ(int64_cell(step->chunk()->chunk(), 1U), 10);
  EXPECT_GE(step->chunk()->charged_memory_bytes(), step->chunk()->chunk().retained_buffer_bytes());
  EXPECT_FALSE(*destroyed);

  source.reset();
  EXPECT_FALSE(*destroyed);
  step =
      common::make_unexpected(common::Status{common::StatusCode::kInternal, "replace owning step"});
  EXPECT_TRUE(*destroyed);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(CsegScanOperatorTest, DecodesCompressedPagesAndEndsSticky) {
  auto resources = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  std::unique_ptr<PhysicalOperator> source =
      scan(resources, pin(cseg::test::make_valid_part(cseg::PageCompression::kZstd)));
  {
    const auto step = source->next(resources);
    ASSERT_TRUE(step.has_value());
    ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
    EXPECT_EQ(int64_cell(step->chunk()->chunk(), 1U), 10);
  }
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_EQ(source->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(source->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(CsegScanOperatorTest, SynthesizesNullableTailAndPreservesCallerColumnOrder) {
  auto resources = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  const schema::SchemaLineage lineage = evolved_lineage();
  auto source = CsegScanOperator::create(
      resources, pin(cseg::test::make_valid_part(cseg::PageCompression::kNone)), lineage,
      cseg::test::identifier<schema::SchemaId>(6U), cseg::test::identifier<schema::TabletId>(3U),
      std::vector<std::uint32_t>{1U, 0U});
  ASSERT_TRUE(source.has_value());
  const auto step = (*source)->next(resources);
  ASSERT_TRUE(step.has_value());
  ASSERT_NE(step->chunk(), nullptr);
  ASSERT_EQ(step->chunk()->chunk().column_count(), 2U);
  const columnar::PhysicalColumnView* added = step->chunk()->chunk().column(0U);
  ASSERT_NE(added, nullptr);
  EXPECT_EQ(added->type().kind(), schema::LogicalTypeKind::kString);
  EXPECT_EQ(added->null_count(), 2U);
  EXPECT_TRUE(step->chunk()->chunk().cell({.column_ordinal = 0U, .selected_row = 0U})->is_null());
  EXPECT_EQ(int64_cell(step->chunk()->chunk(), 1U, 1U), 10);
}

TEST(CsegScanOperatorTest, SupportsEmptyUserProjectionWithSystemValidationAndCardinality) {
  auto resources = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  const schema::SchemaLineage lineage = valid_lineage();
  auto source = CsegScanOperator::create(
      resources, pin(cseg::test::make_valid_part(cseg::PageCompression::kNone)), lineage,
      cseg::test::identifier<schema::SchemaId>(4U), cseg::test::identifier<schema::TabletId>(3U),
      {});
  ASSERT_TRUE(source.has_value());
  const auto step = (*source)->next(resources);
  ASSERT_TRUE(step.has_value());
  ASSERT_NE(step->chunk(), nullptr);
  EXPECT_EQ(step->chunk()->chunk().column_count(), 0U);
  EXPECT_EQ(step->chunk()->chunk().physical_row_count(), 2U);
  EXPECT_EQ(step->chunk()->chunk().selected_row_count(), 2U);
  EXPECT_GT(step->chunk()->chunk().buffer_bytes(),
            step->chunk()->chunk().selection().buffer_bytes());
}

TEST(CsegScanOperatorTest, AppendsTheFrozenBorrowedRowVersionSuffix) {
  auto resources = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  const schema::SchemaLineage lineage = valid_lineage();
  CsegScanLimits limits;
  limits.row_version_columns = RowVersionScanMode::kAppend;
  auto source = CsegScanOperator::create(
      resources, pin(cseg::test::make_valid_part(cseg::PageCompression::kNone)), lineage,
      cseg::test::identifier<schema::SchemaId>(4U), cseg::test::identifier<schema::TabletId>(3U),
      {}, limits);
  ASSERT_TRUE(source.has_value()) << source.error().to_string();
  const auto step = (*source)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  const VectorChunk& chunk = step->chunk()->chunk();
  ASSERT_EQ(chunk.column_count(), kVectorRowVersionColumnCount);
  const auto layout = vector_row_version_layout(0U).value();
  EXPECT_EQ(chunk.column(layout.wal_id_column_ordinal())->type().kind(),
            schema::LogicalTypeKind::kUuid);
  EXPECT_EQ(chunk.column(layout.record_sequence_column_ordinal())->type().kind(),
            schema::LogicalTypeKind::kUInt64);
  EXPECT_EQ(chunk.column(layout.row_ordinal_column_ordinal())->type().kind(),
            schema::LogicalTypeKind::kUInt32);
  EXPECT_EQ(chunk.column(layout.operation_column_ordinal())->type().kind(),
            schema::LogicalTypeKind::kUInt8);
  const common::Uuid::Bytes expected_wal = cseg::test::identifier<schema::SchemaId>(0x70U).bytes();
  EXPECT_TRUE(
      std::ranges::equal(bytes_cell(chunk, layout.wal_id_column_ordinal(), 0U), expected_wal));
  common::ByteReader sequence{bytes_cell(chunk, layout.record_sequence_column_ordinal(), 1U)};
  EXPECT_EQ(sequence.read_u64_le().value(), 7U);
  common::ByteReader ordinal{bytes_cell(chunk, layout.row_ordinal_column_ordinal(), 1U)};
  EXPECT_EQ(ordinal.read_u32_le().value(), 1U);
  ASSERT_EQ(bytes_cell(chunk, layout.operation_column_ordinal(), 0U).size(), 1U);
  EXPECT_EQ(bytes_cell(chunk, layout.operation_column_ordinal(), 0U).front(), std::byte{1U});

  CsegScanLimits too_narrow = limits;
  too_narrow.chunk.maximum_columns = kVectorRowVersionColumnCount - 1U;
  auto rejected = CsegScanOperator::create(
      resources, pin(cseg::test::make_valid_part(cseg::PageCompression::kNone)), lineage,
      cseg::test::identifier<schema::SchemaId>(4U), cseg::test::identifier<schema::TabletId>(3U),
      {}, too_narrow);
  ASSERT_TRUE(rejected.has_value());
  const auto failed = (*rejected)->next(resources);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(CsegScanOperatorTest, RejectsAnotherQueryAndRequestsItsCancellation) {
  auto owner = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  auto impostor = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  std::unique_ptr<PhysicalOperator> source =
      scan(owner, pin(cseg::test::make_valid_part(cseg::PageCompression::kNone)));
  const auto failed = source->next(impostor);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(impostor.is_cancelled());
  EXPECT_FALSE(owner.is_cancelled());
}

TEST(CsegScanOperatorTest, PreCancelledPullDoesNotDecodeOrReleaseTheLiveSourcePin) {
  auto resources = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  const auto destroyed = std::make_shared<bool>(false);
  std::unique_ptr<PhysicalOperator> source =
      scan(resources, pin(cseg::test::make_valid_part(cseg::PageCompression::kNone), destroyed));
  const std::size_t source_charge = resources.reserved_memory_bytes();
  EXPECT_TRUE(resources.request_cancel());
  EXPECT_EQ(source->next(resources).error().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(resources.reserved_memory_bytes(), source_charge);
  EXPECT_FALSE(*destroyed);
  source.reset();
  EXPECT_TRUE(*destroyed);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(CsegScanOperatorTest, RejectsInvalidLimitsAndProjectionBeforeReturningASource) {
  const schema::SchemaLineage lineage = valid_lineage();
  auto resources = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  const CsegPartPin part = pin(cseg::test::make_valid_part(cseg::PageCompression::kNone));
  CsegScanLimits limits;
  limits.chunk.maximum_rows = 0U;
  EXPECT_EQ(CsegScanOperator::create(resources, part, lineage,
                                     cseg::test::identifier<schema::SchemaId>(4U),
                                     cseg::test::identifier<schema::TabletId>(3U), {0U}, limits)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  EXPECT_EQ(CsegScanOperator::create(resources, part, lineage,
                                     cseg::test::identifier<schema::SchemaId>(4U),
                                     cseg::test::identifier<schema::TabletId>(3U), {0U, 0U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(CsegScanOperatorTest, EnforcesRowLogicalAndRetainedLimitsBeforeDecode) {
  for (const std::size_t constrained_limit : {0U, 1U, 2U}) {
    SCOPED_TRACE(constrained_limit);
    auto resources = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
    const schema::SchemaLineage lineage = valid_lineage();
    CsegScanLimits limits;
    if (constrained_limit == 0U)
      limits.chunk.maximum_rows = 1U;
    else if (constrained_limit == 1U)
      limits.chunk.maximum_buffer_bytes = 1U;
    else
      limits.chunk.maximum_retained_buffer_bytes = 1U;
    auto source = CsegScanOperator::create(
        resources, pin(cseg::test::make_valid_part(cseg::PageCompression::kNone)), lineage,
        cseg::test::identifier<schema::SchemaId>(4U), cseg::test::identifier<schema::TabletId>(3U),
        {0U}, limits);
    ASSERT_TRUE(source.has_value());
    const auto failed = (*source)->next(resources);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(resources.is_cancelled());
  }
}

TEST(CsegScanOperatorTest, EnforcesColumnLimitBeforeObservingPageCorruption) {
  cseg::EncodedCsegPart encoded = cseg::test::make_valid_part(cseg::PageCompression::kNone);
  std::vector<std::byte> bytes(encoded.bytes().begin(), encoded.bytes().end());
  const auto metadata = cseg::decode_cseg_v1_metadata_prefix(bytes);
  ASSERT_TRUE(metadata.has_value());
  bytes[static_cast<std::size_t>(metadata->pages()[0].page_offset)] ^= std::byte{1U};
  auto owner = std::make_shared<const std::vector<std::byte>>(std::move(bytes));
  CsegPartPin corrupt = CsegPartPin::create(owner, *owner, owner->capacity() + 64U).value();

  auto resources = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  const schema::SchemaLineage lineage = evolved_lineage();
  CsegScanLimits limits;
  limits.chunk.maximum_columns = 1U;
  auto source = CsegScanOperator::create(
      resources, std::move(corrupt), lineage, cseg::test::identifier<schema::SchemaId>(6U),
      cseg::test::identifier<schema::TabletId>(3U), {0U, 1U}, limits);
  ASSERT_TRUE(source.has_value());
  const auto failed = (*source)->next(resources);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(resources.is_cancelled());
}

TEST(CsegScanOperatorTest, ReservesOutputBeforePageCorruptionCanBeObserved) {
  const CsegPartPin valid = pin(cseg::test::make_valid_part(cseg::PageCompression::kNone));
  auto measuring = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  std::unique_ptr<PhysicalOperator> measured = scan(measuring, valid);
  const std::size_t source_charge = measuring.reserved_memory_bytes();
  measured.reset();

  auto constrained = QueryResourceContext::create(source_charge + 1U).value();
  std::unique_ptr<PhysicalOperator> source = scan(constrained, valid);
  const auto failed = source->next(constrained);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(constrained.is_cancelled());
}

TEST(CsegScanOperatorTest, DetectsRequestedPageCorruptionAndCancels) {
  cseg::EncodedCsegPart encoded = cseg::test::make_valid_part(cseg::PageCompression::kNone);
  std::vector<std::byte> bytes(encoded.bytes().begin(), encoded.bytes().end());
  const auto metadata = cseg::decode_cseg_v1_metadata_prefix(bytes);
  ASSERT_TRUE(metadata.has_value());
  bytes[static_cast<std::size_t>(metadata->pages()[0].page_offset)] ^= std::byte{1U};
  auto owner = std::make_shared<const std::vector<std::byte>>(std::move(bytes));
  CsegPartPin corrupt = CsegPartPin::create(owner, *owner, owner->capacity() + 64U).value();
  auto resources = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  std::unique_ptr<PhysicalOperator> source = scan(resources, std::move(corrupt));
  const auto failed = source->next(resources);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(resources.is_cancelled());
}

TEST(CsegScanOperatorTest, EventTimePruningSkipsDisjointGranulePagesBeforeDecode) {
  cseg::EncodedCsegPart encoded = cseg::test::make_valid_part_with_rows(
      10U, 5U, cseg::PageCompression::kNone, {.first_event_time = 0});
  std::vector<std::byte> bytes(encoded.bytes().begin(), encoded.bytes().end());
  const auto metadata = cseg::decode_cseg_v1_metadata_prefix(bytes);
  ASSERT_TRUE(metadata.has_value());
  ASSERT_EQ(metadata->granules().size(), 2U);
  // Damage a mandatory system page in the first granule. A point predicate selecting only the
  // second granule must neither validate nor decode this body.
  const std::size_t skipped_page = static_cast<std::size_t>(metadata->pages()[1U].page_offset);
  bytes[skipped_page] ^= std::byte{1U};
  auto owner = std::make_shared<const std::vector<std::byte>>(std::move(bytes));
  CsegPartPin part = CsegPartPin::create(owner, *owner, owner->capacity() + 64U).value();
  auto resources = QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  const schema::SchemaLineage lineage = valid_lineage();
  auto source = CsegScanOperator::create_event_time_pruned(
      resources, std::move(part), lineage, cseg::test::identifier<schema::SchemaId>(4U),
      cseg::test::identifier<schema::TabletId>(3U), {0U},
      {.lower = cseg::EventTimeBound{.value = 7, .inclusive = true},
       .upper = cseg::EventTimeBound{.value = 7, .inclusive = true}});
  ASSERT_TRUE(source.has_value()) << source.error().to_string();
  const auto step = (*source)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_NE(step->chunk(), nullptr);
  ASSERT_EQ(step->chunk()->chunk().selected_row_count(), 5U);
  for (std::size_t row = 0U; row < 5U; ++row)
    EXPECT_EQ(int64_cell(step->chunk()->chunk(), row), static_cast<std::int64_t>(row + 5U));
  EXPECT_EQ((*source)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_FALSE(resources.is_cancelled());
}

TEST(CsegScanOperatorTest, EmptyPruningPlanEndsWithoutPageWorkAndLimitsAreValidated) {
  const schema::SchemaLineage lineage = valid_lineage();
  const CsegPartPin part = pin(cseg::test::make_valid_part_with_rows(10U, 2U));
  auto resources = QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  const cseg::EventTimePredicate empty{.lower = cseg::EventTimeBound{.value = 2, .inclusive = true},
                                       .upper =
                                           cseg::EventTimeBound{.value = 1, .inclusive = true}};
  auto source = CsegScanOperator::create_event_time_pruned(
      resources, part, lineage, cseg::test::identifier<schema::SchemaId>(4U),
      cseg::test::identifier<schema::TabletId>(3U), {0U}, empty);
  ASSERT_TRUE(source.has_value()) << source.error().to_string();
  EXPECT_GT(resources.reserved_memory_bytes(), 0U);
  EXPECT_EQ((*source)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_EQ((*source)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);

  CsegScanLimits invalid_limits;
  invalid_limits.pruning.max_granules = 0U;
  EXPECT_EQ(CsegScanOperator::create_event_time_pruned(
                resources, part, lineage, cseg::test::identifier<schema::SchemaId>(4U),
                cseg::test::identifier<schema::TabletId>(3U), {0U}, empty, invalid_limits)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(CsegScanOperatorTest, ComposesWithPhysicalShapeAndLimitWhileKeepingOutputAlive) {
  auto resources = QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  std::unique_ptr<PhysicalOperator> source =
      scan(resources, pin(cseg::test::make_valid_part(cseg::PageCompression::kNone)));
  auto plan =
      PhysicalPipelinePlan::create(
          {{.type = cseg::test::type(schema::LogicalTypeKind::kTimestampNs), .nullable = false}},
          {LimitStage{.maximum_rows = 1U}})
          .value();
  common::Result<std::unique_ptr<PhysicalOperator>> instantiated =
      plan.instantiate(std::move(source));
  std::unique_ptr<PhysicalOperator> pipeline = std::move(*instantiated);
  {
    const auto step = pipeline->next(resources);
    ASSERT_TRUE(step.has_value());
    ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
    EXPECT_EQ(step->chunk()->chunk().selected_row_count(), 1U);
    EXPECT_EQ(int64_cell(step->chunk()->chunk(), 0U), -5);
  }
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_EQ(pipeline->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(CsegScanOperatorPropertyTest, PreservesRowsAcrossDeterministicGranuleBoundaries) {
  for (const cseg::PageCompression compression :
       {cseg::PageCompression::kNone, cseg::PageCompression::kZstd}) {
    for (const std::uint32_t rows : {1U, 2U, 7U, 17U}) {
      for (const std::uint32_t granule_rows : {1U, 3U, 8U}) {
        auto resources = QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
        std::unique_ptr<PhysicalOperator> source = scan(
            resources, pin(cseg::test::make_valid_part_with_rows(rows, granule_rows, compression)));
        std::uint32_t observed = 0U;
        for (;;) {
          common::Result<PhysicalOperatorStep> step = source->next(resources);
          ASSERT_TRUE(step.has_value()) << "rows=" << rows << " granule=" << granule_rows;
          if (step->kind() == PhysicalOperatorStepKind::kEnd)
            break;
          ASSERT_NE(step->chunk(), nullptr);
          for (std::size_t selected = 0U; selected < step->chunk()->chunk().selected_row_count();
               ++selected) {
            EXPECT_EQ(int64_cell(step->chunk()->chunk(), selected),
                      static_cast<std::int64_t>(observed) - 100);
            ++observed;
          }
        }
        EXPECT_EQ(observed, rows);
        EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      }
    }
  }
}

TEST(CsegScanOperatorPropertyTest, PointPruningMatchesIndependentGranuleIntersectionModel) {
  for (const cseg::PageCompression compression :
       {cseg::PageCompression::kNone, cseg::PageCompression::kZstd}) {
    for (const std::int64_t point : {-10, -8, -7, -5, 0, 7, 8, 9}) {
      SCOPED_TRACE(static_cast<int>(compression));
      SCOPED_TRACE(point);
      auto resources = QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
      const schema::SchemaLineage lineage = valid_lineage();
      auto source = CsegScanOperator::create_event_time_pruned(
          resources,
          pin(cseg::test::make_valid_part_with_rows(17U, 3U, compression,
                                                    {.first_event_time = -8})),
          lineage, cseg::test::identifier<schema::SchemaId>(4U),
          cseg::test::identifier<schema::TabletId>(3U), {0U},
          {.lower = cseg::EventTimeBound{.value = point, .inclusive = true},
           .upper = cseg::EventTimeBound{.value = point, .inclusive = true}});
      ASSERT_TRUE(source.has_value()) << source.error().to_string();
      std::vector<std::int64_t> observed;
      for (;;) {
        common::Result<PhysicalOperatorStep> step = (*source)->next(resources);
        ASSERT_TRUE(step.has_value()) << step.error().to_string();
        if (step->kind() == PhysicalOperatorStepKind::kEnd)
          break;
        for (std::size_t row = 0U; row < step->chunk()->chunk().selected_row_count(); ++row)
          observed.push_back(int64_cell(step->chunk()->chunk(), row));
      }
      std::vector<std::int64_t> expected;
      for (std::int64_t first = -8; first <= 8; first += 3) {
        const std::int64_t last = std::min<std::int64_t>(first + 2, 8);
        if (first <= point && point <= last) {
          for (std::int64_t value = first; value <= last; ++value)
            expected.push_back(value);
        }
      }
      EXPECT_EQ(observed, expected);
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    }
  }
}

} // namespace
} // namespace chronos::query
