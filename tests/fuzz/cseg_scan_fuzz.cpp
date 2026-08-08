#include "chronos/common/bytes.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/query/cseg_scan.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/row_version.hpp"
#include "chronos/query/snapshot_pipeline.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace {

[[nodiscard]] const chronos::schema::SchemaLineage& lineage() {
  // NOLINTNEXTLINE(bugprone-throwing-static-initialization)
  static const chronos::schema::SchemaLineage value = [] {
    const chronos::schema::ColumnId event_time =
        chronos::cseg::test::identifier<chronos::schema::ColumnId>(5U);
    std::vector<chronos::schema::ColumnDefinition> columns;
    columns.push_back(chronos::schema::ColumnDefinition::create(
                          event_time, "event_time",
                          chronos::cseg::test::type(chronos::schema::LogicalTypeKind::kTimestampNs),
                          false)
                          .value());
    chronos::schema::TableSchema table =
        chronos::schema::TableSchema::create(
            chronos::cseg::test::identifier<chronos::schema::TableId>(2U),
            chronos::cseg::test::identifier<chronos::schema::SchemaId>(4U),
            chronos::schema::SchemaVersion::initial(), std::nullopt, std::move(columns),
            {.event_time_column = event_time,
             .physical_ordering_key = {event_time},
             .partition_columns = {event_time},
             .shard_key = {event_time},
             .deduplication_key = {}})
            .value();
    return chronos::schema::SchemaLineage::create(std::move(table)).value();
  }();
  return value;
}

void exercise(const std::shared_ptr<const std::vector<std::byte>>& owner,
              const std::span<const std::uint8_t> input) {
  if (owner->empty())
    return;
  auto part = chronos::query::CsegPartPin::create(owner, *owner, owner->capacity() + 64U);
  if (!part.has_value())
    return;
  auto resources = chronos::query::QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U);
  if (!resources.has_value())
    return;
  std::vector<std::uint32_t> projection;
  const std::size_t requested = input.empty() ? 1U : input.size() % 4U;
  projection.reserve(requested);
  for (std::size_t index = 0U; index < requested; ++index)
    projection.push_back(input.empty() ? 0U : input[index % input.size()] % 3U);
  chronos::query::CsegScanLimits limits;
  limits.row_version_columns = !input.empty() && (input.back() & 16U) != 0U
                                   ? chronos::query::RowVersionScanMode::kAppend
                                   : chronos::query::RowVersionScanMode::kOmit;
  chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> source =
      !input.empty() && (input.front() & 2U) != 0U
          ? chronos::query::CsegScanOperator::create_event_time_pruned(
                *resources, std::move(*part), lineage(),
                chronos::cseg::test::identifier<chronos::schema::SchemaId>(4U),
                chronos::cseg::test::identifier<chronos::schema::TabletId>(3U),
                std::move(projection),
                {.lower =
                     chronos::cseg::EventTimeBound{.value = static_cast<std::int8_t>(input.front()),
                                                   .inclusive = (input.front() & 4U) != 0U},
                 .upper =
                     chronos::cseg::EventTimeBound{.value = static_cast<std::int8_t>(input.back()),
                                                   .inclusive = (input.back() & 4U) != 0U}},
                limits)
          : chronos::query::CsegScanOperator::create(
                *resources, std::move(*part), lineage(),
                chronos::cseg::test::identifier<chronos::schema::SchemaId>(4U),
                chronos::cseg::test::identifier<chronos::schema::TabletId>(3U),
                std::move(projection), limits);
  if (!source.has_value())
    return;
  if (!input.empty() && (input.front() & 1U) != 0U)
    static_cast<void>(resources->request_cancel());
  for (std::size_t pull = 0U; pull < 3U; ++pull) {
    auto step = (*source)->next(*resources);
    if (!step.has_value() || step->kind() == chronos::query::PhysicalOperatorStepKind::kEnd)
      break;
    const chronos::query::AccountedVectorChunk* chunk = step->chunk();
    if (chunk != nullptr && chunk->chunk().column_count() != 0U &&
        chunk->chunk().selected_row_count() != 0U) {
      [[maybe_unused]] const auto cell =
          chunk->chunk().cell({.column_ordinal = 0U, .selected_row = 0U});
    }
  }
}

void exercise_exact_prune_filter(const std::shared_ptr<const std::vector<std::byte>>& owner,
                                 const std::span<const std::uint8_t> input) {
  if (owner->empty())
    return;
  auto part = chronos::query::CsegPartPin::create(owner, *owner, owner->capacity() + 64U);
  auto resources = chronos::query::QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U);
  if (!part.has_value() || !resources.has_value())
    return;
  const std::int64_t lower =
      input.empty() ? -100 : static_cast<std::int64_t>(static_cast<std::int8_t>(input.front()));
  const std::int64_t upper =
      input.empty() ? 100 : static_cast<std::int64_t>(static_cast<std::int8_t>(input.back()));
  const bool lower_inclusive = input.empty() || (input.front() & 1U) != 0U;
  const bool upper_inclusive = input.empty() || (input.back() & 1U) != 0U;
  chronos::query::CsegScanLimits limits;
  limits.row_version_columns = !input.empty() && (input.back() & 16U) != 0U
                                   ? chronos::query::RowVersionScanMode::kAppend
                                   : chronos::query::RowVersionScanMode::kOmit;
  auto source = chronos::query::CsegScanOperator::create_event_time_pruned(
      *resources, std::move(*part), lineage(),
      chronos::cseg::test::identifier<chronos::schema::SchemaId>(4U),
      chronos::cseg::test::identifier<chronos::schema::TabletId>(3U), {0U},
      {.lower = chronos::cseg::EventTimeBound{.value = lower, .inclusive = lower_inclusive},
       .upper = chronos::cseg::EventTimeBound{.value = upper, .inclusive = upper_inclusive}},
      limits);
  if (!source.has_value())
    return;
  source = chronos::query::TimestampRangeFilterOperator::create(
      std::move(*source), 0U,
      {.lower = chronos::query::TimestampRangeBound{.value = lower, .inclusive = lower_inclusive},
       .upper = chronos::query::TimestampRangeBound{.value = upper, .inclusive = upper_inclusive}});
  if (!source.has_value())
    return;
  if (!input.empty() && (input.front() & 8U) != 0U) {
    source = chronos::query::ColumnSubsetOperator::create(std::move(*source), {});
    if (!source.has_value())
      return;
  }
  for (std::size_t pull = 0U; pull < 8U; ++pull) {
    auto step = (*source)->next(*resources);
    if (!step.has_value() || step->kind() == chronos::query::PhysicalOperatorStepKind::kEnd)
      break;
    if (step->chunk() != nullptr)
      static_cast<void>(step->chunk()->chunk().selected_row_count());
  }
}

void exercise_complete_snapshot(const std::span<const std::uint8_t> input) {
  // NOLINTNEXTLINE(bugprone-throwing-static-initialization)
  static const chronos::query::test::SnapshotTabletScanFixture fixture{17U};
  auto resources = chronos::query::QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U);
  if (!resources.has_value())
    return;
  chronos::query::SnapshotTabletScanLimits limits;
  limits.maximum_heads = input.empty() || (input.front() & 1U) != 0U ? 1U : 0U;
  limits.maximum_retained_configuration_bytes =
      input.size() < 2U || (input[1] & 1U) != 0U
          ? chronos::query::kDefaultSnapshotTabletScanConfigurationByteLimit
          : 1U;
  limits.head.chunk.maximum_rows =
      input.empty() ? 17U : static_cast<std::uint32_t>(input.back() % 17U) + 1U;
  const bool append_suffix = input.size() >= 3U && (input[2] & 1U) != 0U;
  limits.cseg.row_version_columns = append_suffix ? chronos::query::RowVersionScanMode::kAppend
                                                  : chronos::query::RowVersionScanMode::kOmit;
  limits.head.row_version_columns = input.size() >= 4U && (input[3] & 1U) != 0U
                                        ? chronos::query::RowVersionScanMode::kAppend
                                        : limits.cseg.row_version_columns;
  std::optional<chronos::cseg::EventTimePredicate> predicate;
  if (input.size() >= 2U) {
    predicate = chronos::cseg::EventTimePredicate{
        .lower = chronos::cseg::EventTimeBound{.value = static_cast<std::int8_t>(input.front()),
                                               .inclusive = (input.front() & 2U) != 0U},
        .upper = chronos::cseg::EventTimeBound{.value = static_cast<std::int8_t>(input.back()),
                                               .inclusive = (input.back() & 2U) != 0U}};
  }
  auto source = fixture.source(*resources, predicate, limits);
  if (!source.has_value())
    return;
  if (!input.empty() && (input.front() & 4U) != 0U)
    static_cast<void>(resources->request_cancel());
  for (std::size_t pull = 0U; pull < 32U; ++pull) {
    auto step = (*source)->next(*resources);
    if (!step.has_value() || step->kind() == chronos::query::PhysicalOperatorStepKind::kEnd)
      break;
    if (step->chunk() != nullptr)
      static_cast<void>(step->chunk()->chunk().selected_row_count());
  }
}

void exercise_snapshot_pipeline(const std::span<const std::uint8_t> input) {
  // NOLINTNEXTLINE(bugprone-throwing-static-initialization)
  static const chronos::query::test::SnapshotTabletScanFixture fixture{17U};
  std::vector<chronos::query::PhysicalColumnShape> shape{
      {.type = fixture.schema_ptr()->columns().front().type(), .nullable = false}};
  if (!input.empty() && (input.front() & 1U) != 0U) {
    for (const chronos::query::VectorRowVersionColumnKind kind :
         {chronos::query::VectorRowVersionColumnKind::kWalId,
          chronos::query::VectorRowVersionColumnKind::kRecordSequence,
          chronos::query::VectorRowVersionColumnKind::kRowOrdinal,
          chronos::query::VectorRowVersionColumnKind::kOperation}) {
      shape.push_back({.type = chronos::query::vector_row_version_column_type(kind).value(),
                       .nullable = false});
    }
    if (input.size() > 1U && (input[1] & 1U) != 0U)
      shape.back().nullable = true;
  }
  auto plan = chronos::query::PhysicalPipelinePlan::create(
      std::move(shape), {chronos::query::LimitStage{input.empty() ? 17U : input.back() % 18U}});
  auto resources = chronos::query::QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U);
  if (!plan.has_value() || !resources.has_value())
    return;
  chronos::query::SnapshotTabletPipelineLimits limits;
  limits.scan.maximum_heads = input.size() < 3U || (input[2] & 1U) != 0U ? 1U : 0U;
  auto pipeline = chronos::query::instantiate_snapshot_tablet_pipeline(
      *resources, fixture.storage(), fixture.snapshot(),
      chronos::query::test::SnapshotTabletScanFixture::tablet_id(), fixture.lineage(),
      fixture.schema_ptr()->schema_id(), *plan, limits);
  if (!pipeline.has_value())
    return;
  if (input.size() > 3U && (input[3] & 1U) != 0U)
    static_cast<void>(resources->request_cancel());
  for (std::size_t pull = 0U; pull < 32U; ++pull) {
    auto step = (*pipeline)->next(*resources);
    if (!step.has_value() || step->kind() == chronos::query::PhysicalOperatorStepKind::kEnd)
      break;
  }
}

[[nodiscard]] chronos::query::PhysicalAsofPlan
snapshot_asof_plan(const chronos::query::test::SnapshotTabletScanFixture& fixture) {
  using namespace chronos::query;
  std::vector<PhysicalColumnShape> source_shape{
      {.type = fixture.schema_ptr()->columns().front().type(), .nullable = false}};
  for (const VectorRowVersionColumnKind kind :
       {VectorRowVersionColumnKind::kWalId, VectorRowVersionColumnKind::kRecordSequence,
        VectorRowVersionColumnKind::kRowOrdinal, VectorRowVersionColumnKind::kOperation}) {
    source_shape.push_back(
        {.type = vector_row_version_column_type(kind).value(), .nullable = false});
  }
  std::vector<VectorAsofColumnShape> asof_shape;
  std::vector<std::size_t> outputs;
  for (std::size_t ordinal = 0U; ordinal < source_shape.size(); ++ordinal) {
    asof_shape.push_back(
        {.type = source_shape[ordinal].type, .nullable = source_shape[ordinal].nullable});
    outputs.push_back(ordinal);
  }
  VectorAsofJoinDefinition definition{
      .left_input_columns = asof_shape,
      .right_input_columns = asof_shape,
      .equality_keys = {{.left_column_ordinal = 0U, .right_column_ordinal = 0U}},
      .left_timestamp_column_ordinal = 0U,
      .right_timestamp_column_ordinal = 0U,
      .right_physical_ordering_key_ordinals = {0U},
      .right_row_version_first_column_ordinal = 1U,
      .left_output_column_ordinals = outputs,
      .right_output_column_ordinals = outputs,
      .left_outer = true};
  std::vector<PhysicalColumnShape> joined_shape;
  const std::vector<VectorAsofColumnShape> joined_shapes =
      vector_asof_join_output_shape(definition).value();
  joined_shape.reserve(joined_shapes.size());
  for (const VectorAsofColumnShape& column : joined_shapes)
    joined_shape.push_back({.type = column.type, .nullable = column.nullable});
  std::vector<PhysicalAsofPlanJoin> joins;
  joins.push_back({.left_preparation = PhysicalPipelinePlan::create(source_shape, {}).value(),
                   .right_preparation = PhysicalPipelinePlan::create(source_shape, {}).value(),
                   .definition = std::move(definition)});
  return PhysicalAsofPlan::create(
             std::move(joins),
             PhysicalPipelinePlan::create(std::move(joined_shape),
                                          {ColumnSubsetStage{.column_ordinals = {5U}}})
                 .value())
      .value();
}

void exercise_snapshot_asof(const std::span<const std::uint8_t> input) {
  using namespace chronos::query;
  // NOLINTNEXTLINE(bugprone-throwing-static-initialization)
  static const test::SnapshotTabletScanFixture fixture{17U};
  // NOLINTNEXTLINE(bugprone-throwing-static-initialization)
  static const PhysicalAsofPlan plan = snapshot_asof_plan(fixture);
  auto resources = QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U);
  if (!resources.has_value())
    return;
  SnapshotTabletSourceBinding left{.target_tablet = test::SnapshotTabletScanFixture::tablet_id(),
                                   .lineage = std::cref(fixture.lineage()),
                                   .destination_schema_id = fixture.schema_ptr()->schema_id()};
  SnapshotTabletSourceBinding right = left;
  if (input.size() > 1U && (input[1] & 1U) != 0U)
    right.destination_schema_id = chronos::cseg::test::identifier<chronos::schema::SchemaId>(0x77U);
  right.limits.scan.maximum_heads = input.size() < 3U || (input[2] & 1U) != 0U ? 1U : 0U;
  const std::array sources{left, right};
  std::span<const SnapshotTabletSourceBinding> selected{sources};
  if (!input.empty() && (input.front() & 1U) != 0U)
    selected = selected.first(1U);
  auto pipeline = instantiate_snapshot_asof_plan(*resources, fixture.storage(), fixture.snapshot(),
                                                 selected, plan);
  if (!pipeline.has_value())
    return;
  if (input.size() > 3U && (input[3] & 1U) != 0U)
    static_cast<void>(resources->request_cancel());
  for (std::size_t pull = 0U; pull < 32U; ++pull) {
    auto step = (*pipeline)->next(*resources);
    if (!step.has_value() || step->kind() == PhysicalOperatorStepKind::kEnd)
      break;
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const std::span<const std::uint8_t> input{data, size};
  std::vector<std::byte> hostile(size);
  for (std::size_t index = 0U; index < size; ++index)
    hostile[index] = std::byte{data[index]};
  auto hostile_owner = std::make_shared<const std::vector<std::byte>>(std::move(hostile));
  exercise(hostile_owner, input);
  exercise_exact_prune_filter(std::move(hostile_owner), input);

  // Keep a rich authenticated seed in every run so configuration and output ownership stay hot.
  // NOLINTNEXTLINE(bugprone-throwing-static-initialization)
  static const std::vector<std::byte> canonical = [] {
    chronos::cseg::EncodedCsegPart part = chronos::cseg::test::make_valid_part_with_rows(
        17U, 3U, chronos::cseg::PageCompression::kZstd);
    return std::vector<std::byte>{part.bytes().begin(), part.bytes().end()};
  }();
  std::vector<std::byte> mutated = canonical;
  if (!input.empty())
    mutated[input.front() % mutated.size()] ^=
        std::byte{static_cast<std::uint8_t>(input.back() | 1U)};
  auto mutated_owner = std::make_shared<const std::vector<std::byte>>(std::move(mutated));
  exercise(mutated_owner, input);
  exercise_exact_prune_filter(std::move(mutated_owner), input);
  exercise_complete_snapshot(input);
  exercise_snapshot_pipeline(input);
  exercise_snapshot_asof(input);
  return 0;
}
