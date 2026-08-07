#include "chronos/common/bytes.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/query/cseg_scan.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <cstddef>
#include <cstdint>
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
  return 0;
}
