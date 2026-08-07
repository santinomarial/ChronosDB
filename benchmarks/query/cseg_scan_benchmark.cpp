#include "chronos/cseg/part_codec.hpp"
#include "chronos/query/cseg_scan.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"
#include "support/counting_allocator.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] schema::SchemaLineage lineage() {
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

struct Fixture {
  Fixture(const std::uint32_t rows, const cseg::PageCompression compression)
      : schema_lineage(lineage()),
        encoded(std::make_shared<const cseg::EncodedCsegPart>(
            cseg::test::make_valid_part_with_rows(rows, rows, compression))),
        part(CsegPartPin::create(encoded, encoded->bytes(),
                                 encoded->retained_buffer_bytes() + sizeof(cseg::EncodedCsegPart) +
                                     64U)
                 .value()),
        resources(QueryResourceContext::create(std::size_t{1U} * 1024U * 1024U * 1024U).value()) {}

  [[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>> source() const {
    return CsegScanOperator::create(resources, part, schema_lineage,
                                    cseg::test::identifier<schema::SchemaId>(4U),
                                    cseg::test::identifier<schema::TabletId>(3U), {0U});
  }

  schema::SchemaLineage schema_lineage;
  std::shared_ptr<const cseg::EncodedCsegPart> encoded;
  CsegPartPin part;
  QueryResourceContext resources;
};

void scan_one_cseg_granule(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const cseg::PageCompression compression =
      state.range(1) == 0 ? cseg::PageCompression::kNone : cseg::PageCompression::kZstd;
  const Fixture fixture{rows, compression};

  std::size_t measured_allocations = 0U;
  std::size_t measured_bytes = 0U;
  {
    auto source = fixture.source();
    if (!source.has_value()) {
      const std::string message = source.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark_support::ScopedAllocationCounting counting;
    auto step = (*source)->next(fixture.resources);
    measured_allocations = counting.stop().allocations;
    if (!step.has_value() || step->chunk() == nullptr) {
      const std::string message =
          step.has_value() ? "CSEG scan returned no benchmark chunk" : step.error().to_string();
      state.SkipWithError(message);
      return;
    }
    measured_bytes = step->chunk()->chunk().buffer_bytes();
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto source = fixture.source();
    if (!source.has_value()) {
      const std::string message = source.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
    auto step = (*source)->next(fixture.resources);
    benchmark::DoNotOptimize(step);
    state.PauseTiming();
    if (!step.has_value() || step->chunk() == nullptr) {
      const std::string message =
          step.has_value() ? "CSEG scan returned no benchmark chunk" : step.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * rows);
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(measured_bytes));
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.SetLabel(compression == cseg::PageCompression::kNone
                     ? "raw one-user plus system granule; source pre-opened per iteration"
                     : "Zstd-policy one-user plus system granule; source pre-opened per iteration");
}

// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(scan_one_cseg_granule)->ArgsProduct({{64, 1'024, 65'536}, {0, 1}});

} // namespace
} // namespace chronos::query
