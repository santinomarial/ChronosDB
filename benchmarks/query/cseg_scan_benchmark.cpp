#include "chronos/cseg/part_codec.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/cseg_scan.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/snapshot_pipeline.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"
#include "support/counting_allocator.hpp"

#include <array>
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
  Fixture(const std::uint32_t rows, const cseg::PageCompression compression,
          const std::uint32_t rows_per_granule = 0U)
      : schema_lineage(lineage()),
        encoded(std::make_shared<const cseg::EncodedCsegPart>(cseg::test::make_valid_part_with_rows(
            rows, rows_per_granule == 0U ? rows : rows_per_granule, compression))),
        part(CsegPartPin::create(encoded, encoded->bytes(),
                                 encoded->retained_buffer_bytes() + sizeof(cseg::EncodedCsegPart) +
                                     64U)
                 .value()),
        resources(QueryResourceContext::create(std::size_t{1U} * 1024U * 1024U * 1024U).value()) {}

  [[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
  source(const RowVersionScanMode row_version_columns) const {
    CsegScanLimits limits;
    limits.row_version_columns = row_version_columns;
    return CsegScanOperator::create(resources, part, schema_lineage,
                                    cseg::test::identifier<schema::SchemaId>(4U),
                                    cseg::test::identifier<schema::TabletId>(3U), {0U}, limits);
  }

  [[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
  pruned_source(const std::int64_t event_time) const {
    return CsegScanOperator::create_event_time_pruned(
        resources, part, schema_lineage, cseg::test::identifier<schema::SchemaId>(4U),
        cseg::test::identifier<schema::TabletId>(3U), {0U},
        {.lower = cseg::EventTimeBound{.value = event_time, .inclusive = true},
         .upper = cseg::EventTimeBound{.value = event_time, .inclusive = true}});
  }

  [[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
  exact_pruned_source(const std::int64_t event_time) const {
    common::Result<std::unique_ptr<PhysicalOperator>> source = pruned_source(event_time);
    if (!source.has_value())
      return source;
    return TimestampRangeFilterOperator::create(
        std::move(*source), 0U,
        {.lower = TimestampRangeBound{.value = event_time, .inclusive = true},
         .upper = TimestampRangeBound{.value = event_time, .inclusive = true}});
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
  const RowVersionScanMode row_version_columns =
      state.range(2) == 0 ? RowVersionScanMode::kOmit : RowVersionScanMode::kAppend;
  const Fixture fixture{rows, compression};

  std::size_t measured_allocations = 0U;
  std::size_t measured_bytes = 0U;
  {
    auto source = fixture.source(row_version_columns);
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
    auto source = fixture.source(row_version_columns);
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
  const std::string suffix = row_version_columns == RowVersionScanMode::kAppend
                                 ? "; system pages exposed as borrowed row-version suffix"
                                 : "; system pages validated but hidden";
  state.SetLabel((compression == cseg::PageCompression::kNone
                      ? "raw one-user granule; source pre-opened per iteration"
                      : "Zstd-policy one-user granule; source pre-opened per iteration") +
                 suffix);
}

// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(scan_one_cseg_granule)->ArgsProduct({{64, 1'024, 65'536}, {0, 1}, {0, 1}});

void scan_one_selected_granule_among_many(benchmark::State& state) {
  constexpr std::uint32_t kRowsPerGranule = 64U;
  const auto granules = static_cast<std::uint32_t>(state.range(0));
  const cseg::PageCompression compression =
      state.range(1) == 0 ? cseg::PageCompression::kNone : cseg::PageCompression::kZstd;
  const std::uint32_t rows = granules * kRowsPerGranule;
  const Fixture fixture{rows, compression, kRowsPerGranule};
  const std::int64_t selected_event_time =
      -100 + static_cast<std::int64_t>((granules / 2U) * kRowsPerGranule);

  std::size_t measured_allocations = 0U;
  std::size_t measured_bytes = 0U;
  {
    auto source = fixture.pruned_source(selected_event_time);
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
          step.has_value() ? "pruned CSEG scan returned no chunk" : step.error().to_string();
      state.SkipWithError(message);
      return;
    }
    measured_bytes = step->chunk()->chunk().buffer_bytes();
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto source = fixture.pruned_source(selected_event_time);
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
          step.has_value() ? "pruned CSEG scan returned no chunk" : step.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * kRowsPerGranule);
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(measured_bytes));
  state.counters["candidate_granules"] = static_cast<double>(granules);
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.SetLabel(compression == cseg::PageCompression::kNone
                     ? "raw; one middle granule selected; planning excluded"
                     : "Zstd policy; one middle granule selected; planning excluded");
}

// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(scan_one_selected_granule_among_many)->ArgsProduct({{64, 4'096}, {0, 1}});

void scan_one_exact_row_among_many_granules(benchmark::State& state) {
  constexpr std::uint32_t kRowsPerGranule = 64U;
  const auto granules = static_cast<std::uint32_t>(state.range(0));
  const cseg::PageCompression compression =
      state.range(1) == 0 ? cseg::PageCompression::kNone : cseg::PageCompression::kZstd;
  const std::uint32_t rows = granules * kRowsPerGranule;
  const Fixture fixture{rows, compression, kRowsPerGranule};
  const std::int64_t selected_event_time =
      -100 + static_cast<std::int64_t>((granules / 2U) * kRowsPerGranule + 31U);

  std::size_t measured_allocations = 0U;
  std::size_t measured_bytes = 0U;
  {
    auto source = fixture.exact_pruned_source(selected_event_time);
    if (!source.has_value()) {
      const std::string message = source.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark_support::ScopedAllocationCounting counting;
    auto step = (*source)->next(fixture.resources);
    measured_allocations = counting.stop().allocations;
    if (!step.has_value() || step->chunk() == nullptr ||
        step->chunk()->chunk().selected_row_count() != 1U) {
      const std::string message = step.has_value()
                                      ? "exact pruned CSEG scan returned the wrong row count"
                                      : step.error().to_string();
      state.SkipWithError(message);
      return;
    }
    measured_bytes = step->chunk()->chunk().buffer_bytes();
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto source = fixture.exact_pruned_source(selected_event_time);
    if (!source.has_value()) {
      const std::string message = source.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
    auto step = (*source)->next(fixture.resources);
    benchmark::DoNotOptimize(step);
    state.PauseTiming();
    if (!step.has_value() || step->chunk() == nullptr ||
        step->chunk()->chunk().selected_row_count() != 1U) {
      const std::string message = step.has_value()
                                      ? "exact pruned CSEG scan returned the wrong row count"
                                      : step.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * kRowsPerGranule);
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(measured_bytes));
  state.counters["candidate_granules"] = static_cast<double>(granules);
  state.counters["matching_rows"] = 1.0;
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.SetLabel(compression == cseg::PageCompression::kNone
                     ? "raw; prune one middle granule then exact-filter one row"
                     : "Zstd policy; prune one middle granule then exact-filter one row");
}

// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(scan_one_exact_row_among_many_granules)->ArgsProduct({{64, 4'096}, {0, 1}});

void scan_complete_head_only_snapshot(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const test::SnapshotTabletScanFixture fixture{rows};
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{1U} * 1024U * 1024U * 1024U).value();
  std::size_t observed_rows = 0U;
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto source = fixture.source(resources);
    if (!source.has_value()) {
      const std::string message = source.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
    std::size_t iteration_rows = 0U;
    while (true) {
      auto step = (*source)->next(resources);
      if (!step.has_value()) {
        const std::string message = step.error().to_string();
        state.SkipWithError(message);
        return;
      }
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        break;
      iteration_rows += step->chunk()->chunk().selected_row_count();
    }
    observed_rows = iteration_rows;
    benchmark::DoNotOptimize(observed_rows);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(observed_rows));
  state.counters["published_heads"] = 1.0;
  state.counters["rows"] = static_cast<double>(observed_rows);
  state.SetLabel("exact aggregate publication; empty durable subset plus one active head");
}

// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(scan_complete_head_only_snapshot)->Arg(64)->Arg(1'024)->Arg(65'536);

void instantiate_and_execute_snapshot_pipeline(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const test::SnapshotTabletScanFixture fixture{rows};
  PhysicalPipelinePlan plan =
      PhysicalPipelinePlan::create(
          {{.type = fixture.schema_ptr()->columns().front().type(), .nullable = false}},
          {LimitStage{rows / 2U}})
          .value();
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{1U} * 1024U * 1024U * 1024U).value();
  std::size_t observed_rows = 0U;
  for ([[maybe_unused]] auto iteration : state) {
    auto pipeline = instantiate_snapshot_tablet_pipeline(
        resources, fixture.storage(), fixture.snapshot(),
        test::SnapshotTabletScanFixture::tablet_id(), fixture.lineage(),
        fixture.schema_ptr()->schema_id(), plan);
    if (!pipeline.has_value()) {
      const std::string message = pipeline.error().to_string();
      state.SkipWithError(message);
      return;
    }
    std::size_t iteration_rows = 0U;
    while (true) {
      auto step = (*pipeline)->next(resources);
      if (!step.has_value()) {
        const std::string message = step.error().to_string();
        state.SkipWithError(message);
        return;
      }
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        break;
      iteration_rows += step->chunk()->chunk().selected_row_count();
    }
    observed_rows = iteration_rows;
    benchmark::DoNotOptimize(observed_rows);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(observed_rows));
  state.counters["input_rows"] = static_cast<double>(rows);
  state.counters["limited_rows"] = static_cast<double>(observed_rows);
  state.SetLabel("snapshot plan/load/compose plus checked pipeline execution");
}

// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(instantiate_and_execute_snapshot_pipeline)->Arg(64)->Arg(1'024)->Arg(65'536);

[[nodiscard]] PhysicalAsofPlan lower_snapshot_asof(const test::SnapshotTabletScanFixture& fixture) {
  const std::vector<QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = fixture.schema_ptr()}};
  auto catalog = std::make_shared<const QueryCatalogSnapshot>(
      QueryCatalogSnapshot::create(1U, tables).value());
  auto parsed =
      parse_sql_v1_select("SELECT r.event_time FROM metrics AS l ASOF JOIN metrics AS r "
                          "ON l.event_time = r.event_time AND r.event_time <= l.event_time");
  auto bound = bind_sql_v1_select(std::move(parsed).value(), std::move(catalog));
  return std::move(lower_bound_sql_asof_select(*bound)).value();
}

void instantiate_and_execute_snapshot_asof(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const test::SnapshotTabletScanFixture fixture{rows};
  const PhysicalAsofPlan plan = lower_snapshot_asof(fixture);
  const SnapshotTabletSourceBinding source{
      .target_tablet = test::SnapshotTabletScanFixture::tablet_id(),
      .lineage = std::cref(fixture.lineage()),
      .destination_schema_id = fixture.schema_ptr()->schema_id()};
  const std::array sources{source, source};
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{1U} * 1024U * 1024U * 1024U).value();
  std::size_t observed_rows = 0U;
  for ([[maybe_unused]] auto iteration : state) {
    auto pipeline = instantiate_snapshot_asof_plan(resources, fixture.storage(), fixture.snapshot(),
                                                   sources, plan);
    if (!pipeline.has_value()) {
      const std::string message = pipeline.error().to_string();
      state.SkipWithError(message);
      return;
    }
    std::size_t iteration_rows = 0U;
    while (true) {
      auto step = (*pipeline)->next(resources);
      if (!step.has_value()) {
        const std::string message = step.error().to_string();
        state.SkipWithError(message);
        return;
      }
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        break;
      iteration_rows += step->chunk()->chunk().selected_row_count();
    }
    observed_rows = iteration_rows;
    benchmark::DoNotOptimize(observed_rows);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(observed_rows));
  state.counters["source_rows"] = static_cast<double>(rows) * 2.0;
  state.counters["joined_rows"] = static_cast<double>(observed_rows);
  state.SetLabel("one held aggregate epoch; two complete sources; bounded ASOF execution");
}

// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(instantiate_and_execute_snapshot_asof)->Arg(64)->Arg(1'024);

} // namespace
} // namespace chronos::query
#include <functional>
