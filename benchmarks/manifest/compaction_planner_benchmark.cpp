#include "chronos/manifest/compaction_planner.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace chronos::manifest {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint64_t value) {
  common::Uuid::Bytes bytes{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[bytes.size() - 1U - index] =
        static_cast<std::byte>(static_cast<std::uint8_t>(value >> (index * 8U)));
  }
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] std::vector<PartDescriptor> make_parts(const std::size_t count) {
  const schema::TableId table_id = id<schema::TableId>(1U);
  const schema::TabletId tablet_id = id<schema::TabletId>(2U);
  const schema::SchemaId schema_id = id<schema::SchemaId>(3U);
  std::vector<PartDescriptor> parts;
  parts.reserve(count);
  for (std::size_t index = 0U; index < count; ++index) {
    const std::uint64_t sequence = static_cast<std::uint64_t>(index) + 1U;
    std::int64_t minimum = static_cast<std::int64_t>(index) * 100;
    if (index % 8U == 7U) {
      minimum -= 400;
    }
    parts.push_back({.part_id = id<cseg::PartId>(sequence),
                     .table_id = table_id,
                     .tablet_id = tablet_id,
                     .schema_id = schema_id,
                     .schema_version = schema::SchemaVersion::initial(),
                     .file_length = 1U << 20U,
                     .row_count = 65'536U,
                     .minimum_record_sequence = sequence,
                     .maximum_record_sequence = sequence,
                     .minimum_event_time = minimum,
                     .maximum_event_time = minimum + 99});
  }
  return parts;
}

void benchmark_compaction_planning(benchmark::State& state) {
  const auto count = static_cast<std::size_t>(state.range(0));
  const std::vector<PartDescriptor> parts = make_parts(count);
  std::size_t selected = 0U;
  for ([[maybe_unused]] auto iteration : state) {
    auto plan = plan_append_only_compaction(parts);
    if (!plan.has_value() || !plan->has_value()) {
      const std::string message = plan.has_value() ? "generated overlap workload produced no plan"
                                                   : plan.error().to_string();
      state.SkipWithError(message);
      return;
    }
    selected = (**plan).input_part_ids().size();
    benchmark::DoNotOptimize((**plan).input_part_ids().data());
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(count));
  state.counters["selected_parts"] = static_cast<double>(selected);
  state.SetLabel("one tablet/schema; every eighth arrival late by four ranges; bounded fan-in");
}

// Google Benchmark registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_compaction_planning)->Arg(128)->Arg(4'096)->Arg(65'536);

} // namespace
} // namespace chronos::manifest
