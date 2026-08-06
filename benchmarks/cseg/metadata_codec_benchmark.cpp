#include "chronos/cseg/metadata_codec.hpp"
#include "chronos/cseg/pruning.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t value) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(value);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] chronos::schema::LogicalType type(const chronos::schema::LogicalTypeKind kind) {
  return chronos::schema::LogicalType::create(kind).value();
}

struct Fixture {
  std::vector<chronos::cseg::CsegColumnDescriptor> columns;
  std::vector<chronos::cseg::CsegGranuleDescriptor> granules;
  std::vector<chronos::cseg::CsegPageMetadataInput> pages;

  explicit Fixture(const std::uint32_t granule_count) {
    using namespace chronos::cseg;
    using chronos::schema::LogicalTypeKind;
    columns = {
        CsegColumnDescriptor{.column_id = id<chronos::schema::ColumnId>(5U),
                             .storage_kind = StorageKind::kUser,
                             .logical_type = type(LogicalTypeKind::kTimestampNs),
                             .nullable = false,
                             .event_time = true,
                             .schema_ordinal = 0U,
                             .ordering_ordinal = 0U},
        system(StorageKind::kWalId, LogicalTypeKind::kUuid),
        system(StorageKind::kRecordSequence, LogicalTypeKind::kUInt64),
        system(StorageKind::kRowOrdinal, LogicalTypeKind::kUInt32),
        system(StorageKind::kOperation, LogicalTypeKind::kUInt8),
    };
    granules.reserve(granule_count);
    pages.reserve(static_cast<std::size_t>(granule_count) * columns.size());
    for (std::uint32_t ordinal = 0U; ordinal < granule_count; ++ordinal) {
      granules.push_back(CsegGranuleDescriptor{
          .first_row = static_cast<std::uint64_t>(ordinal) * 64U,
          .row_count = 64U,
          .first_page_index = static_cast<std::uint64_t>(ordinal) * columns.size(),
          .minimum_event_time = ordinal,
          .maximum_event_time = ordinal,
      });
      pages.push_back(page(512U));
      pages.push_back(page(1'024U));
      pages.push_back(page(512U));
      pages.push_back(page(256U));
      pages.push_back(page(64U));
    }
  }

  [[nodiscard]] static chronos::cseg::CsegColumnDescriptor
  system(const chronos::cseg::StorageKind kind, const chronos::schema::LogicalTypeKind type_kind) {
    return {.column_id = std::nullopt,
            .storage_kind = kind,
            .logical_type = type(type_kind),
            .nullable = false,
            .event_time = false,
            .schema_ordinal = std::nullopt,
            .ordering_ordinal = std::nullopt};
  }

  [[nodiscard]] static chronos::cseg::CsegPageMetadataInput page(const std::uint64_t length) {
    return {.compression = chronos::cseg::PageCompression::kNone,
            .row_count = 64U,
            .null_count = 0U,
            .stored_length = length,
            .uncompressed_length = length,
            .validity_length = 0U,
            .offsets_length = 0U,
            .values_length = length,
            .page_crc32c = 0U};
  }

  [[nodiscard]] chronos::cseg::CsegMetadataEncodeInput input() const {
    const auto granule_count = static_cast<std::uint32_t>(granules.size());
    return {.part_id = id<chronos::cseg::PartId>(1U),
            .table_id = id<chronos::schema::TableId>(2U),
            .tablet_id = id<chronos::schema::TabletId>(3U),
            .schema_id = id<chronos::schema::SchemaId>(4U),
            .schema_version = chronos::schema::SchemaVersion::initial(),
            .row_count = static_cast<std::uint64_t>(granule_count) * 64U,
            .event_time_column_ordinal = 0U,
            .ordering_column_count = 1U,
            .minimum_event_time = 0,
            .maximum_event_time = static_cast<std::int64_t>(granule_count - 1U),
            .columns = columns,
            .granules = granules,
            .pages = pages};
  }
};

void benchmark_encode(benchmark::State& state) {
  const auto granules = static_cast<std::uint32_t>(state.range(0));
  const Fixture fixture{granules};
  std::size_t metadata_size = 0U;
  for ([[maybe_unused]] auto iteration : state) {
    auto encoded = chronos::cseg::encode_cseg_v1_metadata(fixture.input());
    if (!encoded.has_value()) {
      const std::string message = encoded.error().to_string();
      state.SkipWithError(message);
      return;
    }
    metadata_size = encoded->size();
    benchmark::DoNotOptimize(encoded->bytes().data());
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(granules) * 5);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(metadata_size));
  state.SetLabel("canonical directory planning, validation, encoding, and both CRCs; local only");
}

void benchmark_decode(benchmark::State& state) {
  const auto granules = static_cast<std::uint32_t>(state.range(0));
  const Fixture fixture{granules};
  const auto encoded = chronos::cseg::encode_cseg_v1_metadata(fixture.input()).value();
  for ([[maybe_unused]] auto iteration : state) {
    auto decoded = chronos::cseg::decode_cseg_v1_metadata_exact(encoded.bytes());
    if (!decoded.has_value()) {
      const std::string message = decoded.error().status().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(decoded->pages().data());
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(granules) * 5);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(encoded.size()));
  state.SetLabel("header/metadata CRCs plus complete structural directory validation; local only");
}

void benchmark_event_time_pruning(benchmark::State& state) {
  const auto granules = static_cast<std::uint32_t>(state.range(0));
  const Fixture fixture{granules};
  const auto encoded = chronos::cseg::encode_cseg_v1_metadata(fixture.input()).value();
  const auto decoded = chronos::cseg::decode_cseg_v1_metadata_exact(encoded.bytes()).value();
  const std::int64_t lower = static_cast<std::int64_t>(granules / 3U);
  const chronos::cseg::EventTimePredicate predicate{
      .lower = chronos::cseg::EventTimeBound{.value = lower, .inclusive = true},
      .upper = chronos::cseg::EventTimeBound{
          .value = lower + static_cast<std::int64_t>(granules / 8U), .inclusive = false}};
  std::size_t selected = 0U;
  for ([[maybe_unused]] auto iteration : state) {
    auto plan = chronos::cseg::plan_cseg_v1_event_time_pruning(decoded, predicate);
    if (!plan.has_value()) {
      const std::string message = plan.error().to_string();
      state.SkipWithError(message);
      return;
    }
    selected = plan->selected_granules().size();
    benchmark::DoNotOptimize(plan->selected_granules().data());
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(granules));
  state.counters["selected_granules"] = static_cast<double>(selected);
  state.SetLabel("authenticated metadata already decoded; owned conservative ordinal plan");
}

// Google Benchmark registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_encode)->Arg(1)->Arg(64)->Arg(4096);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_decode)->Arg(1)->Arg(64)->Arg(4096);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_event_time_pruning)->Arg(64)->Arg(4096)->Arg(65'536);

} // namespace
