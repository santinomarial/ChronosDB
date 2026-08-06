#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/cseg/projected_reader.hpp"
#include "chronos/cseg/validator.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <benchmark/benchmark.h>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

template <typename Integer> void append_le(std::vector<std::byte>& bytes, const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

struct Fixture {
  std::uint32_t rows;
  chronos::cseg::PartId part_id{id<chronos::cseg::PartId>(1U)};
  chronos::schema::TableId table_id{id<chronos::schema::TableId>(2U)};
  chronos::schema::TabletId tablet_id{id<chronos::schema::TabletId>(3U)};
  chronos::schema::SchemaId schema_id{id<chronos::schema::SchemaId>(4U)};
  chronos::schema::LogicalType timestamp{
      chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kTimestampNs).value()};
  chronos::schema::LogicalType uuid{
      chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kUuid).value()};
  chronos::schema::LogicalType uint64{
      chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kUInt64).value()};
  chronos::schema::LogicalType uint32{
      chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kUInt32).value()};
  chronos::schema::LogicalType uint8{
      chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kUInt8).value()};
  std::vector<chronos::cseg::CsegColumnDescriptor> columns;
  std::vector<chronos::cseg::CsegGranuleDescriptor> granules;
  std::vector<chronos::cseg::EncodedCsegPage> pages;
  chronos::cseg::EncodedCsegPart encoded;

  Fixture(const std::uint32_t row_count, const chronos::cseg::PageCompression policy)
      : rows(row_count), columns(make_columns()), granules{granule()}, pages(make_pages(policy)),
        encoded(chronos::cseg::encode_cseg_v1_part(input()).value()) {}

  [[nodiscard]] chronos::cseg::CsegPartEncodeInput input() const {
    return {.part_id = part_id,
            .table_id = table_id,
            .tablet_id = tablet_id,
            .schema_id = schema_id,
            .schema_version = chronos::schema::SchemaVersion::initial(),
            .row_count = rows,
            .event_time_column_ordinal = 0U,
            .ordering_column_count = 1U,
            .minimum_event_time = 0,
            .maximum_event_time = static_cast<std::int64_t>(rows) - 1,
            .columns = columns,
            .granules = granules,
            .pages = pages};
  }

  [[nodiscard]] chronos::schema::TableSchema schema_value() const {
    std::vector<chronos::schema::ColumnDefinition> definitions;
    definitions.push_back(chronos::schema::ColumnDefinition::create(
                              id<chronos::schema::ColumnId>(5U), "event_time", timestamp, false)
                              .value());
    return chronos::schema::TableSchema::create(
               table_id, schema_id, chronos::schema::SchemaVersion::initial(), std::nullopt,
               std::move(definitions),
               {.event_time_column = id<chronos::schema::ColumnId>(5U),
                .physical_ordering_key = {id<chronos::schema::ColumnId>(5U)},
                .partition_columns = {id<chronos::schema::ColumnId>(5U)},
                .shard_key = {id<chronos::schema::ColumnId>(5U)},
                .deduplication_key = {}})
        .value();
  }

private:
  [[nodiscard]] std::vector<chronos::cseg::CsegColumnDescriptor> make_columns() const {
    using chronos::cseg::CsegColumnDescriptor;
    using chronos::cseg::StorageKind;
    return {
        CsegColumnDescriptor{.column_id = id<chronos::schema::ColumnId>(5U),
                             .storage_kind = StorageKind::kUser,
                             .logical_type = timestamp,
                             .nullable = false,
                             .event_time = true,
                             .schema_ordinal = 0U,
                             .ordering_ordinal = 0U},
        CsegColumnDescriptor{.column_id = std::nullopt,
                             .storage_kind = StorageKind::kWalId,
                             .logical_type = uuid,
                             .nullable = false,
                             .event_time = false,
                             .schema_ordinal = std::nullopt,
                             .ordering_ordinal = std::nullopt},
        CsegColumnDescriptor{.column_id = std::nullopt,
                             .storage_kind = StorageKind::kRecordSequence,
                             .logical_type = uint64,
                             .nullable = false,
                             .event_time = false,
                             .schema_ordinal = std::nullopt,
                             .ordering_ordinal = std::nullopt},
        CsegColumnDescriptor{.column_id = std::nullopt,
                             .storage_kind = StorageKind::kRowOrdinal,
                             .logical_type = uint32,
                             .nullable = false,
                             .event_time = false,
                             .schema_ordinal = std::nullopt,
                             .ordering_ordinal = std::nullopt},
        CsegColumnDescriptor{.column_id = std::nullopt,
                             .storage_kind = StorageKind::kOperation,
                             .logical_type = uint8,
                             .nullable = false,
                             .event_time = false,
                             .schema_ordinal = std::nullopt,
                             .ordering_ordinal = std::nullopt},
    };
  }

  [[nodiscard]] chronos::cseg::CsegGranuleDescriptor granule() const {
    return {.first_row = 0U,
            .row_count = rows,
            .first_page_index = 0U,
            .minimum_event_time = 0,
            .maximum_event_time = static_cast<std::int64_t>(rows) - 1};
  }

  [[nodiscard]] chronos::cseg::EncodedCsegPage
  encode(const chronos::schema::LogicalType type, const chronos::common::ByteView values,
         const chronos::cseg::PageCompression policy) const {
    const auto physical = chronos::columnar::PhysicalColumnView::create(
        {.type = type, .nullable = false, .row_count = rows, .null_count = 0U},
        {.validity = {}, .offsets = {}, .values = values});
    return chronos::cseg::encode_cseg_v1_page(physical.value(), policy).value();
  }

  [[nodiscard]] std::vector<chronos::cseg::EncodedCsegPage>
  make_pages(const chronos::cseg::PageCompression policy) const {
    std::vector<std::byte> event_time;
    std::vector<std::byte> wal_id;
    std::vector<std::byte> sequence;
    std::vector<std::byte> ordinal;
    std::vector<std::byte> operation(rows, std::byte{1U});
    const auto wal = id<chronos::schema::SchemaId>(6U).bytes();
    for (std::uint32_t row = 0U; row < rows; ++row) {
      append_le(event_time, static_cast<std::int64_t>(row));
      wal_id.insert(wal_id.end(), wal.begin(), wal.end());
      append_le(sequence, std::uint64_t{1U});
      append_le(ordinal, row);
    }
    std::vector<chronos::cseg::EncodedCsegPage> result;
    result.reserve(5U);
    result.push_back(encode(timestamp, event_time, policy));
    result.push_back(encode(uuid, wal_id, policy));
    result.push_back(encode(uint64, sequence, policy));
    result.push_back(encode(uint32, ordinal, policy));
    result.push_back(encode(uint8, operation, policy));
    return result;
  }
};

void benchmark_part_encode(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto policy = state.range(1) == 0 ? chronos::cseg::PageCompression::kNone
                                          : chronos::cseg::PageCompression::kZstd;
  const Fixture fixture{rows, policy};
  for ([[maybe_unused]] auto iteration : state) {
    auto encoded = chronos::cseg::encode_cseg_v1_part(fixture.input());
    if (!encoded.has_value()) {
      const std::string message = encoded.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(encoded->bytes().data());
  }
  state.SetItemsProcessed(state.iterations() * rows);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(fixture.encoded.size()));
  state.SetLabel("five-page metadata composition and aligned copy; local only");
}

void benchmark_part_decode(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto policy = state.range(1) == 0 ? chronos::cseg::PageCompression::kNone
                                          : chronos::cseg::PageCompression::kZstd;
  const Fixture fixture{rows, policy};
  for ([[maybe_unused]] auto iteration : state) {
    auto decoded = chronos::cseg::decode_cseg_v1_part_exact(fixture.encoded.bytes());
    if (!decoded.has_value()) {
      const std::string message = decoded.error().status().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(decoded->metadata().pages().data());
  }
  state.SetItemsProcessed(state.iterations() * rows);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(fixture.encoded.size()));
  state.SetLabel("metadata CRC + all page CRC/decompression/PLAIN + padding; local only");
}

void benchmark_part_validate(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto policy = state.range(1) == 0 ? chronos::cseg::PageCompression::kNone
                                          : chronos::cseg::PageCompression::kZstd;
  const Fixture fixture{rows, policy};
  const auto decoded = chronos::cseg::decode_cseg_v1_part_exact(fixture.encoded.bytes());
  if (!decoded.has_value()) {
    const std::string message = decoded.error().status().to_string();
    state.SkipWithError(message);
    return;
  }
  for ([[maybe_unused]] auto iteration : state) {
    const chronos::common::Status validated =
        chronos::cseg::validate_cseg_v1_part_contents(*decoded);
    if (!validated.is_ok()) {
      const std::string message = validated.to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * rows);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(fixture.encoded.size()));
  state.SetLabel("system semantics + extrema + strict global ordering; local only");
}

void benchmark_projected_read(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto policy = state.range(1) == 0 ? chronos::cseg::PageCompression::kNone
                                          : chronos::cseg::PageCompression::kZstd;
  const Fixture fixture{rows, policy};
  chronos::schema::SchemaLineage lineage =
      chronos::schema::SchemaLineage::create(fixture.schema_value()).value();
  const auto reader = chronos::cseg::open_cseg_v1_projected_reader_exact(
      fixture.encoded.bytes(), lineage, fixture.schema_id, fixture.tablet_id);
  if (!reader.has_value()) {
    const std::string message = reader.error().status().to_string();
    state.SkipWithError(message);
    return;
  }
  const std::array<std::uint32_t, 1> event_time{0U};
  const std::span<const std::uint32_t> projection =
      std::span<const std::uint32_t>{event_time}.first(static_cast<std::size_t>(state.range(2)));
  for ([[maybe_unused]] auto iteration : state) {
    auto granule = reader->read_granule(0U, projection);
    if (!granule.has_value()) {
      const std::string message = granule.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(granule->operation().values().data());
  }
  std::uint64_t stored_bytes = 0U;
  for (std::size_t page = projection.empty() ? 1U : 0U; page < 5U; ++page) {
    stored_bytes += reader->metadata().pages()[page].stored_length;
  }
  state.SetItemsProcessed(state.iterations() * rows);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(stored_bytes));
  state.SetLabel(projection.empty()
                     ? "system-only projected granule; metadata pre-opened; local only"
                     : "one user + system pages; metadata pre-opened; local only");
}

// Google Benchmark registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_part_encode)->ArgsProduct({{64, 1024, 65536}, {0, 1}});
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_part_decode)->ArgsProduct({{64, 1024, 65536}, {0, 1}});
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_part_validate)->ArgsProduct({{64, 1024, 65536}, {0, 1}});
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_projected_read)->ArgsProduct({{64, 1024, 65536}, {0, 1}, {0, 1}});

} // namespace
