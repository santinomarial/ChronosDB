#include "chronos/columnar/column_vector.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/page_codec.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/manifest/compaction.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <benchmark/benchmark.h>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
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

template <typename Integer>
void store_little_endian(std::vector<std::byte>& bytes, const std::size_t offset,
                         const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes[offset + index] = static_cast<std::byte>(encoded >> (index * 8U));
  }
}

[[nodiscard]] cseg::EncodedCsegPage encode_page(const schema::LogicalType type,
                                                const std::uint32_t row_count,
                                                const common::ByteView values) {
  const auto physical = columnar::PhysicalColumnView::create(
      {.type = type, .nullable = false, .row_count = row_count, .null_count = 0U},
      {.validity = {}, .offsets = {}, .values = values});
  return cseg::encode_cseg_v1_page(*physical, cseg::PageCompression::kNone).value();
}

struct EquivalenceBenchmarkFixture {
  explicit EquivalenceBenchmarkFixture(const std::uint32_t row_count)
      : schema(make_schema()), sorted_rows(row_count) {
    wal_id.bytes.back() = std::byte{1U};
    for (std::uint32_t row = 0U; row < row_count; ++row) {
      sorted_rows[row] = row;
    }
    std::array<std::vector<std::uint32_t>, 4U> partitions;
    for (std::uint32_t row = 0U; row < row_count; ++row) {
      partitions[row % partitions.size()].push_back(row);
    }
    input_owners.reserve(partitions.size());
    for (std::size_t index = 0U; index < partitions.size(); ++index) {
      input_owners.push_back(make_part(id<cseg::PartId>(100U + index), partitions[index]));
    }
    output_owners.push_back(make_part(id<cseg::PartId>(1'000U), sorted_rows));
    for (std::size_t index = 0U; index < input_owners.size(); ++index) {
      inputs.push_back(
          {.part_id = id<cseg::PartId>(100U + index), .bytes = input_owners[index].bytes()});
    }
    outputs.push_back(
        {.part_id = id<cseg::PartId>(1'000U), .bytes = output_owners.front().bytes()});
  }

  [[nodiscard]] schema::TableSchema make_schema() const {
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event_id, std::string{"event_time"},
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    return schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                       std::nullopt, std::move(columns),
                                       {.event_time_column = event_id,
                                        .physical_ordering_key = {event_id},
                                        .partition_columns = {event_id},
                                        .shard_key = {event_id},
                                        .deduplication_key = {}})
        .value();
  }

  [[nodiscard]] cseg::EncodedCsegPart make_part(const cseg::PartId part_id,
                                                const std::span<const std::uint32_t> rows) const {
    const std::uint32_t count = static_cast<std::uint32_t>(rows.size());
    const auto timestamp =
        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value();
    const auto uuid = schema::LogicalType::create(schema::LogicalTypeKind::kUuid).value();
    const auto uint64 = schema::LogicalType::create(schema::LogicalTypeKind::kUInt64).value();
    const auto uint32 = schema::LogicalType::create(schema::LogicalTypeKind::kUInt32).value();
    const auto uint8 = schema::LogicalType::create(schema::LogicalTypeKind::kUInt8).value();
    std::vector<std::byte> events(rows.size() * sizeof(std::int64_t));
    std::vector<std::byte> wal(rows.size() * wal_id.bytes.size());
    std::vector<std::byte> sequences(rows.size() * sizeof(std::uint64_t));
    std::vector<std::byte> ordinals(rows.size() * sizeof(std::uint32_t), std::byte{0});
    std::vector<std::byte> operations(rows.size(), std::byte{cseg::format::kAppendRowsOperation});
    for (std::size_t index = 0U; index < rows.size(); ++index) {
      store_little_endian(events, index * sizeof(std::int64_t),
                          static_cast<std::int64_t>(rows[index]));
      std::ranges::copy(wal_id.bytes,
                        wal.begin() + static_cast<std::ptrdiff_t>(index * wal_id.bytes.size()));
      store_little_endian(sequences, index * sizeof(std::uint64_t),
                          static_cast<std::uint64_t>(rows[index]) + 1U);
    }
    std::vector<cseg::EncodedCsegPage> pages;
    pages.push_back(encode_page(timestamp, count, events));
    pages.push_back(encode_page(uuid, count, wal));
    pages.push_back(encode_page(uint64, count, sequences));
    pages.push_back(encode_page(uint32, count, ordinals));
    pages.push_back(encode_page(uint8, count, operations));
    const std::vector<cseg::CsegColumnDescriptor> columns{
        {.column_id = event_id,
         .storage_kind = cseg::StorageKind::kUser,
         .logical_type = timestamp,
         .nullable = false,
         .event_time = true,
         .schema_ordinal = 0U,
         .ordering_ordinal = 0U},
        {.column_id = std::nullopt,
         .storage_kind = cseg::StorageKind::kWalId,
         .logical_type = uuid,
         .nullable = false},
        {.column_id = std::nullopt,
         .storage_kind = cseg::StorageKind::kRecordSequence,
         .logical_type = uint64,
         .nullable = false},
        {.column_id = std::nullopt,
         .storage_kind = cseg::StorageKind::kRowOrdinal,
         .logical_type = uint32,
         .nullable = false},
        {.column_id = std::nullopt,
         .storage_kind = cseg::StorageKind::kOperation,
         .logical_type = uint8,
         .nullable = false}};
    const std::vector<cseg::CsegGranuleDescriptor> granules{{
        .first_row = 0U,
        .row_count = count,
        .first_page_index = 0U,
        .minimum_event_time = rows.front(),
        .maximum_event_time = rows.back(),
    }};
    return cseg::encode_cseg_v1_part({.part_id = part_id,
                                      .table_id = table_id,
                                      .tablet_id = tablet_id,
                                      .schema_id = schema_id,
                                      .schema_version = schema::SchemaVersion::initial(),
                                      .row_count = count,
                                      .event_time_column_ordinal = 0U,
                                      .ordering_column_count = 1U,
                                      .minimum_event_time = rows.front(),
                                      .maximum_event_time = rows.back(),
                                      .columns = columns,
                                      .granules = granules,
                                      .pages = pages})
        .value();
  }

  schema::TableId table_id{id<schema::TableId>(1U)};
  schema::TabletId tablet_id{id<schema::TabletId>(2U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(3U)};
  schema::ColumnId event_id{id<schema::ColumnId>(4U)};
  wal::WalId wal_id{};
  schema::TableSchema schema;
  std::vector<std::uint32_t> sorted_rows;
  std::vector<cseg::EncodedCsegPart> input_owners;
  std::vector<cseg::EncodedCsegPart> output_owners;
  std::vector<CompactionPartImage> inputs;
  std::vector<CompactionPartImage> outputs;
};

void benchmark_compaction_equivalence(benchmark::State& state) {
  const auto row_count = static_cast<std::uint32_t>(state.range(0));
  const EquivalenceBenchmarkFixture fixture{row_count};
  for (auto _ : state) {
    (void)_;
    common::Status equivalence = validate_append_only_cseg_v1_equivalence(
        fixture.inputs, fixture.outputs, fixture.schema, fixture.tablet_id, fixture.wal_id,
        {.max_parts_per_side = 4U, .max_rows_per_side = row_count});
    benchmark::DoNotOptimize(equivalence);
  }
  state.SetItemsProcessed(state.iterations() * row_count);
  state.SetBytesProcessed(state.iterations() *
                          static_cast<std::int64_t>(fixture.output_owners.front().size()));
  state.SetLabel("4 interleaved inputs to 1 output; full decode/validation/cell proof; local only");
}

void benchmark_compaction_merge(benchmark::State& state) {
  const auto row_count = static_cast<std::uint32_t>(state.range(0));
  const EquivalenceBenchmarkFixture fixture{row_count};
  const AppendOnlyCompactionRequest request{
      .inputs = fixture.inputs,
      .schema = std::cref(fixture.schema),
      .tablet_id = fixture.tablet_id,
      .wal_id = fixture.wal_id,
      .output_part_id = id<cseg::PartId>(1'000U),
      .compression = cseg::PageCompression::kNone,
      .limits = {.equivalence = {.max_parts_per_side = 4U, .max_rows_per_side = row_count},
                 .max_rows = row_count},
  };
  for (auto _ : state) {
    (void)_;
    common::Result<EncodedCompactionPart> merged = merge_append_only_cseg_v1(request);
    benchmark::DoNotOptimize(merged);
  }
  state.SetItemsProcessed(state.iterations() * row_count);
  state.SetBytesProcessed(state.iterations() *
                          static_cast<std::int64_t>(fixture.output_owners.front().size()));
  state.SetLabel("4 interleaved inputs to 1 output; merge plus independent proof; local only");
}

BENCHMARK(benchmark_compaction_equivalence)->Arg(1'024)->Arg(65'536);
BENCHMARK(benchmark_compaction_merge)->Arg(1'024)->Arg(65'536);

} // namespace
} // namespace chronos::manifest
