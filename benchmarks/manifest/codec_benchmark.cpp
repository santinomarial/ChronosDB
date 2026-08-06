#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/part_validation.hpp"
#include "chronos/manifest/validation.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <array>
#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
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

template <typename Identifier> [[nodiscard]] Identifier cseg_id(const std::uint8_t value) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{value};
  return Identifier::from_bytes(bytes).value();
}

struct BenchmarkManifest {
  explicit BenchmarkManifest(const std::size_t retry_count) {
    wal_id.bytes.back() = std::byte{1U};
    constexpr std::size_t part_count = 64U;
    parts.reserve(part_count);
    for (std::size_t index = 0U; index < part_count; ++index) {
      parts.push_back(PartDescriptor{.part_id = id<cseg::PartId>(index + 1U),
                                     .table_id = table_id,
                                     .tablet_id = tablet_id,
                                     .schema_id = schema_id,
                                     .schema_version = schema::SchemaVersion::initial(),
                                     .file_length = 1'208U,
                                     .row_count = 1U,
                                     .minimum_record_sequence = index + 1U,
                                     .maximum_record_sequence = index + 1U,
                                     .minimum_event_time = static_cast<std::int64_t>(index),
                                     .maximum_event_time = static_cast<std::int64_t>(index)});
    }
    tablets.push_back(TabletDescriptor{.table_id = table_id,
                                       .tablet_id = tablet_id,
                                       .recovery_schema_id = schema_id,
                                       .recovery_schema_version = schema::SchemaVersion::initial(),
                                       .durable_record_sequence = retry_count + part_count,
                                       .first_part_index = 0U,
                                       .part_count = part_count,
                                       .durable_row_count = part_count});
    retries.reserve(retry_count);
    for (std::size_t index = 0U; index < retry_count; ++index) {
      ingest::Sha256Digest::Bytes digest{};
      digest.back() = static_cast<std::byte>(static_cast<std::uint8_t>(index));
      retries.push_back(RetryDescriptor{
          .client_id = client_id,
          .client_batch_id = id<ingest::ClientBatchId>(index + 1U),
          .table_id = table_id,
          .tablet_id = tablet_id,
          .request_digest = ingest::Sha256Digest{digest},
          .wal_id = wal_id,
          .record_sequence = index + 1U,
          .applied_row_count = 1U,
      });
    }
  }

  [[nodiscard]] ManifestEncodeInput input(const std::uint64_t generation = 42U) const {
    return {
        .generation = generation,
        .database_id = database_id,
        .wal_id = wal_id,
        .reclaim_checkpoint = {.record_sequence = 1U, .segment_number = 1U, .byte_offset = 128U},
        .tablets = tablets,
        .parts = parts,
        .retries = retries};
  }

  [[nodiscard]] schema::SchemaLineage lineage() const {
    const schema::ColumnId event_id = id<schema::ColumnId>(6U);
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event_id, std::string{"event_time"},
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    schema::TableSchema table_schema =
        schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                    std::nullopt, std::move(columns),
                                    {.event_time_column = event_id,
                                     .physical_ordering_key = {event_id},
                                     .partition_columns = {event_id},
                                     .shard_key = {event_id},
                                     .deduplication_key = {}})
            .value();
    return schema::SchemaLineage::create(std::move(table_schema)).value();
  }

  DatabaseId database_id{id<DatabaseId>(1U)};
  wal::WalId wal_id{};
  schema::TableId table_id{id<schema::TableId>(2U)};
  schema::TabletId tablet_id{id<schema::TabletId>(3U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(4U)};
  ingest::ClientId client_id{id<ingest::ClientId>(5U)};
  std::vector<TabletDescriptor> tablets;
  std::vector<PartDescriptor> parts;
  std::vector<RetryDescriptor> retries;
};

void benchmark_encode(benchmark::State& state) {
  const BenchmarkManifest model{static_cast<std::size_t>(state.range(0))};
  const EncodedManifest sample = encode_manifest_v1(model.input()).value();
  for (auto _ : state) {
    (void)_;
    common::Result<EncodedManifest> encoded = encode_manifest_v1(model.input());
    benchmark::DoNotOptimize(encoded);
  }
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(sample.size()));
  state.counters["retries"] = static_cast<double>(model.retries.size());
}

void benchmark_decode(benchmark::State& state) {
  const BenchmarkManifest model{static_cast<std::size_t>(state.range(0))};
  const EncodedManifest encoded = encode_manifest_v1(model.input()).value();
  for (auto _ : state) {
    (void)_;
    ManifestDecodeResult decoded = decode_manifest_v1_exact(encoded.bytes());
    benchmark::DoNotOptimize(decoded);
  }
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(encoded.size()));
  state.counters["retries"] = static_cast<double>(model.retries.size());
}

void benchmark_transition(benchmark::State& state) {
  const BenchmarkManifest model{static_cast<std::size_t>(state.range(0))};
  const EncodedManifest predecessor_bytes = encode_manifest_v1(model.input(41U)).value();
  const EncodedManifest next_bytes = encode_manifest_v1(model.input(42U)).value();
  const DecodedManifestView predecessor =
      decode_manifest_v1_exact(predecessor_bytes.bytes()).value();
  const DecodedManifestView next = decode_manifest_v1_exact(next_bytes.bytes()).value();
  const schema::SchemaLineage lineage = model.lineage();
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = model.tablet_id, .lineage = std::cref(lineage)}};
  for (auto _ : state) {
    (void)_;
    common::Status validation = validate_manifest_v1_transition(predecessor, next, bindings);
    benchmark::DoNotOptimize(validation);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(model.retries.size() + model.parts.size()));
  state.counters["retries"] = static_cast<double>(model.retries.size());
}

void benchmark_compaction_transition(benchmark::State& state) {
  const BenchmarkManifest predecessor_model{static_cast<std::size_t>(state.range(0))};
  BenchmarkManifest next_model = predecessor_model;
  constexpr std::size_t removed_count = 32U;
  std::vector<cseg::PartId> input_ids;
  input_ids.reserve(removed_count);
  for (std::size_t index = 0U; index < removed_count; ++index) {
    input_ids.push_back(predecessor_model.parts[index].part_id);
  }
  next_model.parts.erase(next_model.parts.begin(), next_model.parts.begin() + removed_count);
  next_model.parts.push_back(PartDescriptor{
      .part_id = id<cseg::PartId>(1'000U),
      .table_id = next_model.table_id,
      .tablet_id = next_model.tablet_id,
      .schema_id = next_model.schema_id,
      .schema_version = schema::SchemaVersion::initial(),
      .file_length = 32'768U,
      .row_count = removed_count,
      .minimum_record_sequence = 1U,
      .maximum_record_sequence = removed_count,
      .minimum_event_time = 0,
      .maximum_event_time = static_cast<std::int64_t>(removed_count - 1U),
  });
  next_model.tablets.front().part_count = next_model.parts.size();
  const std::array output_ids{next_model.parts.back().part_id};
  const EncodedManifest predecessor_bytes =
      encode_manifest_v1(predecessor_model.input(41U)).value();
  const EncodedManifest next_bytes = encode_manifest_v1(next_model.input(42U)).value();
  const DecodedManifestView predecessor =
      decode_manifest_v1_exact(predecessor_bytes.bytes()).value();
  const DecodedManifestView next = decode_manifest_v1_exact(next_bytes.bytes()).value();
  const schema::SchemaLineage lineage = predecessor_model.lineage();
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = predecessor_model.tablet_id, .lineage = std::cref(lineage)}};
  const ManifestCompactionReplacement replacement{
      .tablet_id = predecessor_model.tablet_id,
      .input_part_ids = input_ids,
      .output_part_ids = output_ids,
  };
  for (auto _ : state) {
    (void)_;
    common::Status validation =
        validate_manifest_v1_compaction_transition(predecessor, next, bindings, replacement);
    benchmark::DoNotOptimize(validation);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(predecessor_model.retries.size() +
                                                    predecessor_model.parts.size() +
                                                    next_model.parts.size()));
  state.counters["retries"] = static_cast<double>(predecessor_model.retries.size());
  state.SetLabel("64 predecessor parts; 32-to-1 replacement; structural proof only");
}

void benchmark_referenced_part_validation(benchmark::State& state) {
  const cseg::EncodedCsegPart part = cseg::test::make_valid_part(cseg::PageCompression::kZstd);
  const schema::TableId table_id = cseg_id<schema::TableId>(2U);
  const schema::TabletId tablet_id = cseg_id<schema::TabletId>(3U);
  const schema::SchemaId schema_id = cseg_id<schema::SchemaId>(4U);
  const schema::ColumnId event_id = cseg_id<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_id, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  schema::SchemaLineage lineage =
      schema::SchemaLineage::create(
          schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                      std::nullopt, std::move(columns),
                                      {.event_time_column = event_id,
                                       .physical_ordering_key = {event_id},
                                       .partition_columns = {event_id},
                                       .shard_key = {event_id},
                                       .deduplication_key = {}})
              .value())
          .value();
  wal::WalId wal_id{};
  wal_id.bytes.front() = std::byte{0x70U};
  const std::array tablets{TabletDescriptor{
      .table_id = table_id,
      .tablet_id = tablet_id,
      .recovery_schema_id = schema_id,
      .recovery_schema_version = schema::SchemaVersion::initial(),
      .durable_record_sequence = 7U,
      .first_part_index = 0U,
      .part_count = 1U,
      .durable_row_count = 2U,
  }};
  const cseg::PartId part_id = cseg_id<cseg::PartId>(1U);
  const std::array parts{PartDescriptor{
      .part_id = part_id,
      .table_id = table_id,
      .tablet_id = tablet_id,
      .schema_id = schema_id,
      .schema_version = schema::SchemaVersion::initial(),
      .file_length = part.size(),
      .row_count = 2U,
      .minimum_record_sequence = 7U,
      .maximum_record_sequence = 7U,
      .minimum_event_time = -5,
      .maximum_event_time = 10,
  }};
  const EncodedManifest manifest_bytes =
      encode_manifest_v1({
                             .generation = 1U,
                             .database_id = id<DatabaseId>(1U),
                             .wal_id = wal_id,
                             .reclaim_checkpoint = {.record_sequence = 0U,
                                                    .segment_number = 1U,
                                                    .byte_offset = 64U},
                             .tablets = tablets,
                             .parts = parts,
                             .retries = {},
                         })
          .value();
  const DecodedManifestView manifest = decode_manifest_v1_exact(manifest_bytes.bytes()).value();
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  const std::string name = part_file_name(part_id);
  const std::array images{ReferencedPartImage{.file_name = name, .bytes = part.bytes()}};

  for (auto _ : state) {
    (void)_;
    common::Status validation = validate_manifest_v1_referenced_parts(manifest, bindings, images);
    benchmark::DoNotOptimize(validation);
  }
  state.SetItemsProcessed(state.iterations() * 2);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(part.size()));
  state.SetLabel("exact CSEG decode/content/schema plus manifest WAL/extrema binding; local only");
}

BENCHMARK(benchmark_encode)->Arg(16)->Arg(1'024)->Arg(4'096);
BENCHMARK(benchmark_decode)->Arg(16)->Arg(1'024)->Arg(4'096);
BENCHMARK(benchmark_transition)->Arg(16)->Arg(1'024)->Arg(4'096);
BENCHMARK(benchmark_compaction_transition)->Arg(16)->Arg(1'024)->Arg(4'096);
BENCHMARK(benchmark_referenced_part_validation);

} // namespace
} // namespace chronos::manifest
