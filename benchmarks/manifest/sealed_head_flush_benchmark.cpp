#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/head/mutable_head.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/manifest/checkpoint_builder.hpp"
#include "chronos/manifest/generation_builder.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/sealed_head_flush.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/wal_writer.hpp"

#include <array>
#include <benchmark/benchmark.h>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
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

[[nodiscard]] head::HeadSnapshot sealed_snapshot(const std::uint32_t row_count) {
  const schema::ColumnId event_id = id<schema::ColumnId>(1U);
  const schema::LogicalType event_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value();
  std::vector<schema::ColumnDefinition> definitions;
  definitions.push_back(
      schema::ColumnDefinition::create(event_id, "event_time", event_type, false).value());
  const auto table_schema = std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(3U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(definitions),
                                  {.event_time_column = event_id,
                                   .physical_ordering_key = {event_id},
                                   .partition_columns = {event_id},
                                   .shard_key = {event_id},
                                   .deduplication_key = {}})
          .value());
  std::vector<std::byte> values;
  values.reserve(static_cast<std::size_t>(row_count) * sizeof(std::int64_t));
  for (std::uint32_t row = row_count; row > 0U; --row) {
    append_le(values, static_cast<std::int64_t>(row));
  }
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(columnar::OwnedColumnVector::create(
                        {.column_id = event_id,
                         .type = event_type,
                         .nullable = false,
                         .row_count = row_count,
                         .null_count = 0U},
                        {.validity = {}, .offsets = {}, .values = std::move(values)})
                        .value());
  const auto batch = std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(table_schema, std::move(columns)).value());
  head::MutableHead mutable_head =
      head::MutableHead::create(table_schema, id<schema::TabletId>(4U), 1U,
                                {.row_capacity = row_count, .variable_value_bytes = {0U}})
          .value();
  head::PreparedHeadAppend prepared = mutable_head.prepare_append(batch).value();
  if (!prepared.mark_wal_started().is_ok()) {
    std::terminate();
  }
  wal::WalId wal{};
  wal.bytes.front() = std::byte{5U};
  if (!prepared.publish({.wal_id = wal, .record_sequence = 1U}).has_value()) {
    std::terminate();
  }
  return mutable_head.seal().value();
}

void flush_benchmark(benchmark::State& state, const cseg::PageCompression compression) {
  const std::uint32_t rows = static_cast<std::uint32_t>(state.range(0));
  const head::HeadSnapshot snapshot = sealed_snapshot(rows);
  const cseg::PartId part = id<cseg::PartId>(6U);
  std::uint64_t encoded_bytes = 0U;
  for (auto _ : state) {
    static_cast<void>(_);
    const auto encoded =
        encode_sealed_head_v1({.snapshot = snapshot, .part_id = part, .compression = compression});
    if (!encoded.has_value()) {
      state.SkipWithError(encoded.error().to_string());
      break;
    }
    encoded_bytes = encoded->encoded_part.size();
    benchmark::DoNotOptimize(encoded->encoded_part.bytes().data());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * rows);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(encoded_bytes));
}

void flush_raw(benchmark::State& state) {
  flush_benchmark(state, cseg::PageCompression::kNone);
}

void flush_zstd(benchmark::State& state) {
  flush_benchmark(state, cseg::PageCompression::kZstd);
}

BENCHMARK(flush_raw)->Arg(1'024)->Arg(65'536);
BENCHMARK(flush_zstd)->Arg(1'024)->Arg(65'536);

void generation_builder_benchmark(benchmark::State& state) {
  const std::uint32_t rows = static_cast<std::uint32_t>(state.range(0));
  const head::HeadSnapshot snapshot = sealed_snapshot(rows);
  const auto flushed = encode_sealed_head_v1({.snapshot = snapshot,
                                              .part_id = id<cseg::PartId>(6U),
                                              .compression = cseg::PageCompression::kZstd});
  if (!flushed.has_value()) {
    state.SkipWithError(flushed.error().to_string());
    return;
  }
  const schema::SchemaLineage lineage =
      schema::SchemaLineage::create(*snapshot.schema_ptr()).value();
  wal::WalId wal_id{};
  wal_id.bytes.front() = std::byte{5U};
  const EncodedManifest predecessor_bytes =
      encode_manifest_v1(
          {.generation = 1U,
           .database_id = id<DatabaseId>(7U),
           .wal_id = wal_id,
           .reclaim_checkpoint = {.record_sequence = 0U, .segment_number = 1U, .byte_offset = 64U},
           .tablets = {},
           .parts = {},
           .retries = {}})
          .value();
  const DecodedManifestView predecessor =
      decode_manifest_v1_exact(predecessor_bytes.bytes()).value();
  ingest::Sha256Digest::Bytes digest_bytes{};
  digest_bytes.front() = std::byte{8U};
  const std::array retries{RetryDescriptor{
      .client_id = id<ingest::ClientId>(9U),
      .client_batch_id = id<ingest::ClientBatchId>(10U),
      .table_id = snapshot.table_id(),
      .tablet_id = snapshot.tablet_id(),
      .request_digest = ingest::Sha256Digest{digest_bytes},
      .wal_id = wal_id,
      .record_sequence = 1U,
      .applied_row_count = rows,
  }};
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = snapshot.tablet_id(), .lineage = std::cref(lineage)}};
  std::uint64_t encoded_bytes = 0U;
  for (auto _ : state) {
    static_cast<void>(_);
    const auto encoded = build_manifest_v1_for_sealed_head({
        .predecessor = predecessor,
        .sealed_part = *flushed,
        .new_retries = retries,
        .schema_bindings = bindings,
        .part_validation_limits = {},
    });
    if (!encoded.has_value()) {
      state.SkipWithError(encoded.error().to_string());
      break;
    }
    encoded_bytes = encoded->size();
    benchmark::DoNotOptimize(encoded->bytes().data());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * rows);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(encoded_bytes));
}

BENCHMARK(generation_builder_benchmark)->Arg(1'024)->Arg(65'536);

class BenchmarkDirectory {
public:
  BenchmarkDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-checkpoint-bench-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
    }
  }

  ~BenchmarkDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::string& path() const noexcept {
    return path_;
  }
  [[nodiscard]] bool valid() const noexcept {
    return !path_.empty();
  }

private:
  std::string path_;
};

class FixedWalIdGenerator final : public wal::WalLogIdGenerator {
public:
  explicit FixedWalIdGenerator(const wal::WalId id) noexcept : id_(id) {}

  [[nodiscard]] common::Result<wal::WalId> generate() override {
    return id_;
  }

private:
  wal::WalId id_;
};

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
checkpoint_batch(const std::shared_ptr<const schema::TableSchema>& table_schema,
                 const std::uint32_t row_count) {
  std::vector<std::byte> values;
  values.reserve(static_cast<std::size_t>(row_count) * sizeof(std::int64_t));
  for (std::uint32_t row = row_count; row > 0U; --row) {
    append_le(values, static_cast<std::int64_t>(row));
  }
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(columnar::OwnedColumnVector::create(
                        {.column_id = table_schema->columns().front().id(),
                         .type = table_schema->columns().front().type(),
                         .nullable = false,
                         .row_count = row_count,
                         .null_count = 0U},
                        {.validity = {}, .offsets = {}, .values = std::move(values)})
                        .value());
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(table_schema, std::move(columns)).value());
}

void checkpoint_builder_benchmark(benchmark::State& state) {
  const std::uint32_t rows = static_cast<std::uint32_t>(state.range(0));
  const head::HeadSnapshot snapshot = sealed_snapshot(rows);
  const std::shared_ptr<const columnar::OwnedColumnarBatch> batch =
      checkpoint_batch(snapshot.schema_ptr(), rows);
  const columnar::EncodedColumnarBatch encoded_batch =
      columnar::encode_columnar_batch_v1(*batch).value();
  const wal::EncodedApplicationPayload application =
      ingest::encode_columnar_append_v1({.client_id = id<ingest::ClientId>(9U),
                                         .client_batch_id = id<ingest::ClientBatchId>(10U),
                                         .tablet_id = snapshot.tablet_id()},
                                        encoded_batch)
          .value();
  BenchmarkDirectory directory;
  if (!directory.valid()) {
    state.SkipWithError("Temporary WAL benchmark directory creation failed");
    return;
  }
  wal::WalId wal_id{};
  wal_id.bytes.front() = std::byte{5U};
  FixedWalIdGenerator generator{wal_id};
  wal::WalWriter writer =
      wal::WalWriter::create_new({.directory_path = directory.path()}, generator).value();
  const wal::WalAppendResult appended =
      writer.append_application_entry(application.bytes()).value();
  if (!writer.synchronize().has_value() || !writer.close().is_ok()) {
    state.SkipWithError("WAL benchmark fixture did not become durable");
    return;
  }
  const auto flushed = encode_sealed_head_v1({.snapshot = snapshot,
                                              .part_id = id<cseg::PartId>(6U),
                                              .compression = cseg::PageCompression::kZstd});
  if (!flushed.has_value()) {
    state.SkipWithError(flushed.error().to_string());
    return;
  }
  const schema::SchemaLineage lineage =
      schema::SchemaLineage::create(*snapshot.schema_ptr()).value();
  const EncodedManifest predecessor_bytes =
      encode_manifest_v1(
          {.generation = 1U,
           .database_id = id<DatabaseId>(7U),
           .wal_id = wal_id,
           .reclaim_checkpoint = {.record_sequence = 0U, .segment_number = 1U, .byte_offset = 64U},
           .tablets = {},
           .parts = {},
           .retries = {}})
          .value();
  const DecodedManifestView predecessor =
      decode_manifest_v1_exact(predecessor_bytes.bytes()).value();
  const ingest::DecodedColumnarAppendView command =
      ingest::decode_columnar_append_v1_exact(application.bytes()).value();
  const std::array retries{RetryDescriptor{.client_id = command.client_id(),
                                           .client_batch_id = command.client_batch_id(),
                                           .table_id = command.table_id(),
                                           .tablet_id = command.tablet_id(),
                                           .request_digest = command.request_digest(),
                                           .wal_id = wal_id,
                                           .record_sequence = appended.record_sequence,
                                           .applied_row_count = rows}};
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = snapshot.tablet_id(), .lineage = std::cref(lineage)}};
  const EncodedManifest candidate_bytes =
      build_manifest_v1_for_sealed_head({.predecessor = predecessor,
                                         .sealed_part = *flushed,
                                         .new_retries = retries,
                                         .schema_bindings = bindings,
                                         .part_validation_limits = {}})
          .value();
  const DecodedManifestView candidate = decode_manifest_v1_exact(candidate_bytes.bytes()).value();
  const std::string file_name = part_file_name(flushed->descriptor.part_id);
  const std::array images{
      ReferencedPartImage{.file_name = file_name, .bytes = flushed->encoded_part.bytes()}};
  std::uint64_t encoded_bytes = 0U;
  for (auto _ : state) {
    static_cast<void>(_);
    const auto checkpointed =
        build_manifest_v1_checkpointed_generation({.wal_directory = directory.path(),
                                                   .predecessor = predecessor,
                                                   .candidate = candidate,
                                                   .schema_bindings = bindings,
                                                   .referenced_parts = images,
                                                   .command_decode_limits = {},
                                                   .part_validation_limits = {}});
    if (!checkpointed.has_value()) {
      state.SkipWithError(checkpointed.error().to_string());
      break;
    }
    encoded_bytes = checkpointed->encoded_manifest.size();
    benchmark::DoNotOptimize(checkpointed->encoded_manifest.bytes().data());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * rows);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(encoded_bytes));
}

BENCHMARK(checkpoint_builder_benchmark)->Arg(1'024)->Arg(65'536);

} // namespace
} // namespace chronos::manifest
