#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint16_t value) {
  chronos::common::Uuid::Bytes bytes{};
  bytes[14] = static_cast<std::byte>((value >> 8U) & 0xffU);
  bytes[15] = static_cast<std::byte>(value & 0xffU);
  return Identifier::from_bytes(bytes).value();
}

template <typename Identifier> [[nodiscard]] Identifier request_id(const std::uint8_t first) {
  chronos::common::Uuid::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    bytes[index] = static_cast<std::byte>(first + index);
  }
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] chronos::columnar::EncodedColumnarBatch encoded_batch(const std::uint32_t rows) {
  const auto timestamp =
      chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kTimestampNs).value();
  std::vector<chronos::schema::ColumnDefinition> definitions;
  definitions.push_back(chronos::schema::ColumnDefinition::create(id<chronos::schema::ColumnId>(1U),
                                                                  "ts", timestamp, false)
                            .value());
  const chronos::schema::ColumnId event_time = id<chronos::schema::ColumnId>(1U);
  chronos::schema::TableSchemaRoles roles{.event_time_column = event_time,
                                          .physical_ordering_key = {event_time},
                                          .partition_columns = {event_time},
                                          .shard_key = {event_time},
                                          .deduplication_key = {}};
  auto schema = std::make_shared<const chronos::schema::TableSchema>(
      chronos::schema::TableSchema::create(id<chronos::schema::TableId>(50U),
                                           id<chronos::schema::SchemaId>(51U),
                                           chronos::schema::SchemaVersion::initial(), std::nullopt,
                                           std::move(definitions), std::move(roles))
          .value());
  std::vector<chronos::columnar::OwnedColumnVector> columns;
  columns.push_back(chronos::columnar::OwnedColumnVector::create(
                        chronos::columnar::ColumnVectorMetadata{.column_id = event_time,
                                                                .type = timestamp,
                                                                .nullable = false,
                                                                .row_count = rows,
                                                                .null_count = 0U},
                        chronos::columnar::ColumnVectorBuffers{
                            .validity = {},
                            .offsets = {},
                            .values = std::vector<std::byte>(static_cast<std::size_t>(rows) * 8U)})
                        .value());
  const auto batch =
      chronos::columnar::OwnedColumnarBatch::create(std::move(schema), std::move(columns)).value();
  return chronos::columnar::encode_columnar_batch_v1(batch).value();
}

[[nodiscard]] chronos::ingest::ColumnarAppendEncodeInput command_input() {
  return {.client_id = request_id<chronos::ingest::ClientId>(0x10U),
          .client_batch_id = request_id<chronos::ingest::ClientBatchId>(0x20U),
          .tablet_id = id<chronos::schema::TabletId>(52U)};
}

void benchmark_digest(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const chronos::columnar::EncodedColumnarBatch batch = encoded_batch(rows);
  const auto input = chronos::ingest::ColumnarAppendDigestInput{
      .table_id = id<chronos::schema::TableId>(50U),
      .tablet_id = id<chronos::schema::TabletId>(52U),
      .schema_id = id<chronos::schema::SchemaId>(51U),
      .schema_version = chronos::schema::SchemaVersion::initial(),
      .encoded_batch = batch.bytes()};
  for ([[maybe_unused]] auto iteration : state) {
    auto digest = chronos::ingest::compute_columnar_append_v1_request_digest(input);
    if (!digest.has_value()) {
      const std::string message = digest.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(digest->bytes().data());
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(batch.size()));
  state.SetLabel("OpenSSL 3 provider SHA-256 over canonical request preimage; local only");
}

void benchmark_encode(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const chronos::columnar::EncodedColumnarBatch batch = encoded_batch(rows);
  const auto input = command_input();
  for ([[maybe_unused]] auto iteration : state) {
    auto encoded = chronos::ingest::encode_columnar_append_v1(input, batch);
    if (!encoded.has_value()) {
      const std::string message = encoded.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(encoded->bytes().data());
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(batch.size()));
  state.SetLabel("batch revalidation, digest, header, and owned payload copy; local only");
}

void benchmark_decode(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const chronos::columnar::EncodedColumnarBatch batch = encoded_batch(rows);
  const auto payload = chronos::ingest::encode_columnar_append_v1(command_input(), batch).value();
  for ([[maybe_unused]] auto iteration : state) {
    auto decoded = chronos::ingest::decode_columnar_append_v1_exact(payload.bytes());
    if (!decoded.has_value()) {
      const std::string message = decoded.error().status().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(decoded->batch().columns().data());
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(payload.size()));
  state.SetLabel("command, nested batch integrity, metadata, and digest validation; local only");
}

// Google Benchmark registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_digest)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_encode)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_decode)->Arg(64)->Arg(1024)->Arg(65536);

} // namespace
