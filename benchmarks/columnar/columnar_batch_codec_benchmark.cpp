#include "chronos/columnar/columnar_batch_codec.hpp"
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

[[nodiscard]] chronos::schema::LogicalType type(const chronos::schema::LogicalTypeKind kind) {
  return chronos::schema::LogicalType::create(kind).value();
}

void append_u32(std::vector<std::byte>& bytes, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes.push_back(static_cast<std::byte>((value >> (index * 8U)) & 0xffU));
  }
}

[[nodiscard]] chronos::columnar::OwnedColumnarBatch make_batch(const std::uint32_t rows) {
  using chronos::columnar::ColumnVectorBuffers;
  using chronos::columnar::ColumnVectorMetadata;
  using chronos::columnar::OwnedColumnVector;
  using chronos::schema::ColumnDefinition;
  using chronos::schema::ColumnId;
  using chronos::schema::LogicalTypeKind;

  std::vector<ColumnDefinition> definitions;
  definitions.push_back(
      ColumnDefinition::create(id<ColumnId>(1U), "ts", type(LogicalTypeKind::kTimestampNs), false)
          .value());
  definitions.push_back(
      ColumnDefinition::create(id<ColumnId>(2U), "tag", type(LogicalTypeKind::kString), false)
          .value());
  definitions.push_back(
      ColumnDefinition::create(id<ColumnId>(3U), "value", type(LogicalTypeKind::kFloat64), false)
          .value());

  const chronos::schema::TableSchemaRoles roles{
      .event_time_column = id<ColumnId>(1U),
      .physical_ordering_key = {id<ColumnId>(1U)},
      .partition_columns = {id<ColumnId>(1U)},
      .shard_key = {id<ColumnId>(1U)},
      .deduplication_key = {},
  };
  auto schema = std::make_shared<const chronos::schema::TableSchema>(
      chronos::schema::TableSchema::create(
          id<chronos::schema::TableId>(10U), id<chronos::schema::SchemaId>(11U),
          chronos::schema::SchemaVersion::initial(), std::nullopt, std::move(definitions), roles)
          .value());

  std::vector<OwnedColumnVector> columns;
  columns.push_back(
      OwnedColumnVector::create(ColumnVectorMetadata{.column_id = id<ColumnId>(1U),
                                                     .type = type(LogicalTypeKind::kTimestampNs),
                                                     .nullable = false,
                                                     .row_count = rows,
                                                     .null_count = 0U},
                                ColumnVectorBuffers{.validity = {},
                                                    .offsets = {},
                                                    .values = std::vector<std::byte>(
                                                        static_cast<std::size_t>(rows) * 8U)})
          .value());

  std::vector<std::byte> offsets;
  std::vector<std::byte> strings;
  offsets.reserve((static_cast<std::size_t>(rows) + 1U) * sizeof(std::uint32_t));
  strings.reserve(static_cast<std::size_t>(rows) * 8U);
  append_u32(offsets, 0U);
  for (std::uint32_t row = 0U; row < rows; ++row) {
    for (std::uint32_t index = 0U; index < 8U; ++index) {
      strings.push_back(static_cast<std::byte>('a' + ((row + index) % 26U)));
    }
    append_u32(offsets, static_cast<std::uint32_t>(strings.size()));
  }
  columns.push_back(
      OwnedColumnVector::create(ColumnVectorMetadata{.column_id = id<ColumnId>(2U),
                                                     .type = type(LogicalTypeKind::kString),
                                                     .nullable = false,
                                                     .row_count = rows,
                                                     .null_count = 0U},
                                ColumnVectorBuffers{.validity = {},
                                                    .offsets = std::move(offsets),
                                                    .values = std::move(strings)})
          .value());
  columns.push_back(
      OwnedColumnVector::create(ColumnVectorMetadata{.column_id = id<ColumnId>(3U),
                                                     .type = type(LogicalTypeKind::kFloat64),
                                                     .nullable = false,
                                                     .row_count = rows,
                                                     .null_count = 0U},
                                ColumnVectorBuffers{.validity = {},
                                                    .offsets = {},
                                                    .values = std::vector<std::byte>(
                                                        static_cast<std::size_t>(rows) * 8U)})
          .value());
  return chronos::columnar::OwnedColumnarBatch::create(std::move(schema), std::move(columns))
      .value();
}

void benchmark_encode(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const chronos::columnar::OwnedColumnarBatch batch = make_batch(rows);
  std::size_t encoded_size = 0U;
  for ([[maybe_unused]] auto iteration : state) {
    auto encoded = chronos::columnar::encode_columnar_batch_v1(batch);
    if (!encoded.has_value()) {
      const std::string message = encoded.error().to_string();
      state.SkipWithError(message);
      return;
    }
    encoded_size = encoded->size();
    benchmark::DoNotOptimize(encoded->bytes().data());
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(encoded_size));
  state.SetLabel("canonical encode including both CRC32C passes; local measurement only");
}

void benchmark_decode(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const chronos::columnar::OwnedColumnarBatch batch = make_batch(rows);
  const chronos::columnar::EncodedColumnarBatch encoded =
      chronos::columnar::encode_columnar_batch_v1(batch).value();
  for ([[maybe_unused]] auto iteration : state) {
    auto decoded = chronos::columnar::decode_columnar_batch_v1_exact(encoded.bytes());
    if (!decoded.has_value()) {
      const std::string message = decoded.error().status().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(decoded->columns().data());
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(encoded.size()));
  state.SetLabel(
      "physical validation including both CRC32C checks and UTF-8; local measurement only");
}

// Google Benchmark intentionally registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_encode)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_decode)->Arg(64)->Arg(1024)->Arg(65536);

} // namespace
