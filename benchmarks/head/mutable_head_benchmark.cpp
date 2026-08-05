#include "chronos/head/mutable_head.hpp"
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

struct Fixture {
  std::shared_ptr<const chronos::columnar::OwnedColumnarBatch> batch;
  chronos::schema::TabletId tablet_id;
  std::size_t variable_bytes;
};

[[nodiscard]] Fixture make_fixture(const std::uint32_t rows) {
  using chronos::columnar::ColumnVectorBuffers;
  using chronos::columnar::ColumnVectorMetadata;
  using chronos::columnar::OwnedColumnVector;
  using chronos::schema::ColumnDefinition;
  using chronos::schema::ColumnId;
  using chronos::schema::LogicalTypeKind;

  const ColumnId timestamp_id = id<ColumnId>(1U);
  const ColumnId string_id = id<ColumnId>(2U);
  const ColumnId boolean_id = id<ColumnId>(3U);
  std::vector<ColumnDefinition> definitions;
  definitions.push_back(
      ColumnDefinition::create(timestamp_id, "ts", type(LogicalTypeKind::kTimestampNs), false)
          .value());
  definitions.push_back(
      ColumnDefinition::create(string_id, "tag", type(LogicalTypeKind::kString), false).value());
  definitions.push_back(
      ColumnDefinition::create(boolean_id, "enabled", type(LogicalTypeKind::kBool), false).value());
  const chronos::schema::TableSchemaRoles roles{
      .event_time_column = timestamp_id,
      .physical_ordering_key = {timestamp_id},
      .partition_columns = {timestamp_id},
      .shard_key = {timestamp_id},
      .deduplication_key = {},
  };
  auto schema = std::make_shared<const chronos::schema::TableSchema>(
      chronos::schema::TableSchema::create(
          id<chronos::schema::TableId>(10U), id<chronos::schema::SchemaId>(11U),
          chronos::schema::SchemaVersion::initial(), std::nullopt, std::move(definitions), roles)
          .value());

  std::vector<OwnedColumnVector> columns;
  columns.push_back(
      OwnedColumnVector::create(ColumnVectorMetadata{.column_id = timestamp_id,
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
  const std::size_t variable_bytes = strings.size();
  columns.push_back(
      OwnedColumnVector::create(ColumnVectorMetadata{.column_id = string_id,
                                                     .type = type(LogicalTypeKind::kString),
                                                     .nullable = false,
                                                     .row_count = rows,
                                                     .null_count = 0U},
                                ColumnVectorBuffers{.validity = {},
                                                    .offsets = std::move(offsets),
                                                    .values = std::move(strings)})
          .value());
  columns.push_back(
      OwnedColumnVector::create(
          ColumnVectorMetadata{.column_id = boolean_id,
                               .type = type(LogicalTypeKind::kBool),
                               .nullable = false,
                               .row_count = rows,
                               .null_count = 0U},
          ColumnVectorBuffers{.validity = {},
                              .offsets = {},
                              .values = std::vector<std::byte>(
                                  (static_cast<std::size_t>(rows) + 7U) / 8U, std::byte{0x55})})
          .value());

  auto batch = std::make_shared<const chronos::columnar::OwnedColumnarBatch>(
      chronos::columnar::OwnedColumnarBatch::create(std::move(schema), std::move(columns)).value());
  return Fixture{.batch = std::move(batch),
                 .tablet_id = id<chronos::schema::TabletId>(12U),
                 .variable_bytes = variable_bytes};
}

[[nodiscard]] chronos::common::Result<chronos::head::MutableHead>
make_head(const Fixture& fixture) {
  return chronos::head::MutableHead::create(
      fixture.batch->schema_ptr(), fixture.tablet_id, 1U,
      chronos::head::MutableHeadCapacity{.row_capacity = fixture.batch->row_count(),
                                         .variable_value_bytes = {0U, fixture.variable_bytes, 0U}});
}

[[nodiscard]] chronos::head::HeadCommitPosition commit_position() {
  chronos::wal::WalId wal_id;
  wal_id.bytes.back() = std::byte{1U};
  return chronos::head::HeadCommitPosition{.wal_id = wal_id, .record_sequence = 1U};
}

void benchmark_publish(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const Fixture fixture = make_fixture(rows);
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto created = make_head(fixture);
    if (!created.has_value()) {
      const std::string message = created.error().to_string();
      state.SkipWithError(message);
      return;
    }
    std::optional<chronos::head::MutableHead> target{std::move(created.value())};
    state.ResumeTiming();

    auto prepared = target->prepare_append(fixture.batch, commit_position());
    if (!prepared.has_value()) {
      const std::string message = prepared.error().to_string();
      state.SkipWithError(message);
      return;
    }
    const chronos::common::Status started = prepared->mark_wal_started();
    if (!started.is_ok()) {
      const std::string message = started.to_string();
      state.SkipWithError(message);
      return;
    }
    auto published = prepared->publish();
    if (!published.has_value()) {
      const std::string message = published.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(published->row_count());
    benchmark::ClobberMemory();

    state.PauseTiming();
    target.reset();
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetBytesProcessed(state.iterations() *
                          static_cast<std::int64_t>(fixture.batch->buffer_bytes()));
  state.SetLabel(
      "prepare, materialize, and release-publish; arena allocation and WAL I/O excluded");
}

void benchmark_snapshot(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const Fixture fixture = make_fixture(rows);
  chronos::head::MutableHead target = make_head(fixture).value();
  auto prepared = target.prepare_append(fixture.batch, commit_position()).value();
  const chronos::common::Status started = prepared.mark_wal_started();
  if (!started.is_ok()) {
    const std::string message = started.to_string();
    state.SkipWithError(message);
    return;
  }
  const auto published = prepared.publish().value();
  benchmark::DoNotOptimize(published.row_count());

  for ([[maybe_unused]] auto iteration : state) {
    auto snapshot = target.snapshot();
    if (!snapshot.has_value()) {
      const std::string message = snapshot.error().to_string();
      state.SkipWithError(message);
      return;
    }
    for (std::size_t ordinal = 0U; ordinal < snapshot->column_count(); ++ordinal) {
      const auto column = snapshot->column(ordinal);
      if (!column.has_value()) {
        const std::string message = column.error().to_string();
        state.SkipWithError(message);
        return;
      }
      benchmark::DoNotOptimize(column->row_count());
    }
  }
  state.SetItemsProcessed(state.iterations());
  state.SetLabel("acquire one publication and construct all borrowed column views");
}

// Google Benchmark intentionally registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_publish)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_snapshot)->Arg(64)->Arg(1024)->Arg(65536);

} // namespace
