#include "chronos/head/mutable_head.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "support/counting_allocator.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
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

struct FixtureShape {
  std::uint32_t rows;
  std::uint32_t string_bytes_per_row;
};

[[nodiscard]] Fixture make_fixture(const FixtureShape shape) {
  using chronos::columnar::ColumnVectorBuffers;
  using chronos::columnar::ColumnVectorMetadata;
  using chronos::columnar::OwnedColumnVector;
  using chronos::schema::ColumnDefinition;
  using chronos::schema::ColumnId;
  using chronos::schema::LogicalTypeKind;
  const std::uint32_t rows = shape.rows;
  const std::uint32_t string_bytes_per_row = shape.string_bytes_per_row;

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
  strings.reserve(static_cast<std::size_t>(rows) * string_bytes_per_row);
  append_u32(offsets, 0U);
  for (std::uint32_t row = 0U; row < rows; ++row) {
    for (std::uint32_t index = 0U; index < string_bytes_per_row; ++index) {
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

[[nodiscard]] chronos::benchmark_support::AllocationCounts
measure_publish_allocations(const Fixture& fixture) {
  chronos::head::MutableHead target = make_head(fixture).value();
  chronos::benchmark_support::ScopedAllocationCounting counting;
  auto prepared = target.prepare_append(fixture.batch).value();
  static_cast<void>(prepared.mark_wal_started());
  static_cast<void>(prepared.publish(commit_position()));
  return counting.stop();
}

void benchmark_publish(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto string_bytes = static_cast<std::uint32_t>(state.range(1));
  const Fixture fixture =
      make_fixture(FixtureShape{.rows = rows, .string_bytes_per_row = string_bytes});
  const chronos::head::MutableHeadMetrics memory = make_head(fixture)->metrics();
  const chronos::benchmark_support::AllocationCounts allocations =
      measure_publish_allocations(fixture);
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

    auto prepared = target->prepare_append(fixture.batch);
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
    auto published = prepared->publish(commit_position());
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
  state.counters["logical_batch_bytes"] = static_cast<double>(fixture.batch->buffer_bytes());
  state.counters["retained_head_bytes"] = static_cast<double>(memory.retained_storage_bytes);
  state.counters["allocs_per_publish"] = static_cast<double>(allocations.allocations);
  state.counters["allocated_bytes_per_publish"] = static_cast<double>(allocations.allocated_bytes);
  state.SetLabel(
      "prepare, materialize, and release-publish; arena allocation and WAL I/O excluded");
}

void benchmark_snapshot(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const Fixture fixture = make_fixture(FixtureShape{.rows = rows, .string_bytes_per_row = 8U});
  chronos::head::MutableHead target = make_head(fixture).value();
  auto prepared = target.prepare_append(fixture.batch).value();
  const chronos::common::Status started = prepared.mark_wal_started();
  if (!started.is_ok()) {
    const std::string message = started.to_string();
    state.SkipWithError(message);
    return;
  }
  const auto published = prepared.publish(commit_position()).value();
  benchmark::DoNotOptimize(published.row_count());
  chronos::benchmark_support::ScopedAllocationCounting counting;
  auto allocation_snapshot = target.snapshot().value();
  for (std::size_t ordinal = 0U; ordinal < allocation_snapshot.column_count(); ++ordinal) {
    benchmark::DoNotOptimize(allocation_snapshot.column(ordinal).value().row_count());
  }
  const chronos::benchmark_support::AllocationCounts allocations = counting.stop();

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
  state.counters["allocs_per_snapshot"] = static_cast<double>(allocations.allocations);
  state.counters["allocated_bytes_per_snapshot"] = static_cast<double>(allocations.allocated_bytes);
  state.SetLabel("acquire one publication and construct all borrowed column views");
}

[[nodiscard]] std::uint64_t scan_checksum(const chronos::head::HeadColumnView& timestamps,
                                          const chronos::head::HeadColumnView& strings,
                                          const chronos::head::HeadColumnView& booleans) {
  std::uint64_t checksum = 0U;
  for (const std::byte value : timestamps.fixed_values()) {
    checksum += std::to_integer<std::uint8_t>(value);
  }
  for (std::uint32_t row = 0U; row < timestamps.row_count(); ++row) {
    checksum += strings.variable_offsets()[row + 1U] - strings.variable_offsets()[row];
    checksum += booleans.boolean_values()[row];
  }
  for (const std::byte value : strings.variable_values()) {
    checksum += std::to_integer<std::uint8_t>(value);
  }
  return checksum;
}

[[nodiscard]] std::size_t scanned_bytes(const chronos::head::HeadColumnView& timestamps,
                                        const chronos::head::HeadColumnView& strings,
                                        const chronos::head::HeadColumnView& booleans) {
  return timestamps.fixed_values().size() + strings.variable_values().size() +
         (static_cast<std::size_t>(timestamps.row_count()) * 2U * sizeof(std::uint32_t)) +
         booleans.boolean_values().size();
}

void benchmark_scan(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto string_bytes = static_cast<std::uint32_t>(state.range(1));
  const Fixture fixture =
      make_fixture(FixtureShape{.rows = rows, .string_bytes_per_row = string_bytes});
  chronos::head::MutableHead target = make_head(fixture).value();
  auto prepared = target.prepare_append(fixture.batch).value();
  if (!prepared.mark_wal_started().is_ok()) {
    state.SkipWithError("failed to cross the benchmark WAL boundary");
    return;
  }
  const auto snapshot = prepared.publish(commit_position()).value();
  const auto timestamps = snapshot.column(0U).value();
  const auto strings = snapshot.column(1U).value();
  const auto booleans = snapshot.column(2U).value();
  std::uint64_t expected = static_cast<std::uint64_t>(fixture.variable_bytes) + ((rows + 1U) / 2U);
  for (const std::byte value : strings.variable_values()) {
    expected += std::to_integer<std::uint8_t>(value);
  }

  chronos::benchmark_support::ScopedAllocationCounting counting;
  const std::uint64_t allocation_probe = scan_checksum(timestamps, strings, booleans);
  const chronos::benchmark_support::AllocationCounts allocations = counting.stop();
  if (allocation_probe != expected) {
    state.SkipWithError("allocation probe scan produced an unexpected checksum");
    return;
  }

  for ([[maybe_unused]] auto iteration : state) {
    std::uint64_t checksum = scan_checksum(timestamps, strings, booleans);
    if (checksum != expected) {
      state.SkipWithError("borrowed mutable-head scan produced an unexpected checksum");
      return;
    }
    benchmark::DoNotOptimize(checksum);
  }
  const std::size_t bytes = scanned_bytes(timestamps, strings, booleans);
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(bytes));
  state.counters["retained_head_bytes"] =
      static_cast<double>(target.metrics().retained_storage_bytes);
  state.counters["allocs_per_scan"] = static_cast<double>(allocations.allocations);
  state.counters["allocated_bytes_per_scan"] = static_cast<double>(allocations.allocated_bytes);
  state.SetLabel("scan fixed, variable-offset, variable-value, and Boolean head storage");
}

struct SharedScanFixture {
  std::mutex mutex;
  std::shared_ptr<const chronos::head::HeadSnapshot> snapshot;
  std::size_t users{};
  std::uint64_t expected{};
  std::size_t bytes{};
};

SharedScanFixture shared_scan_fixture;

struct SharedScanLease {
  std::shared_ptr<const chronos::head::HeadSnapshot> snapshot;
  chronos::head::HeadColumnView timestamps;
  chronos::head::HeadColumnView strings;
  chronos::head::HeadColumnView booleans;
  std::uint64_t expected;
  std::size_t bytes;
};

[[nodiscard]] SharedScanLease acquire_shared_scan(const FixtureShape shape) {
  std::lock_guard lock{shared_scan_fixture.mutex};
  if (shared_scan_fixture.users == 0U) {
    const Fixture fixture = make_fixture(shape);
    chronos::head::MutableHead target = make_head(fixture).value();
    auto prepared = target.prepare_append(fixture.batch).value();
    static_cast<void>(prepared.mark_wal_started());
    auto published = prepared.publish(commit_position()).value();
    shared_scan_fixture.snapshot =
        std::make_shared<const chronos::head::HeadSnapshot>(std::move(published));
    const auto timestamps = shared_scan_fixture.snapshot->column(0U).value();
    const auto strings = shared_scan_fixture.snapshot->column(1U).value();
    const auto booleans = shared_scan_fixture.snapshot->column(2U).value();
    shared_scan_fixture.expected = scan_checksum(timestamps, strings, booleans);
    shared_scan_fixture.bytes = scanned_bytes(timestamps, strings, booleans);
  }
  ++shared_scan_fixture.users;
  return SharedScanLease{.snapshot = shared_scan_fixture.snapshot,
                         .timestamps = shared_scan_fixture.snapshot->column(0U).value(),
                         .strings = shared_scan_fixture.snapshot->column(1U).value(),
                         .booleans = shared_scan_fixture.snapshot->column(2U).value(),
                         .expected = shared_scan_fixture.expected,
                         .bytes = shared_scan_fixture.bytes};
}

void release_shared_scan() {
  std::lock_guard lock{shared_scan_fixture.mutex};
  --shared_scan_fixture.users;
  if (shared_scan_fixture.users == 0U) {
    shared_scan_fixture.snapshot.reset();
  }
}

void benchmark_concurrent_scan(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto string_bytes = static_cast<std::uint32_t>(state.range(1));
  const SharedScanLease lease =
      acquire_shared_scan(FixtureShape{.rows = rows, .string_bytes_per_row = string_bytes});
  bool valid = true;
  for ([[maybe_unused]] auto iteration : state) {
    std::uint64_t checksum = scan_checksum(lease.timestamps, lease.strings, lease.booleans);
    if (checksum != lease.expected) {
      valid = false;
      break;
    }
    benchmark::DoNotOptimize(checksum);
  }
  if (!valid) {
    state.SkipWithError("concurrent borrowed scan produced an unexpected checksum");
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(lease.bytes));
  state.SetLabel("concurrent readers scan the same immutable published head buffers");
  release_shared_scan();
}

void benchmark_seal(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const Fixture fixture = make_fixture(FixtureShape{.rows = rows, .string_bytes_per_row = 8U});
  auto allocation_target = make_head(fixture).value();
  auto allocation_prepared = allocation_target.prepare_append(fixture.batch).value();
  static_cast<void>(allocation_prepared.mark_wal_started());
  static_cast<void>(allocation_prepared.publish(commit_position()));
  chronos::benchmark_support::ScopedAllocationCounting counting;
  auto allocation_sealed = allocation_target.seal().value();
  const chronos::benchmark_support::AllocationCounts allocations = counting.stop();
  benchmark::DoNotOptimize(allocation_sealed.row_count());
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto target = make_head(fixture).value();
    auto prepared = target.prepare_append(fixture.batch).value();
    if (!prepared.mark_wal_started().is_ok() || !prepared.publish(commit_position()).has_value()) {
      state.SkipWithError("failed to publish the benchmark generation");
      return;
    }
    state.ResumeTiming();

    auto sealed = target.seal();
    if (!sealed.has_value() || sealed->row_count() != rows) {
      state.SkipWithError("mutable-head seal did not preserve the published boundary");
      return;
    }
    benchmark::DoNotOptimize(sealed->row_count());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations());
  state.counters["allocs_per_seal"] = static_cast<double>(allocations.allocations);
  state.counters["allocated_bytes_per_seal"] = static_cast<double>(allocations.allocated_bytes);
  state.SetLabel("seal one published generation and acquire its owning boundary");
}

// Google Benchmark intentionally registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_publish)
    ->Args({64, 0})
    ->Args({64, 8})
    ->Args({64, 64})
    ->Args({1024, 0})
    ->Args({1024, 8})
    ->Args({1024, 64})
    ->Args({65536, 0})
    ->Args({65536, 8})
    ->Args({65536, 64});
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_snapshot)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_scan)
    ->Args({64, 0})
    ->Args({64, 8})
    ->Args({64, 64})
    ->Args({1024, 0})
    ->Args({1024, 8})
    ->Args({1024, 64})
    ->Args({65536, 0})
    ->Args({65536, 8})
    ->Args({65536, 64});
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_seal)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_concurrent_scan)
    ->Args({1024, 8})
    ->Args({1024, 64})
    ->Args({65536, 8})
    ->Args({65536, 64})
    ->Threads(1)
    ->Threads(2)
    ->Threads(4);

} // namespace
