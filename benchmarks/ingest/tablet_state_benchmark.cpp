#include "chronos/ingest/sealed_head_flush_queue.hpp"
#include "chronos/ingest/tablet_state.hpp"
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

template <typename Identifier> [[nodiscard]] Identifier request_id(const std::uint8_t seed) {
  chronos::common::Uuid::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    bytes[index] = static_cast<std::byte>(seed + index);
  }
  return Identifier::from_bytes(bytes).value();
}

struct TabletFixture {
  std::shared_ptr<const chronos::columnar::OwnedColumnarBatch> batch;
  std::shared_ptr<const chronos::columnar::OwnedColumnarBatch> successor_batch;
  chronos::schema::TabletId tablet_id;
};

[[nodiscard]] TabletFixture make_fixture(const std::uint32_t rows,
                                         const bool deduplication = false) {
  const auto timestamp =
      chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kTimestampNs).value();
  const chronos::schema::ColumnId timestamp_id = id<chronos::schema::ColumnId>(1U);
  std::vector<chronos::schema::ColumnDefinition> definitions;
  definitions.push_back(
      chronos::schema::ColumnDefinition::create(timestamp_id, "ts", timestamp, false).value());
  const chronos::schema::TableSchemaRoles roles{
      .event_time_column = timestamp_id,
      .physical_ordering_key = {timestamp_id},
      .partition_columns = {timestamp_id},
      .shard_key = {timestamp_id},
      .deduplication_key = deduplication ? std::vector<chronos::schema::ColumnId>{timestamp_id}
                                         : std::vector<chronos::schema::ColumnId>{}};
  auto schema = std::make_shared<const chronos::schema::TableSchema>(
      chronos::schema::TableSchema::create(
          id<chronos::schema::TableId>(80U), id<chronos::schema::SchemaId>(81U),
          chronos::schema::SchemaVersion::initial(), std::nullopt, std::move(definitions), roles)
          .value());
  const auto make_columns = [&] {
    std::vector<chronos::columnar::OwnedColumnVector> columns;
    std::vector<std::byte> values(static_cast<std::size_t>(rows) * 8U);
    for (std::uint32_t row = 0U; row < rows; ++row) {
      for (std::size_t index = 0U; index < 8U; ++index) {
        values[(static_cast<std::size_t>(row) * 8U) + index] =
            static_cast<std::byte>((static_cast<std::uint64_t>(row) >> (index * 8U)) & 0xffU);
      }
    }
    columns.push_back(chronos::columnar::OwnedColumnVector::create(
                          chronos::columnar::ColumnVectorMetadata{.column_id = timestamp_id,
                                                                  .type = timestamp,
                                                                  .nullable = false,
                                                                  .row_count = rows,
                                                                  .null_count = 0U},
                          chronos::columnar::ColumnVectorBuffers{
                              .validity = {}, .offsets = {}, .values = std::move(values)})
                          .value());
    return columns;
  };
  auto batch = std::make_shared<const chronos::columnar::OwnedColumnarBatch>(
      chronos::columnar::OwnedColumnarBatch::create(schema, make_columns()).value());

  std::vector<chronos::schema::ColumnDefinition> successor_definitions;
  successor_definitions.push_back(
      chronos::schema::ColumnDefinition::create(timestamp_id, "event_time", timestamp, false)
          .value());
  auto successor_schema = std::make_shared<const chronos::schema::TableSchema>(
      chronos::schema::TableSchema::create(schema->table_id(), id<chronos::schema::SchemaId>(83U),
                                           chronos::schema::SchemaVersion::from_value(2U).value(),
                                           schema->schema_id(), std::move(successor_definitions),
                                           roles)
          .value());
  auto successor_batch = std::make_shared<const chronos::columnar::OwnedColumnarBatch>(
      chronos::columnar::OwnedColumnarBatch::create(std::move(successor_schema), make_columns())
          .value());
  return TabletFixture{.batch = std::move(batch),
                       .successor_batch = std::move(successor_batch),
                       .tablet_id = id<chronos::schema::TabletId>(82U)};
}

[[nodiscard]] chronos::ingest::RetryIdentity retry_identity(const std::uint8_t seed) {
  return chronos::ingest::RetryIdentity{
      .client_id = request_id<chronos::ingest::ClientId>(seed),
      .client_batch_id =
          request_id<chronos::ingest::ClientBatchId>(static_cast<std::uint8_t>(seed + 32U))};
}

[[nodiscard]] chronos::ingest::ColumnarAppendMutationIdentity
mutation(const TabletFixture& fixture, const std::uint8_t digest_seed) {
  chronos::ingest::Sha256Digest::Bytes digest{};
  digest.back() = static_cast<std::byte>(digest_seed);
  return chronos::ingest::ColumnarAppendMutationIdentity{
      .table_id = fixture.batch->schema().table_id(),
      .tablet_id = fixture.tablet_id,
      .request_digest = chronos::ingest::Sha256Digest{digest}};
}

[[nodiscard]] chronos::head::HeadCommitPosition position(const std::uint64_t sequence) {
  chronos::wal::WalId wal_id;
  wal_id.bytes.back() = std::byte{1U};
  return chronos::head::HeadCommitPosition{.wal_id = wal_id, .record_sequence = sequence};
}

[[nodiscard]] chronos::common::Result<chronos::ingest::TabletState>
make_tablet(const TabletFixture& fixture, const std::uint32_t row_capacity,
            std::shared_ptr<chronos::ingest::SealedHeadFlushQueue> flush_queue = nullptr) {
  return chronos::ingest::TabletState::create(
      fixture.batch->schema_ptr(), fixture.tablet_id,
      chronos::ingest::TabletStateConfig{
          .head_capacity = chronos::head::MutableHeadCapacity{.row_capacity = row_capacity,
                                                              .variable_value_bytes = {0U}},
          .maximum_schema_versions = 2U,
          .maximum_sealed_generations = 1U,
          .maximum_retry_entries = 2U,
          .flush_queue = std::move(flush_queue)});
}

[[nodiscard]] chronos::ingest::SealedHeadFlushWork take_flush_work(
    chronos::common::Result<std::optional<chronos::ingest::SealedHeadFlushWork>> result) {
  auto work = std::move(result).value();
  // Benchmark setup checks readiness and deliberately fails by exception if that invariant breaks.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return std::move(work).value();
}

[[nodiscard]] bool publish_first(chronos::ingest::TabletState& tablet, const TabletFixture& fixture,
                                 benchmark::State& state) {
  auto prepared = tablet.prepare_append(retry_identity(1U), mutation(fixture, 1U), fixture.batch);
  if (!prepared.has_value()) {
    const std::string message = prepared.error().to_string();
    state.SkipWithError(message);
    return false;
  }
  chronos::common::Status started = prepared->mark_wal_started();
  if (!started.is_ok()) {
    const std::string message = started.to_string();
    state.SkipWithError(message);
    return false;
  }
  auto published = prepared->publish(position(1U));
  if (!published.has_value()) {
    const std::string message = published.error().to_string();
    state.SkipWithError(message);
    return false;
  }
  benchmark::DoNotOptimize(published->outcome.get());
  return true;
}

void benchmark_tablet_publish(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const TabletFixture fixture = make_fixture(rows);
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto created = make_tablet(fixture, rows);
    if (!created.has_value()) {
      const std::string message = created.error().to_string();
      state.SkipWithError(message);
      return;
    }
    std::optional<chronos::ingest::TabletState> tablet{std::move(*created)};
    state.ResumeTiming();

    if (!publish_first(*tablet, fixture, state)) {
      return;
    }
    benchmark::ClobberMemory();

    state.PauseTiming();
    tablet.reset();
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetBytesProcessed(state.iterations() *
                          static_cast<std::int64_t>(fixture.batch->buffer_bytes()));
  state.SetLabel("pre-WAL reservation, head materialization, retry entry, and outer publication");
}

void benchmark_tablet_snapshot(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const TabletFixture fixture = make_fixture(rows);
  chronos::ingest::TabletState tablet = make_tablet(fixture, rows).value();
  if (!publish_first(tablet, fixture, state)) {
    return;
  }
  for ([[maybe_unused]] auto iteration : state) {
    auto snapshot = tablet.snapshot();
    if (!snapshot.has_value()) {
      const std::string message = snapshot.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(snapshot->active_generation().row_count());
    benchmark::DoNotOptimize(snapshot->retry_entry_count());
  }
  state.SetItemsProcessed(state.iterations());
  state.SetLabel("acquire one owning outer tablet epoch and inspect active/retry boundaries");
}

void benchmark_tablet_rotation(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const TabletFixture fixture = make_fixture(rows);
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto created = make_tablet(fixture, rows);
    if (!created.has_value()) {
      const std::string message = created.error().to_string();
      state.SkipWithError(message);
      return;
    }
    std::optional<chronos::ingest::TabletState> tablet{std::move(*created)};
    if (!publish_first(*tablet, fixture, state)) {
      return;
    }
    state.ResumeTiming();

    auto prepared =
        tablet->prepare_append(retry_identity(2U), mutation(fixture, 2U), fixture.batch);
    if (!prepared.has_value()) {
      const std::string message = prepared.error().to_string();
      state.SkipWithError(message);
      return;
    }
    chronos::common::Status cancelled = prepared->cancel_before_wal();
    if (!cancelled.is_ok()) {
      const std::string message = cancelled.to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(tablet->metrics().active_generation);
    benchmark::ClobberMemory();

    state.PauseTiming();
    tablet.reset();
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetLabel("seal, allocate next bounded generation, and publish topology-only epoch");
}

void benchmark_tablet_rotation_with_flush_handoff(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const TabletFixture fixture = make_fixture(rows);
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto queue = chronos::ingest::SealedHeadFlushQueue::create({.capacity = 1U}).value();
    auto created = make_tablet(fixture, rows, queue);
    if (!created.has_value()) {
      const std::string message = created.error().to_string();
      state.SkipWithError(message);
      return;
    }
    std::optional<chronos::ingest::TabletState> tablet{std::move(*created)};
    if (!publish_first(*tablet, fixture, state)) {
      return;
    }
    state.ResumeTiming();

    auto prepared =
        tablet->prepare_append(retry_identity(2U), mutation(fixture, 2U), fixture.batch);
    if (!prepared.has_value()) {
      const std::string message = prepared.error().to_string();
      state.SkipWithError(message);
      return;
    }
    chronos::common::Status cancelled = prepared->cancel_before_wal();
    auto acquired = queue->try_acquire();
    if (!cancelled.is_ok() || !acquired.has_value() || !acquired->has_value()) {
      state.SkipWithError("flush handoff rotation or acquisition failed");
      return;
    }
    chronos::ingest::SealedHeadFlushWork work = take_flush_work(std::move(acquired));
    chronos::common::Status released = work.release_for_retry();
    if (!released.is_ok()) {
      const std::string message = released.to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::ClobberMemory();

    state.PauseTiming();
    tablet.reset();
    queue.reset();
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetLabel("reserve, seal, publish topology, hand off, acquire, and retry one queue slot");
}

void benchmark_flush_queue_acquire_retry(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const TabletFixture fixture = make_fixture(rows);
  auto queue = chronos::ingest::SealedHeadFlushQueue::create({.capacity = 1U}).value();
  chronos::ingest::TabletState tablet = make_tablet(fixture, rows, queue).value();
  if (!publish_first(tablet, fixture, state)) {
    return;
  }
  auto prepared = tablet.prepare_append(retry_identity(2U), mutation(fixture, 2U), fixture.batch);
  if (!prepared.has_value() || !prepared->cancel_before_wal().is_ok()) {
    state.SkipWithError("could not prepare the benchmark flush queue");
    return;
  }

  for ([[maybe_unused]] auto iteration : state) {
    auto acquired = queue->try_acquire();
    if (!acquired.has_value() || !acquired->has_value()) {
      state.SkipWithError("flush queue did not return its ready item");
      return;
    }
    chronos::ingest::SealedHeadFlushWork work = take_flush_work(std::move(acquired));
    benchmark::DoNotOptimize(work.snapshot());
    const chronos::common::Status released = work.release_for_retry();
    if (!released.is_ok()) {
      const std::string message = released.to_string();
      state.SkipWithError(message);
      return;
    }
  }
  state.SetItemsProcessed(state.iterations());
  state.SetLabel("single-consumer mutex acquire and retry-safe lease release");
}

void benchmark_tablet_schema_switch(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const TabletFixture fixture = make_fixture(rows);
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto created = make_tablet(fixture, rows);
    if (!created.has_value()) {
      const std::string message = created.error().to_string();
      state.SkipWithError(message);
      return;
    }
    std::optional<chronos::ingest::TabletState> tablet{std::move(*created)};
    chronos::common::Status registered = tablet->register_schema(
        fixture.successor_batch->schema_ptr(),
        chronos::head::MutableHeadCapacity{.row_capacity = rows, .variable_value_bytes = {0U}});
    if (!registered.is_ok() || !publish_first(*tablet, fixture, state)) {
      if (!registered.is_ok()) {
        const std::string message = registered.to_string();
        state.SkipWithError(message);
      }
      return;
    }
    state.ResumeTiming();

    auto prepared =
        tablet->prepare_append(retry_identity(2U), mutation(fixture, 2U), fixture.successor_batch);
    if (!prepared.has_value()) {
      const std::string message = prepared.error().to_string();
      state.SkipWithError(message);
      return;
    }
    chronos::common::Status cancelled = prepared->cancel_before_wal();
    if (!cancelled.is_ok()) {
      const std::string message = cancelled.to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(tablet->snapshot()->schema_ptr().get());
    benchmark::ClobberMemory();

    state.PauseTiming();
    tablet.reset();
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetLabel("seal ancestor, allocate registered successor generation, and publish topology");
}

void benchmark_tablet_deduplication_unique(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const TabletFixture fixture = make_fixture(rows, true);
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto created = make_tablet(fixture, rows);
    if (!created.has_value()) {
      const std::string message = created.error().to_string();
      state.SkipWithError(message);
      return;
    }
    std::optional<chronos::ingest::TabletState> tablet{std::move(*created)};
    state.ResumeTiming();

    auto prepared =
        tablet->prepare_append(retry_identity(1U), mutation(fixture, 1U), fixture.batch);
    if (!prepared.has_value()) {
      const std::string message = prepared.error().to_string();
      state.SkipWithError(message);
      return;
    }
    chronos::common::Status cancelled = prepared->cancel_before_wal();
    if (!cancelled.is_ok()) {
      const std::string message = cancelled.to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(tablet->snapshot()->visible_row_count());
    benchmark::ClobberMemory();

    state.PauseTiming();
    tablet.reset();
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetLabel("sort and validate one unique non-null TIMESTAMP_NS key before WAL");
}

void benchmark_tablet_deduplication_conflict(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const TabletFixture fixture = make_fixture(rows, true);
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto created = make_tablet(fixture, rows);
    if (!created.has_value()) {
      const std::string message = created.error().to_string();
      state.SkipWithError(message);
      return;
    }
    std::optional<chronos::ingest::TabletState> tablet{std::move(*created)};
    if (!publish_first(*tablet, fixture, state)) {
      return;
    }
    state.ResumeTiming();

    const auto rejected =
        tablet->prepare_append(retry_identity(2U), mutation(fixture, 2U), fixture.batch);
    if (rejected.has_value() ||
        rejected.error().code() != chronos::common::StatusCode::kInvalidArgument) {
      state.SkipWithError("visible logical-key conflict was not rejected");
      return;
    }
    benchmark::DoNotOptimize(rejected.error().code());
    benchmark::ClobberMemory();

    state.PauseTiming();
    tablet.reset();
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetLabel("sort incoming keys and reject the first visible-row conflict before WAL");
}

// Google Benchmark registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_tablet_publish)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_tablet_snapshot)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_tablet_rotation)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_tablet_rotation_with_flush_handoff)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_flush_queue_acquire_retry)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_tablet_schema_switch)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_tablet_deduplication_unique)->Arg(64)->Arg(1024)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_tablet_deduplication_conflict)->Arg(64)->Arg(1024)->Arg(65536);

} // namespace
