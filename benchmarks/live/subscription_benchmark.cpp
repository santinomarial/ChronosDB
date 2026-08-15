#include "chronos/live/incremental_aggregate.hpp"
#include "chronos/live/subscription.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace {

[[nodiscard]] chronos::common::Uuid uuid(const std::uint64_t value) {
  chronos::common::Uuid::Bytes bytes{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[bytes.size() - 1U - index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
  return chronos::common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::uint64_t value) {
  return Identifier::from_uuid(uuid(value)).value();
}

struct SubscriptionFixture {
  chronos::common::Uuid database_id{uuid(1U)};
  chronos::schema::TableId table_id{identifier<chronos::schema::TableId>(2U)};
  chronos::schema::TabletId tablet_id{identifier<chronos::schema::TabletId>(3U)};
  chronos::schema::SchemaId schema_id{identifier<chronos::schema::SchemaId>(4U)};
  chronos::wal::WalId wal_id{};
  chronos::live::PlanFingerprint plan{};
  chronos::live::ResumeTokenMacKey token_key{};

  SubscriptionFixture() {
    wal_id.bytes.fill(std::byte{0x44});
    plan.fill(std::byte{0x55});
    token_key.fill(std::byte{0x66});
  }

  [[nodiscard]] chronos::live::SubscriptionSource source() const {
    return {.database_id = database_id,
            .table_id = table_id,
            .tablet_id = tablet_id,
            .wal_id = wal_id,
            .plan_fingerprint = plan,
            .schema_id = schema_id,
            .schema_version = chronos::schema::SchemaVersion::initial(),
            .token_key = token_key};
  }

  [[nodiscard]] chronos::live::SubscriptionRequest request(const std::size_t ordinal) const {
    return {.subscription_id = uuid(static_cast<std::uint64_t>(ordinal) + 10U),
            .plan_fingerprint = plan,
            .schema_id = schema_id,
            .schema_version = chronos::schema::SchemaVersion::initial()};
  }

  [[nodiscard]] chronos::live::CommittedChange change(const std::uint64_t sequence) const {
    return {.position = {tablet_id, wal_id, sequence},
            .schema_id = schema_id,
            .schema_version = chronos::schema::SchemaVersion::initial(),
            .operation = chronos::live::LogicalChangeOperation::kUpsert,
            .result_key = {std::byte{0x01}},
            .payload = {std::byte{0x02}}};
  }
};

struct LiveManagerShape {
  std::size_t fanout{};
  std::size_t buffered_changes{};
};

[[nodiscard]] chronos::common::Result<chronos::live::SubscriptionManager>
make_live_manager(const SubscriptionFixture& fixture, const LiveManagerShape shape) {
  chronos::live::SubscriptionLimits limits;
  limits.maximum_subscriptions = shape.fanout;
  limits.maximum_retained_changes = 2U;
  limits.maximum_buffered_changes_per_subscription = shape.buffered_changes;
  auto manager = chronos::live::SubscriptionManager::create(fixture.source(), limits);
  if (!manager.has_value()) {
    return chronos::common::make_unexpected(manager.error());
  }
  for (std::size_t index = 0U; index < shape.fanout; ++index) {
    const auto request = fixture.request(index);
    auto registration = manager->register_subscription(request);
    if (!registration.has_value()) {
      return chronos::common::make_unexpected(registration.error());
    }
    const chronos::common::Status completed = manager->complete_snapshot(request.subscription_id);
    if (!completed.is_ok()) {
      return chronos::common::make_unexpected(completed);
    }
  }
  return manager;
}

void skip(benchmark::State& state, const chronos::common::Status& status) {
  const std::string message = status.to_string();
  state.SkipWithError(message);
}

void subscription_handoff(benchmark::State& state) {
  const SubscriptionFixture fixture;
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    {
      auto manager = chronos::live::SubscriptionManager::create(fixture.source());
      state.ResumeTiming();
      if (!manager.has_value()) {
        skip(state, manager.error());
        return;
      }
      const auto request = fixture.request(0U);
      auto registration = manager->register_subscription(request);
      chronos::common::Status published = manager->publish_committed(fixture.change(1U));
      chronos::common::Status completed = manager->complete_snapshot(request.subscription_id);
      auto delivery = manager->poll(request.subscription_id, 1U);
      benchmark::DoNotOptimize(delivery.has_value() ? delivery->data() : nullptr);
      state.PauseTiming();
      if (!registration.has_value()) {
        skip(state, registration.error());
        return;
      }
      if (!published.is_ok()) {
        skip(state, published);
        return;
      }
      if (!completed.is_ok()) {
        skip(state, completed);
        return;
      }
      if (!delivery.has_value() || delivery->size() != 1U) {
        state.SkipWithError("handoff did not deliver the exact post-boundary change");
        return;
      }
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations());
  state.SetLabel("register + authenticated token + one snapshot-overlap publish + READY + poll");
}

void subscription_publish_fanout(benchmark::State& state) {
  const SubscriptionFixture fixture;
  const auto fanout = static_cast<std::size_t>(state.range(0));
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    {
      auto manager = make_live_manager(fixture, {.fanout = fanout, .buffered_changes = 1U});
      state.ResumeTiming();
      if (!manager.has_value()) {
        skip(state, manager.error());
        return;
      }
      const chronos::common::Status published = manager->publish_committed(fixture.change(1U));
      benchmark::DoNotOptimize(published.code());
      state.PauseTiming();
      if (!published.is_ok()) {
        skip(state, published);
        return;
      }
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(fanout));
  state.SetLabel("one committed change copied into bounded live subscriber buffers");
}

void subscription_slow_consumer_overflow(benchmark::State& state) {
  const SubscriptionFixture fixture;
  const auto fanout = static_cast<std::size_t>(state.range(0));
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    {
      auto manager = make_live_manager(fixture, {.fanout = fanout, .buffered_changes = 1U});
      if (manager.has_value()) {
        const chronos::common::Status first = manager->publish_committed(fixture.change(1U));
        if (!first.is_ok()) {
          skip(state, first);
          return;
        }
      }
      state.ResumeTiming();
      if (!manager.has_value()) {
        skip(state, manager.error());
        return;
      }
      const chronos::common::Status published = manager->publish_committed(fixture.change(2U));
      benchmark::DoNotOptimize(published.code());
      state.PauseTiming();
      if (!published.is_ok()) {
        skip(state, published);
        return;
      }
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(fanout));
  state.SetLabel("one committed change overflows stalled one-record subscriber buffers");
}

void subscription_resume_replay(benchmark::State& state) {
  const SubscriptionFixture fixture;
  const auto retained = static_cast<std::size_t>(state.range(0));
  chronos::live::SubscriptionLimits limits;
  limits.maximum_subscriptions = 1U;
  limits.maximum_retained_changes = retained;
  limits.maximum_buffered_changes_per_subscription = retained;
  auto manager = chronos::live::SubscriptionManager::create(fixture.source(), limits);
  if (!manager.has_value()) {
    skip(state, manager.error());
    return;
  }
  const auto request = fixture.request(0U);
  auto registration = manager->register_subscription(request);
  if (!registration.has_value()) {
    skip(state, registration.error());
    return;
  }
  const std::vector<std::byte> resume_token = registration->initial_resume_token;
  chronos::common::Status status = manager->complete_snapshot(request.subscription_id);
  for (std::size_t index = 0U; status.is_ok() && index < retained; ++index) {
    status = manager->publish_committed(fixture.change(index + 1U));
  }
  auto cancelled = manager->cancel(request.subscription_id);
  if (!status.is_ok()) {
    skip(state, status);
    return;
  }
  if (!cancelled.has_value()) {
    skip(state, cancelled.error());
    return;
  }

  for ([[maybe_unused]] auto iteration : state) {
    {
      auto resumed = manager->resume_subscription(resume_token);
      auto replayed = manager->poll(request.subscription_id, retained);
      benchmark::DoNotOptimize(replayed.has_value() ? replayed->data() : nullptr);
      state.PauseTiming();
      if (!resumed.has_value()) {
        skip(state, resumed.error());
        return;
      }
      if (!replayed.has_value() || replayed->size() != retained) {
        state.SkipWithError("resume did not reconstruct the exact retained suffix");
        return;
      }
    }
    cancelled = manager->cancel(request.subscription_id);
    if (!cancelled.has_value()) {
      skip(state, cancelled.error());
      return;
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(retained));
  state.SetLabel("authenticated resume + retained suffix reconstruction + bounded poll");
}

void incremental_correction_update(benchmark::State& state) {
  const auto rows = static_cast<std::size_t>(state.range(0));
  chronos::live::IncrementalAggregateSet aggregate;
  for (std::size_t index = 0U; index < rows; ++index) {
    const chronos::common::Status inserted =
        aggregate.upsert({.row_identity = index + 1U,
                          .event_time = static_cast<std::int64_t>(index),
                          .source_order = index + 1U,
                          .value = static_cast<double>(index),
                          .weight = 1.0});
    if (!inserted.is_ok()) {
      skip(state, inserted);
      return;
    }
  }
  bool late = false;
  for ([[maybe_unused]] auto iteration : state) {
    late = !late;
    const chronos::common::Status corrected =
        aggregate.upsert({.row_identity = rows / 2U + 1U,
                          .event_time = late ? -1 : static_cast<std::int64_t>(rows / 2U),
                          .source_order = rows + (late ? 1U : 2U),
                          .value = late ? -100.0 : 100.0,
                          .weight = 2.0});
    benchmark::DoNotOptimize(aggregate.snapshot().sum);
    if (!corrected.is_ok()) {
      skip(state, corrected);
      return;
    }
  }
  state.SetItemsProcessed(state.iterations());
  state.SetLabel("replacement alternates in-order and accepted late aggregate state");
}

void incremental_checkpoint_restore(benchmark::State& state) {
  const auto rows = static_cast<std::size_t>(state.range(0));
  chronos::live::IncrementalAggregateSet aggregate;
  for (std::size_t index = 0U; index < rows; ++index) {
    const chronos::common::Status inserted =
        aggregate.upsert({.row_identity = index + 1U,
                          .event_time = static_cast<std::int64_t>(index),
                          .source_order = index + 1U,
                          .value = static_cast<double>(index),
                          .weight = 1.0});
    if (!inserted.is_ok()) {
      skip(state, inserted);
      return;
    }
  }
  for ([[maybe_unused]] auto iteration : state) {
    {
      auto checkpoint = aggregate.checkpoint();
      if (!checkpoint.has_value()) {
        skip(state, checkpoint.error());
        return;
      }
      auto restored = chronos::live::IncrementalAggregateSet::restore(*checkpoint);
      benchmark::DoNotOptimize(restored.has_value() ? restored->retained_rows() : 0U);
      state.PauseTiming();
      if (!restored.has_value()) {
        skip(state, restored.error());
        return;
      }
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetLabel("exact logical aggregate checkpoint copy + index/state reconstruction");
}

// Google Benchmark intentionally registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(subscription_handoff);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(subscription_publish_fanout)->Arg(1)->Arg(8)->Arg(64);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(subscription_slow_consumer_overflow)->Arg(1)->Arg(8)->Arg(64);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(subscription_resume_replay)->Arg(1)->Arg(128)->Arg(4096);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(incremental_correction_update)->Arg(64)->Arg(4096);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(incremental_checkpoint_restore)->Arg(64)->Arg(4096);

} // namespace
