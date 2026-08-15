#include "chronos/live/snapshot_subscription.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/live/subscription_protocol.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return {common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] bool exact_boundary(const SubscriptionSource& source,
                                  const SubscriptionRegistration& registration,
                                  const manifest::DatabaseStorageSnapshot& snapshot,
                                  const manifest::PublishedTabletStorage& tablet) noexcept {
  if (snapshot.database_id().uuid() != source.database_id || snapshot.wal_id() != source.wal_id ||
      tablet.table_id() != source.table_id || tablet.tablet_id() != source.tablet_id ||
      !registration.snapshot_boundary.same_source(
          SourcePosition::wal(source.tablet_id, source.wal_id, 0U)))
    return false;
  const std::optional<head::HeadCommitPosition>& applied_position = tablet.applied_position();
  if (!applied_position.has_value()) {
    for (const manifest::TabletDescriptor& durable : snapshot.durable_tablets()) {
      if (durable.tablet_id == source.tablet_id)
        return durable.table_id == source.table_id &&
               durable.durable_record_sequence == registration.snapshot_boundary.record_sequence;
    }
    return false;
  }
  const head::HeadCommitPosition& applied = *applied_position;
  return applied.source == head::CommitSource::kWal && applied.wal_id == source.wal_id &&
         applied.record_sequence == registration.snapshot_boundary.record_sequence;
}

} // namespace

class SnapshotSubscription::Impl {
public:
  enum class Phase : std::uint8_t { kSnapshot, kSnapshotEndSent, kReady, kFailed };

  Impl(SubscriptionManager& owner, const query::QueryResourceContext& query_resources,
       common::Uuid identity, std::vector<std::byte> token,
       std::unique_ptr<query::PhysicalOperator> query_pipeline,
       std::vector<SnapshotSubscriptionColumn> output_columns, SnapshotSubscriptionLimits bounds)
      : manager(owner), resources(query_resources), subscription_id(identity),
        initial_token(std::move(token)), pipeline(std::move(query_pipeline)),
        columns(std::move(output_columns)), limits(bounds) {}

  ~Impl() {
    cancel_if_incomplete();
  }

  void cancel_if_incomplete() noexcept {
    if (phase == Phase::kSnapshot || phase == Phase::kSnapshotEndSent) {
      manager.abandon(subscription_id);
      pipeline.reset();
      phase = Phase::kFailed;
    }
  }

  [[nodiscard]] common::Result<std::vector<std::byte>>
  encode_chunk(const query::VectorChunk& chunk) const {
    if (chunk.column_count() != columns.size() ||
        chunk.selected_row_count() > std::numeric_limits<std::uint32_t>::max())
      return common::make_unexpected(invalid("snapshot output shape is invalid"));
    std::vector<network::QueryResultColumn> descriptors;
    descriptors.reserve(columns.size());
    for (std::size_t ordinal = 0U; ordinal < columns.size(); ++ordinal) {
      const columnar::PhysicalColumnView* physical = chunk.column(ordinal);
      if (physical == nullptr || physical->type() != columns[ordinal].type ||
          physical->nullable() != columns[ordinal].nullable)
        return common::make_unexpected(invalid("snapshot chunk disagrees with the bound output"));
      descriptors.push_back(
          {columns[ordinal].name, columns[ordinal].type, columns[ordinal].nullable});
    }
    auto cell_count = common::checked_multiply(chunk.selected_row_count(), columns.size());
    if (!cell_count.has_value())
      return common::make_unexpected(exhausted("snapshot result cell count overflows"));
    std::vector<network::QueryResultCell> cells;
    cells.reserve(*cell_count);
    static constexpr std::array<std::byte, 1U> kFalse{std::byte{0}};
    static constexpr std::array<std::byte, 1U> kTrue{std::byte{1}};
    for (std::size_t row = 0U; row < chunk.selected_row_count(); ++row) {
      for (std::size_t column = 0U; column < columns.size(); ++column) {
        auto cell = chunk.cell({column, row});
        if (!cell.has_value())
          return common::make_unexpected(cell.error());
        if (cell->is_null()) {
          cells.push_back({.is_null = true, .value = {}});
        } else if (cell->kind() == columnar::ColumnCellView::Kind::kBoolean) {
          auto value = cell->boolean();
          if (!value.has_value())
            return common::make_unexpected(value.error());
          cells.push_back({.value = *value ? common::ByteView{kTrue} : common::ByteView{kFalse}});
        } else {
          auto value = cell->bytes();
          if (!value.has_value())
            return common::make_unexpected(value.error());
          cells.push_back({.value = *value});
        }
      }
    }
    return network::encode_query_result_batch(
        static_cast<std::uint32_t>(chunk.selected_row_count()), descriptors, cells, limits.result);
  }

  SubscriptionManager& manager;
  const query::QueryResourceContext& resources;
  common::Uuid subscription_id;
  std::vector<std::byte> initial_token;
  std::unique_ptr<query::PhysicalOperator> pipeline;
  std::vector<SnapshotSubscriptionColumn> columns;
  SnapshotSubscriptionLimits limits;
  Phase phase{Phase::kSnapshot};
};

SnapshotSubscription::SnapshotSubscription(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SnapshotSubscription::~SnapshotSubscription() {
  if (impl_ != nullptr)
    impl_->cancel_if_incomplete();
}

SnapshotSubscription::SnapshotSubscription(SnapshotSubscription&&) noexcept = default;
SnapshotSubscription& SnapshotSubscription::operator=(SnapshotSubscription&&) noexcept = default;

common::Result<SnapshotSubscription> SnapshotSubscription::start(
    SubscriptionManager& manager, const SubscriptionRequest& request,
    const query::QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStoragePublisher& publisher, const schema::TabletId& target_tablet,
    const schema::SchemaLineage& lineage, const schema::SchemaId destination_schema_id,
    const query::PhysicalPipelinePlan& plan, std::vector<SnapshotSubscriptionColumn> columns,
    const SnapshotSubscriptionLimits limits) {
  if (columns.size() != plan.output_columns().size())
    return common::make_unexpected(invalid("snapshot column count does not match the plan output"));
  for (std::size_t ordinal = 0U; ordinal < columns.size(); ++ordinal) {
    if (columns[ordinal].name.empty() ||
        columns[ordinal].type != plan.output_columns()[ordinal].type ||
        columns[ordinal].nullable != plan.output_columns()[ordinal].nullable)
      return common::make_unexpected(invalid("snapshot column does not match the plan output"));
  }

  auto registration = manager.register_subscription(request);
  if (!registration.has_value())
    return common::make_unexpected(registration.error());
  const auto cancel = [&manager, &request]() noexcept { manager.abandon(request.subscription_id); };
  try {
    auto snapshot = publisher.snapshot();
    if (!snapshot.has_value()) {
      cancel();
      return common::make_unexpected(snapshot.error());
    }
    const manifest::PublishedTabletStorage* tablet = snapshot->find_tablet(target_tablet);
    const SubscriptionSource& source = manager.source();
    if (tablet == nullptr || target_tablet != source.tablet_id ||
        destination_schema_id != request.schema_id || lineage.table_id() != source.table_id ||
        !exact_boundary(source, *registration, *snapshot, *tablet)) {
      cancel();
      return common::make_unexpected(
          unavailable("subscription boundary does not match the acquired storage snapshot"));
    }
    auto pipeline = query::instantiate_snapshot_tablet_pipeline(
        resources, storage, *snapshot, target_tablet, lineage, destination_schema_id, plan,
        limits.pipeline);
    if (!pipeline.has_value()) {
      cancel();
      return common::make_unexpected(pipeline.error());
    }
    return SnapshotSubscription{std::make_unique<Impl>(
        manager, resources, request.subscription_id, std::move(registration->initial_resume_token),
        std::move(*pipeline), std::move(columns), limits)};
  } catch (const std::bad_alloc&) {
    cancel();
    return common::make_unexpected(exhausted("snapshot subscription allocation failed"));
  } catch (const std::length_error&) {
    cancel();
    return common::make_unexpected(exhausted("snapshot subscription exceeds container limits"));
  }
}

common::Result<SnapshotSubscriptionOutput> SnapshotSubscription::next() {
  if (impl_->phase == Impl::Phase::kReady)
    return common::make_unexpected(invalid("snapshot subscription is already ready"));
  if (impl_->phase == Impl::Phase::kFailed)
    return common::make_unexpected(unavailable("snapshot subscription has failed"));
  try {
    if (impl_->phase == Impl::Phase::kSnapshotEndSent) {
      auto ready =
          network::encode_subscription_ready(impl_->initial_token, impl_->limits.subscription);
      if (!ready.has_value()) {
        impl_->cancel_if_incomplete();
        return common::make_unexpected(ready.error());
      }
      const common::Status completed = impl_->manager.complete_snapshot(impl_->subscription_id);
      if (!completed.is_ok()) {
        impl_->cancel_if_incomplete();
        return common::make_unexpected(completed);
      }
      impl_->pipeline.reset();
      impl_->phase = Impl::Phase::kReady;
      return SnapshotSubscriptionOutput{network::MessageType::kSubscriptionReady, 0U,
                                        std::move(*ready)};
    }

    auto step = impl_->pipeline->next(impl_->resources);
    if (!step.has_value()) {
      impl_->cancel_if_incomplete();
      return common::make_unexpected(step.error());
    }
    if (step->kind() == query::PhysicalOperatorStepKind::kChunk) {
      auto payload = impl_->encode_chunk(step->chunk()->chunk());
      if (!payload.has_value()) {
        impl_->cancel_if_incomplete();
        return common::make_unexpected(payload.error());
      }
      return SnapshotSubscriptionOutput{network::MessageType::kQueryResult, 0U,
                                        std::move(*payload)};
    }

    std::vector<network::QueryResultColumn> descriptors;
    descriptors.reserve(impl_->columns.size());
    for (const SnapshotSubscriptionColumn& column : impl_->columns)
      descriptors.push_back({column.name, column.type, column.nullable});
    auto end = network::encode_query_result_batch(0U, descriptors, {}, impl_->limits.result);
    if (!end.has_value()) {
      impl_->cancel_if_incomplete();
      return common::make_unexpected(end.error());
    }
    impl_->pipeline.reset();
    impl_->phase = Impl::Phase::kSnapshotEndSent;
    return SnapshotSubscriptionOutput{network::MessageType::kQueryResult,
                                      network::kFrameFlagEndStream, std::move(*end)};
  } catch (const std::bad_alloc&) {
    impl_->cancel_if_incomplete();
    return common::make_unexpected(exhausted("snapshot subscription output allocation failed"));
  } catch (const std::length_error&) {
    impl_->cancel_if_incomplete();
    return common::make_unexpected(
        exhausted("snapshot subscription output exceeds container limits"));
  }
}

bool SnapshotSubscription::ready() const noexcept {
  return impl_->phase == Impl::Phase::kReady;
}

const common::Uuid& SnapshotSubscription::subscription_id() const noexcept {
  return impl_->subscription_id;
}

} // namespace chronos::live
