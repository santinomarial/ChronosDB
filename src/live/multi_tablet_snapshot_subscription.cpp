#include "chronos/live/multi_tablet_snapshot_subscription.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/checked_math.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
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

[[nodiscard]] bool
valid_output_columns(const query::PhysicalPipelinePlan& plan,
                     const std::vector<SnapshotSubscriptionColumn>& columns) noexcept {
  if (columns.size() != plan.output_columns().size())
    return false;
  for (std::size_t ordinal = 0U; ordinal < columns.size(); ++ordinal) {
    if (columns[ordinal].name.empty() ||
        columns[ordinal].type != plan.output_columns()[ordinal].type ||
        columns[ordinal].nullable != plan.output_columns()[ordinal].nullable)
      return false;
  }
  return true;
}

[[nodiscard]] bool exact_wal_boundary(const MultiTabletSubscriptionSource& source,
                                      const MultiTabletSubscriptionMember& member,
                                      const SourcePosition& boundary,
                                      const manifest::DatabaseStorageSnapshot& snapshot) {
  const manifest::PublishedTabletStorage* tablet = snapshot.find_tablet(member.tablet_id);
  if (member.source_kind != SubscriptionSourceKind::kWal || tablet == nullptr ||
      tablet->table_id() != source.table_id || tablet->tablet_id() != member.tablet_id ||
      snapshot.wal_id() != member.wal_id ||
      !boundary.same_source(SourcePosition::wal(member.tablet_id, member.wal_id, 0U)))
    return false;
  const std::optional<head::HeadCommitPosition>& applied_position = tablet->applied_position();
  if (applied_position.has_value()) {
    const head::HeadCommitPosition& applied = *applied_position;
    return applied.source == head::CommitSource::kWal && applied.wal_id == member.wal_id &&
           applied.record_sequence == boundary.record_sequence;
  }
  for (const manifest::TabletDescriptor& durable : snapshot.durable_tablets()) {
    if (durable.tablet_id == member.tablet_id) {
      return durable.table_id == source.table_id &&
             durable.durable_record_sequence == boundary.record_sequence;
    }
  }
  return false;
}

[[nodiscard]] bool exact_boundary(const MultiTabletSubscriptionSource& source,
                                  const MultiTabletSubscriptionRegistration& registration,
                                  const manifest::DatabaseStorageSnapshot& snapshot,
                                  std::vector<schema::TabletId>& target_tablets) {
  if (snapshot.database_id().uuid() != source.database_id ||
      registration.snapshot_boundaries.size() != source.members.size())
    return false;
  target_tablets.reserve(source.members.size());
  for (std::size_t index = 0U; index < source.members.size(); ++index) {
    if (!exact_wal_boundary(source, source.members[index], registration.snapshot_boundaries[index],
                            snapshot))
      return false;
    target_tablets.push_back(source.members[index].tablet_id);
  }
  return true;
}

[[nodiscard]] bool exact_raft_boundary(const MultiTabletSubscriptionSource& source,
                                       const MultiTabletSubscriptionMember& member,
                                       const SourcePosition& boundary,
                                       const ingest::TabletSnapshot& snapshot) noexcept {
  if (member.source_kind != SubscriptionSourceKind::kRaft ||
      snapshot.table_id() != source.table_id || snapshot.tablet_id() != member.tablet_id ||
      !boundary.same_source(SourcePosition::raft(member.tablet_id, member.raft_group_id, 0U)))
    return false;
  const std::optional<head::HeadCommitPosition>& applied = snapshot.applied_position();
  if (!applied.has_value())
    return boundary.record_sequence == 0U && snapshot.visible_row_count() == 0U;
  return applied->source == head::CommitSource::kRaft &&
         applied->raft_group_id == member.raft_group_id &&
         applied->record_sequence == boundary.record_sequence;
}

[[nodiscard]] bool
exact_raft_boundary(const MultiTabletSubscriptionSource& source,
                    const MultiTabletSubscriptionRegistration& registration,
                    const std::vector<ingest::TabletSnapshot>& snapshots) noexcept {
  if (registration.snapshot_boundaries.size() != source.members.size() ||
      snapshots.size() != source.members.size())
    return false;
  for (std::size_t index = 0U; index < source.members.size(); ++index) {
    if (!exact_raft_boundary(source, source.members[index], registration.snapshot_boundaries[index],
                             snapshots[index]))
      return false;
  }
  return true;
}

} // namespace

class MultiTabletSnapshotSubscription::Impl {
public:
  enum class Phase : std::uint8_t { kSnapshot, kSnapshotEndSent, kReady, kFailed };

  Impl(MultiTabletSubscriptionManager& owner, const query::QueryResourceContext& query_resources,
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
      return common::make_unexpected(invalid("multi-tablet snapshot output shape is invalid"));
    std::vector<network::QueryResultColumn> descriptors;
    descriptors.reserve(columns.size());
    for (std::size_t ordinal = 0U; ordinal < columns.size(); ++ordinal) {
      const columnar::PhysicalColumnView* physical = chunk.column(ordinal);
      if (physical == nullptr || physical->type() != columns[ordinal].type ||
          physical->nullable() != columns[ordinal].nullable)
        return common::make_unexpected(
            invalid("multi-tablet snapshot chunk disagrees with bound output"));
      descriptors.push_back(
          {columns[ordinal].name, columns[ordinal].type, columns[ordinal].nullable});
    }
    auto cell_count = common::checked_multiply(chunk.selected_row_count(), columns.size());
    if (!cell_count.has_value())
      return common::make_unexpected(exhausted("multi-tablet snapshot cell count overflows"));
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

  MultiTabletSubscriptionManager& manager;
  const query::QueryResourceContext& resources;
  common::Uuid subscription_id;
  std::vector<std::byte> initial_token;
  std::unique_ptr<query::PhysicalOperator> pipeline;
  std::vector<SnapshotSubscriptionColumn> columns;
  SnapshotSubscriptionLimits limits;
  Phase phase{Phase::kSnapshot};
};

MultiTabletSnapshotSubscription::MultiTabletSnapshotSubscription(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

MultiTabletSnapshotSubscription::~MultiTabletSnapshotSubscription() {
  if (impl_ != nullptr)
    impl_->cancel_if_incomplete();
}

MultiTabletSnapshotSubscription::MultiTabletSnapshotSubscription(
    MultiTabletSnapshotSubscription&&) noexcept = default;
MultiTabletSnapshotSubscription&
MultiTabletSnapshotSubscription::operator=(MultiTabletSnapshotSubscription&&) noexcept = default;

common::Result<MultiTabletSnapshotSubscription> MultiTabletSnapshotSubscription::start(
    MultiTabletSubscriptionManager& manager, const SubscriptionRequest& request,
    const query::QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStoragePublisher& publisher, const schema::SchemaLineage& lineage,
    const schema::SchemaId destination_schema_id, const query::PhysicalPipelinePlan& plan,
    std::vector<SnapshotSubscriptionColumn> columns, const SnapshotSubscriptionLimits limits) {
  if (!valid_output_columns(plan, columns))
    return common::make_unexpected(
        invalid("multi-tablet snapshot columns do not match plan output"));

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
    const MultiTabletSubscriptionSource& source = manager.source();
    std::vector<schema::TabletId> tablets;
    if (destination_schema_id != request.schema_id || lineage.table_id() != source.table_id ||
        !exact_boundary(source, *registration, *snapshot, tablets)) {
      cancel();
      return common::make_unexpected(
          unavailable("multi-tablet subscription boundaries do not match storage snapshot"));
    }
    auto pipeline = query::instantiate_snapshot_tablets_pipeline(
        resources, storage, *snapshot, tablets, lineage, destination_schema_id, plan,
        limits.pipeline);
    if (!pipeline.has_value()) {
      cancel();
      return common::make_unexpected(pipeline.error());
    }
    return MultiTabletSnapshotSubscription{std::make_unique<Impl>(
        manager, resources, request.subscription_id, std::move(registration->initial_resume_token),
        std::move(*pipeline), std::move(columns), limits)};
  } catch (const std::bad_alloc&) {
    cancel();
    return common::make_unexpected(exhausted("multi-tablet snapshot allocation failed"));
  } catch (const std::length_error&) {
    cancel();
    return common::make_unexpected(exhausted("multi-tablet snapshot exceeds container limits"));
  }
}

common::Result<MultiTabletSnapshotSubscription> MultiTabletSnapshotSubscription::start_raft(
    MultiTabletSubscriptionManager& manager, const SubscriptionRequest& request,
    const query::QueryResourceContext& resources,
    const ingest::AsyncRaftTabletApplication& application, const schema::SchemaLineage& lineage,
    const schema::SchemaId destination_schema_id, const query::PhysicalPipelinePlan& plan,
    std::vector<SnapshotSubscriptionColumn> columns, const SnapshotSubscriptionLimits limits) {
  if (!valid_output_columns(plan, columns))
    return common::make_unexpected(
        invalid("Raft multi-tablet snapshot columns do not match plan output"));

  auto registration = manager.register_subscription(request);
  if (!registration.has_value())
    return common::make_unexpected(registration.error());
  const auto cancel = [&manager, &request]() noexcept { manager.abandon(request.subscription_id); };
  try {
    const MultiTabletSubscriptionSource& source = manager.source();
    if (destination_schema_id != request.schema_id || lineage.table_id() != source.table_id) {
      cancel();
      return common::make_unexpected(
          unavailable("Raft subscription plan does not match its registered source"));
    }
    std::vector<ingest::TabletSnapshot> snapshots;
    snapshots.reserve(source.members.size());
    for (const MultiTabletSubscriptionMember& member : source.members) {
      if (member.source_kind != SubscriptionSourceKind::kRaft) {
        cancel();
        return common::make_unexpected(
            invalid("Raft subscription snapshot requires a homogeneous Raft source set"));
      }
      auto snapshot = application.snapshot(member.raft_group_id);
      if (!snapshot.has_value()) {
        cancel();
        return common::make_unexpected(snapshot.error());
      }
      snapshots.push_back(std::move(*snapshot));
    }
    if (!exact_raft_boundary(source, *registration, snapshots)) {
      cancel();
      return common::make_unexpected(
          unavailable("Raft subscription boundaries do not match applied tablet snapshots"));
    }
    auto pipeline = query::instantiate_tablet_states_pipeline(
        resources, snapshots, lineage, destination_schema_id, plan, limits.raft_pipeline);
    if (!pipeline.has_value()) {
      cancel();
      return common::make_unexpected(pipeline.error());
    }
    return MultiTabletSnapshotSubscription{std::make_unique<Impl>(
        manager, resources, request.subscription_id, std::move(registration->initial_resume_token),
        std::move(*pipeline), std::move(columns), limits)};
  } catch (const std::bad_alloc&) {
    cancel();
    return common::make_unexpected(exhausted("Raft multi-tablet snapshot allocation failed"));
  } catch (const std::length_error&) {
    cancel();
    return common::make_unexpected(
        exhausted("Raft multi-tablet snapshot exceeds container limits"));
  }
}

common::Result<MultiTabletSnapshotSubscription> MultiTabletSnapshotSubscription::start_mixed(
    MultiTabletSubscriptionManager& manager, const SubscriptionRequest& request,
    const query::QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStoragePublisher& publisher,
    const ingest::AsyncRaftTabletApplication& application, const schema::SchemaLineage& lineage,
    const schema::SchemaId destination_schema_id, const query::PhysicalPipelinePlan& plan,
    std::vector<SnapshotSubscriptionColumn> columns, const SnapshotSubscriptionLimits limits) {
  if (!valid_output_columns(plan, columns))
    return common::make_unexpected(
        invalid("mixed multi-tablet snapshot columns do not match plan output"));

  auto registration = manager.register_subscription(request);
  if (!registration.has_value())
    return common::make_unexpected(registration.error());
  const auto cancel = [&manager, &request]() noexcept { manager.abandon(request.subscription_id); };
  try {
    auto wal_snapshot = publisher.snapshot();
    if (!wal_snapshot.has_value()) {
      cancel();
      return common::make_unexpected(wal_snapshot.error());
    }
    const MultiTabletSubscriptionSource& source = manager.source();
    if (destination_schema_id != request.schema_id || lineage.table_id() != source.table_id ||
        wal_snapshot->database_id().uuid() != source.database_id ||
        registration->snapshot_boundaries.size() != source.members.size()) {
      cancel();
      return common::make_unexpected(
          unavailable("mixed subscription plan or aggregate publication does not match source"));
    }

    std::size_t raft_count{};
    for (const MultiTabletSubscriptionMember& member : source.members)
      raft_count += member.source_kind == SubscriptionSourceKind::kRaft ? 1U : 0U;
    if (raft_count == 0U || raft_count == source.members.size()) {
      cancel();
      return common::make_unexpected(
          invalid("mixed subscription snapshot requires WAL and Raft sources"));
    }
    std::vector<ingest::TabletSnapshot> raft_snapshots;
    raft_snapshots.reserve(raft_count);
    for (const MultiTabletSubscriptionMember& member : source.members) {
      if (member.source_kind != SubscriptionSourceKind::kRaft)
        continue;
      auto snapshot = application.snapshot(member.raft_group_id);
      if (!snapshot.has_value()) {
        cancel();
        return common::make_unexpected(snapshot.error());
      }
      raft_snapshots.push_back(std::move(*snapshot));
    }

    std::vector<query::MixedSnapshotTabletSourceBinding> bindings;
    bindings.reserve(source.members.size());
    std::size_t raft_index{};
    for (std::size_t index = 0U; index < source.members.size(); ++index) {
      const MultiTabletSubscriptionMember& member = source.members[index];
      const SourcePosition& boundary = registration->snapshot_boundaries[index];
      if (member.source_kind == SubscriptionSourceKind::kWal) {
        if (!exact_wal_boundary(source, member, boundary, *wal_snapshot)) {
          cancel();
          return common::make_unexpected(
              unavailable("mixed subscription WAL boundary does not match aggregate publication"));
        }
        bindings.push_back({member.tablet_id, nullptr});
      } else {
        const ingest::TabletSnapshot& snapshot = raft_snapshots[raft_index++];
        if (!exact_raft_boundary(source, member, boundary, snapshot)) {
          cancel();
          return common::make_unexpected(unavailable(
              "mixed subscription Raft boundary does not match applied tablet snapshot"));
        }
        bindings.push_back({member.tablet_id, &snapshot});
      }
    }
    auto pipeline = query::instantiate_mixed_snapshot_tablets_pipeline(
        resources, storage, *wal_snapshot, bindings, lineage, destination_schema_id, plan,
        limits.pipeline, limits.raft_pipeline);
    if (!pipeline.has_value()) {
      cancel();
      return common::make_unexpected(pipeline.error());
    }
    return MultiTabletSnapshotSubscription{std::make_unique<Impl>(
        manager, resources, request.subscription_id, std::move(registration->initial_resume_token),
        std::move(*pipeline), std::move(columns), limits)};
  } catch (const std::bad_alloc&) {
    cancel();
    return common::make_unexpected(exhausted("mixed multi-tablet snapshot allocation failed"));
  } catch (const std::length_error&) {
    cancel();
    return common::make_unexpected(
        exhausted("mixed multi-tablet snapshot exceeds container limits"));
  }
}

common::Result<SnapshotSubscriptionOutput> MultiTabletSnapshotSubscription::next() {
  if (impl_->phase == Impl::Phase::kReady)
    return common::make_unexpected(invalid("multi-tablet snapshot is already ready"));
  if (impl_->phase == Impl::Phase::kFailed)
    return common::make_unexpected(unavailable("multi-tablet snapshot has failed"));
  try {
    if (impl_->phase == Impl::Phase::kSnapshotEndSent) {
      auto ready =
          network::encode_subscription_ready(impl_->initial_token, impl_->limits.subscription);
      if (!ready.has_value()) {
        impl_->cancel_if_incomplete();
        return common::make_unexpected(ready.error());
      }
      common::Status complete = impl_->manager.complete_snapshot(impl_->subscription_id);
      if (!complete.is_ok()) {
        impl_->cancel_if_incomplete();
        return common::make_unexpected(std::move(complete));
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
    return common::make_unexpected(exhausted("multi-tablet snapshot output allocation failed"));
  } catch (const std::length_error&) {
    impl_->cancel_if_incomplete();
    return common::make_unexpected(
        exhausted("multi-tablet snapshot output exceeds container limits"));
  }
}

bool MultiTabletSnapshotSubscription::ready() const noexcept {
  return impl_->phase == Impl::Phase::kReady;
}

const common::Uuid& MultiTabletSnapshotSubscription::subscription_id() const noexcept {
  return impl_->subscription_id;
}

} // namespace chronos::live
