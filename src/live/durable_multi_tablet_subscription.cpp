#include "chronos/live/durable_multi_tablet_subscription.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
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

[[nodiscard]] common::Status with_context(const std::string_view context,
                                          const common::Status& status) {
  std::string message{context};
  message.append(": ");
  message.append(status.message());
  return {status.code(), std::move(message)};
}

[[nodiscard]] bool canonicalize_and_match(DurableMultiTabletSubscriptionConfig& config) {
  std::ranges::sort(config.source.members, {}, &MultiTabletSubscriptionMember::tablet_id);
  const auto& identity = config.storage.identity;
  if (config.source.database_id != identity.database_id ||
      config.source.table_id != identity.table_id ||
      config.source.plan_fingerprint != identity.plan_fingerprint ||
      config.source.schema_id != identity.schema_id ||
      config.source.schema_version != identity.schema_version ||
      config.source.members.size() != identity.sources.size())
    return false;
  for (std::size_t index = 0U; index < identity.sources.size(); ++index) {
    if (config.source.members[index].tablet_id != identity.sources[index].tablet_id ||
        config.source.members[index].wal_id != identity.sources[index].wal_id)
      return false;
  }
  return true;
}

[[nodiscard]] std::vector<SourcePosition>
retention_frontiers(const MultiTabletSubscriptionCheckpoint& checkpoint) {
  std::vector<SourcePosition> frontiers;
  frontiers.reserve(checkpoint.sources.size());
  for (const MultiTabletSubscriptionCheckpointSource& source : checkpoint.sources) {
    frontiers.push_back({source.latest_position.tablet_id, source.latest_position.wal_id,
                         source.expired_through_sequence});
  }
  return frontiers;
}

void set_restore_boundaries(MultiTabletSubscriptionSource& source,
                            const MultiTabletSubscriptionCheckpoint& checkpoint) {
  for (std::size_t index = 0U; index < checkpoint.sources.size(); ++index) {
    source.members[index].committed_record_sequence =
        checkpoint.sources[index].latest_position.record_sequence;
  }
}

} // namespace

class DurableMultiTabletSubscription::Impl {
public:
  Impl(MultiTabletSubscriptionManager configured_manager,
       MultiTabletSubscriptionCheckpointStorage configured_storage,
       const std::uint64_t configured_generation,
       std::optional<std::vector<SourcePosition>> configured_frontiers,
       const bool configured_dirty) noexcept
      : manager(std::move(configured_manager)), storage(std::move(configured_storage)),
        generation(configured_generation), durable_frontiers(std::move(configured_frontiers)),
        dirty(configured_dirty) {}

  MultiTabletSubscriptionManager manager;
  MultiTabletSubscriptionCheckpointStorage storage;
  std::uint64_t generation{};
  std::optional<std::vector<SourcePosition>> durable_frontiers;
  bool dirty{true};
};

DurableMultiTabletSubscription::DurableMultiTabletSubscription(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
DurableMultiTabletSubscription::~DurableMultiTabletSubscription() = default;
DurableMultiTabletSubscription::DurableMultiTabletSubscription(
    DurableMultiTabletSubscription&&) noexcept = default;
DurableMultiTabletSubscription&
DurableMultiTabletSubscription::operator=(DurableMultiTabletSubscription&&) noexcept = default;

common::Result<DurableMultiTabletSubscription>
DurableMultiTabletSubscription::create_new(DurableMultiTabletSubscriptionConfig config) {
  try {
    if (!canonicalize_and_match(config))
      return common::make_unexpected(
          invalid("durable subscription source and storage identity disagree"));
    auto manager = MultiTabletSubscriptionManager::create(std::move(config.source), config.limits);
    if (!manager.has_value())
      return common::make_unexpected(manager.error());
    auto storage = MultiTabletSubscriptionCheckpointStorage::create(std::move(config.storage));
    if (!storage.has_value())
      return common::make_unexpected(storage.error());
    auto latest = storage->load_latest();
    if (!latest.has_value())
      return common::make_unexpected(latest.error());
    if (latest->has_value())
      return common::make_unexpected(
          common::Status{common::StatusCode::kAlreadyExists,
                         "new durable subscription directory already contains a checkpoint"});
    return DurableMultiTabletSubscription{
        std::make_unique<Impl>(std::move(*manager), std::move(*storage), 0U, std::nullopt, true)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("durable subscription allocation failed"));
  }
}

common::Result<DurableMultiTabletSubscription>
DurableMultiTabletSubscription::open_existing(DurableMultiTabletSubscriptionConfig config) {
  try {
    if (!canonicalize_and_match(config))
      return common::make_unexpected(
          invalid("durable subscription source and storage identity disagree"));
    auto storage =
        MultiTabletSubscriptionCheckpointStorage::open_existing(std::move(config.storage));
    if (!storage.has_value())
      return common::make_unexpected(storage.error());
    auto latest = storage->load_latest();
    if (!latest.has_value())
      return common::make_unexpected(latest.error());
    if (!latest->has_value()) {
      auto manager =
          MultiTabletSubscriptionManager::create(std::move(config.source), config.limits);
      if (!manager.has_value())
        return common::make_unexpected(manager.error());
      return DurableMultiTabletSubscription{
          std::make_unique<Impl>(std::move(*manager), std::move(*storage), 0U, std::nullopt, true)};
    }

    BoundMultiTabletSubscriptionCheckpoint durable = std::move((*latest)->checkpoint);
    set_restore_boundaries(config.source, durable.state);
    auto frontiers = retention_frontiers(durable.state);
    auto manager = MultiTabletSubscriptionManager::restore(std::move(config.source), durable.state,
                                                           config.limits);
    if (!manager.has_value())
      return common::make_unexpected(with_context(
          "durable subscription checkpoint cannot restore its coordinator", manager.error()));
    return DurableMultiTabletSubscription{
        std::make_unique<Impl>(std::move(*manager), std::move(*storage),
                               durable.checkpoint_generation, std::move(frontiers), false)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("durable subscription recovery allocation failed"));
  }
}

common::Result<MultiTabletSubscriptionRegistration>
DurableMultiTabletSubscription::register_subscription(const SubscriptionRequest& request) {
  return impl_->manager.register_subscription(request);
}

common::Result<MultiTabletSubscriptionRegistration>
DurableMultiTabletSubscription::resume_subscription(const common::ByteView encoded_token) {
  return impl_->manager.resume_subscription(encoded_token);
}

common::Result<MultiTabletSubscriptionRegistration>
DurableMultiTabletSubscription::resume_subscription(const common::Uuid& expected_subscription_id,
                                                    const common::ByteView encoded_token) {
  return impl_->manager.resume_subscription(expected_subscription_id, encoded_token);
}

common::Status
DurableMultiTabletSubscription::complete_snapshot(const common::Uuid& subscription_id) {
  return impl_->manager.complete_snapshot(subscription_id);
}

common::Status DurableMultiTabletSubscription::publish_committed(CommittedChange change) {
  common::Status applied = impl_->manager.publish_committed(std::move(change));
  if (applied.is_ok())
    impl_->dirty = true;
  return applied;
}

common::Status DurableMultiTabletSubscription::mark_continuity_lost(const SourcePosition position) {
  common::Status applied = impl_->manager.mark_continuity_lost(position);
  if (applied.is_ok())
    impl_->dirty = true;
  return applied;
}

common::Result<std::vector<DeliveryRecord>>
DurableMultiTabletSubscription::poll(const common::Uuid& subscription_id,
                                     const std::size_t maximum_records) const {
  return impl_->manager.poll(subscription_id, maximum_records);
}

common::Result<std::vector<std::byte>>
DurableMultiTabletSubscription::acknowledge(const common::Uuid& subscription_id,
                                            const std::uint64_t delivery_sequence) {
  return impl_->manager.acknowledge(subscription_id, delivery_sequence);
}

common::Result<std::vector<std::byte>>
DurableMultiTabletSubscription::cancel(const common::Uuid& subscription_id) {
  return impl_->manager.cancel(subscription_id);
}

void DurableMultiTabletSubscription::abandon(const common::Uuid& subscription_id) noexcept {
  impl_->manager.abandon(subscription_id);
}

common::Result<MultiTabletSubscriptionStatus>
DurableMultiTabletSubscription::status(const common::Uuid& subscription_id) const {
  return impl_->manager.status(subscription_id);
}

common::Result<std::vector<SourcePosition>>
DurableMultiTabletSubscription::latest_positions() const {
  return impl_->manager.latest_positions();
}

const MultiTabletSubscriptionSource& DurableMultiTabletSubscription::source() const noexcept {
  return impl_->manager.source();
}

common::Result<MultiTabletSnapshotSubscription> DurableMultiTabletSubscription::start_snapshot(
    const PreparedSubscriptionPlan& plan, const common::Uuid subscription_id,
    const query::QueryResourceContext& resources, const manifest::ManifestStorage& storage,
    const manifest::DatabaseStoragePublisher& publisher, const schema::SchemaLineage& lineage,
    const SnapshotSubscriptionLimits limits) {
  try {
    std::vector<SnapshotSubscriptionColumn> columns{plan.columns().begin(), plan.columns().end()};
    return MultiTabletSnapshotSubscription::start(
        impl_->manager, plan.request(subscription_id), resources, storage, publisher, lineage,
        plan.schema_ptr()->schema_id(), plan.physical_plan(), std::move(columns), limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("durable subscription snapshot allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("durable subscription snapshot exceeds container limits"));
  }
}

common::Result<InstalledMultiTabletSubscriptionCheckpoint>
DurableMultiTabletSubscription::checkpoint() {
  if (impl_->dirty && impl_->generation == std::numeric_limits<std::uint64_t>::max())
    return common::make_unexpected(
        common::Status{common::StatusCode::kOutOfRange,
                       "durable subscription checkpoint generation is exhausted"});
  auto state = impl_->manager.checkpoint();
  if (!state.has_value())
    return common::make_unexpected(state.error());
  std::vector<SourcePosition> candidate_frontiers;
  try {
    candidate_frontiers = retention_frontiers(*state);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("durable subscription frontier allocation failed"));
  }
  const std::uint64_t target_generation = impl_->dirty ? impl_->generation + 1U : impl_->generation;
  if (target_generation == 0U)
    return common::make_unexpected(invalid("durable subscription generation is invalid"));
  BoundMultiTabletSubscriptionCheckpoint checkpoint{target_generation, std::move(*state)};
  auto installed = impl_->storage.install(checkpoint);
  if (!installed.has_value())
    return common::make_unexpected(installed.error());
  impl_->generation = target_generation;
  impl_->durable_frontiers = std::move(candidate_frontiers);
  impl_->dirty = false;
  return installed;
}

common::Result<std::optional<std::vector<SourcePosition>>>
DurableMultiTabletSubscription::durable_retention_frontiers() const {
  try {
    return impl_->durable_frontiers;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("durable subscription frontier copy allocation failed"));
  }
}

std::uint64_t DurableMultiTabletSubscription::checkpoint_generation() const noexcept {
  return impl_->generation;
}

bool DurableMultiTabletSubscription::has_uncheckpointed_changes() const noexcept {
  return impl_->dirty;
}

} // namespace chronos::live
