#include "chronos/live/subscription_retention.hpp"

#include <algorithm>
#include <cstddef>
#include <map>
#include <new>
#include <ranges>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status topology_changed(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status validate_owner_binding(const SubscriptionRetentionConfig& config,
                                                    const std::span<const SourcePosition> frontiers,
                                                    const DurableMultiTabletSubscription& owner) {
  const MultiTabletSubscriptionSource& source = owner.source();
  if (source.database_id != config.database_id || source.table_id != config.table_id ||
      source.members.size() != config.members.size())
    return invalid("subscription retention owner identity or source count disagrees");
  for (std::size_t index = 0U; index < config.members.size(); ++index) {
    const SourcePosition owner_position = source.members[index].position();
    if (!owner_position.same_source(config.members[index].position()))
      return invalid("subscription retention owner source lineage disagrees");
    if (owner_position.record_sequence < frontiers[index].record_sequence)
      return topology_changed("subscription retention owner begins behind authorized reclamation");
  }
  return common::Status::ok();
}

} // namespace

class SubscriptionRetentionCoordinator::Impl {
public:
  explicit Impl(SubscriptionRetentionConfig configured,
                std::vector<SourcePosition> initial_frontiers) noexcept
      : config(std::move(configured)), frontiers(std::move(initial_frontiers)) {}

  SubscriptionRetentionConfig config;
  std::vector<SourcePosition> frontiers;
};

SubscriptionRetentionCoordinator::SubscriptionRetentionCoordinator(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SubscriptionRetentionCoordinator::~SubscriptionRetentionCoordinator() = default;
SubscriptionRetentionCoordinator::SubscriptionRetentionCoordinator(
    SubscriptionRetentionCoordinator&&) noexcept = default;
SubscriptionRetentionCoordinator&
SubscriptionRetentionCoordinator::operator=(SubscriptionRetentionCoordinator&&) noexcept = default;

common::Result<SubscriptionRetentionCoordinator>
SubscriptionRetentionCoordinator::create(SubscriptionRetentionConfig config) {
  if (config.database_id.is_nil() || config.table_id.uuid().is_nil() ||
      config.local_node_id == 0U || config.members.empty() ||
      config.maximum_subscription_owners == 0U ||
      config.subscription_owners.size() > config.maximum_subscription_owners)
    return common::make_unexpected(invalid("subscription retention configuration is invalid"));
  try {
    std::ranges::sort(config.members, {}, &SubscriptionRetentionMember::tablet_id);
    std::vector<SourcePosition> initial;
    initial.reserve(config.members.size());
    for (std::size_t index = 0U; index < config.members.size(); ++index) {
      const SubscriptionRetentionMember& member = config.members[index];
      if (!member.is_valid() ||
          (index != 0U && config.members[index - 1U].tablet_id >= member.tablet_id))
        return common::make_unexpected(
            invalid("subscription retention source topology is invalid"));
      initial.push_back(member.position());
    }
    for (std::size_t owner_index = 0U; owner_index < config.subscription_owners.size();
         ++owner_index) {
      const DurableMultiTabletSubscription* owner = config.subscription_owners[owner_index];
      if (owner == nullptr)
        return common::make_unexpected(invalid("subscription retention owner is null"));
      if (std::ranges::find(config.subscription_owners.begin(),
                            config.subscription_owners.begin() +
                                static_cast<std::ptrdiff_t>(owner_index),
                            owner) !=
          config.subscription_owners.begin() + static_cast<std::ptrdiff_t>(owner_index))
        return common::make_unexpected(invalid("subscription retention owner is duplicated"));
      const common::Status valid = validate_owner_binding(config, initial, *owner);
      if (!valid.is_ok())
        return common::make_unexpected(valid);
      const PlanFingerprint& fingerprint = owner->source().plan_fingerprint;
      if (std::ranges::any_of(config.subscription_owners.begin(),
                              config.subscription_owners.begin() +
                                  static_cast<std::ptrdiff_t>(owner_index),
                              [&fingerprint](const DurableMultiTabletSubscription* prior) {
                                return prior->source().plan_fingerprint == fingerprint;
                              }))
        return common::make_unexpected(invalid("subscription retention owner plan is duplicated"));
    }
    return SubscriptionRetentionCoordinator{
        std::make_unique<Impl>(std::move(config), std::move(initial))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription retention allocation failed"));
  }
}

common::Status SubscriptionRetentionCoordinator::register_subscription_owner(
    const DurableMultiTabletSubscription& owner) {
  common::Status valid = validate_owner_binding(impl_->config, impl_->frontiers, owner);
  if (!valid.is_ok())
    return valid;
  const PlanFingerprint& fingerprint = owner.source().plan_fingerprint;
  if (std::ranges::any_of(impl_->config.subscription_owners,
                          [&owner, &fingerprint](const DurableMultiTabletSubscription* current) {
                            return current == &owner ||
                                   current->source().plan_fingerprint == fingerprint;
                          }))
    return invalid("subscription retention owner is already registered");
  if (impl_->config.subscription_owners.size() >= impl_->config.maximum_subscription_owners)
    return exhausted("subscription retention owner capacity is exhausted");
  try {
    impl_->config.subscription_owners.push_back(&owner);
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    return exhausted("subscription retention owner registration allocation failed");
  } catch (const std::length_error&) {
    return exhausted("subscription retention owner registration exceeds container limits");
  }
}

common::Status SubscriptionRetentionCoordinator::retire_subscription_owner(
    const DurableMultiTabletSubscription& owner) {
  const auto found = std::ranges::find(impl_->config.subscription_owners, &owner);
  if (found == impl_->config.subscription_owners.end())
    return invalid("subscription retention owner is not registered");
  impl_->config.subscription_owners.erase(found);
  return common::Status::ok();
}

std::size_t SubscriptionRetentionCoordinator::subscription_owner_count() const noexcept {
  return impl_->config.subscription_owners.size();
}

common::Result<SubscriptionRetentionReport> SubscriptionRetentionCoordinator::advance(
    const raft::MetadataStateMachine& metadata,
    const std::span<const SourcePosition> storage_safe_frontiers,
    SubscriptionSourceReclaimer& reclaimer) {
  if (storage_safe_frontiers.size() != impl_->config.members.size())
    return common::make_unexpected(
        invalid("subscription retention storage frontier count is invalid"));
  const raft::LogIndex metadata_index = metadata.applied_index();
  if (metadata_index == 0U)
    return common::make_unexpected(
        topology_changed("subscription retention has no committed metadata topology"));
  for (std::size_t index = 0U; index < impl_->config.members.size(); ++index) {
    const SubscriptionRetentionMember& member = impl_->config.members[index];
    const SourcePosition& storage = storage_safe_frontiers[index];
    if (!storage.same_source(member.position()) ||
        storage.record_sequence < impl_->frontiers[index].record_sequence)
      return common::make_unexpected(
          invalid("subscription retention storage frontier regressed or changed lineage"));
    const raft::TabletPlacementMetadata* placement = metadata.find_tablet(member.tablet_id);
    if (placement == nullptr || placement->table_id != impl_->config.table_id ||
        placement->placement_epoch != member.placement_epoch ||
        !std::ranges::contains(placement->replicas, impl_->config.local_node_id))
      return common::make_unexpected(
          topology_changed("subscription retention placement epoch or local replica changed"));
    if (member.source_kind == SubscriptionSourceKind::kRaft) {
      const raft::TabletGroupBindingMetadata* binding =
          metadata.find_tablet_group_binding(member.tablet_id);
      if (binding == nullptr || binding->group_id != member.raft_group_id)
        return common::make_unexpected(
            topology_changed("subscription retention Raft group binding changed"));
    }
  }

  try {
    std::vector<SourcePosition> candidate{storage_safe_frontiers.begin(),
                                          storage_safe_frontiers.end()};
    for (const DurableMultiTabletSubscription* owner : impl_->config.subscription_owners) {
      auto durable = owner->durable_retention_frontiers();
      if (!durable.has_value())
        return common::make_unexpected(durable.error());
      if (!durable->has_value())
        return SubscriptionRetentionReport{metadata_index, true, false, impl_->frontiers};
      std::vector<SourcePosition> durable_frontiers =
          std::move(*durable).value_or(std::vector<SourcePosition>{});
      if (durable_frontiers.size() != candidate.size())
        return common::make_unexpected(common::Status{
            common::StatusCode::kCorruption, "durable subscription frontier source count changed"});
      for (std::size_t index = 0U; index < candidate.size(); ++index) {
        if (!durable_frontiers[index].same_source(candidate[index]))
          return common::make_unexpected(
              common::Status{common::StatusCode::kCorruption,
                             "durable subscription frontier source lineage changed"});
        candidate[index].record_sequence =
            std::min(candidate[index].record_sequence, durable_frontiers[index].record_sequence);
      }
    }

    bool advanced = false;
    std::vector<SubscriptionSourceReclamation> requests;
    requests.reserve(candidate.size());
    for (std::size_t index = 0U; index < candidate.size(); ++index) {
      if (candidate[index].record_sequence < impl_->frontiers[index].record_sequence)
        return common::make_unexpected(
            common::Status{common::StatusCode::kCorruption,
                           "durable subscription frontier moved behind prior reclamation"});
      advanced =
          advanced || candidate[index].record_sequence > impl_->frontiers[index].record_sequence;
      requests.push_back(
          {candidate[index], impl_->config.members[index].placement_epoch, metadata_index});
    }
    if (!advanced)
      return SubscriptionRetentionReport{metadata_index, false, false, impl_->frontiers};
    const common::Status reclaimed = reclaimer.reclaim(requests);
    if (!reclaimed.is_ok())
      return common::make_unexpected(reclaimed);
    impl_->frontiers = candidate;
    return SubscriptionRetentionReport{metadata_index, false, true, std::move(candidate)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription retention advance allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("subscription retention state exceeds limits"));
  }
}

std::span<const SourcePosition>
SubscriptionRetentionCoordinator::authorized_frontiers() const noexcept {
  return impl_->frontiers;
}

} // namespace chronos::live
