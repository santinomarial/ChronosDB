#include "chronos/query/distributed_fragment_worker.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status
validate_local_placement(const raft::TabletPlacementMetadata& placement,
                         const DistributedAggregateFragment& fragment,
                         const std::uint64_t local_node) {
  if (placement.table_id != fragment.table_id || placement.tablet_id != fragment.tablet_id ||
      placement.placement_epoch != fragment.placement_epoch || placement.replicas.empty() ||
      placement.replicas.size() > raft::MetadataLimits{}.maximum_replicas_per_tablet ||
      placement.replicas.front() == 0U || !std::ranges::is_sorted(placement.replicas) ||
      std::ranges::adjacent_find(placement.replicas) != placement.replicas.end() ||
      !std::ranges::binary_search(placement.replicas, local_node)) {
    return unavailable("distributed worker placement no longer authorizes the local replica");
  }
  if (placement.leader_hint.has_value() &&
      !std::ranges::binary_search(placement.replicas, *placement.leader_hint)) {
    return invalid("distributed worker placement leader is not a replica");
  }
  if (fragment.read_policy.consistency == DistributedReadConsistency::kLeaderLinearizable &&
      placement.leader_hint.has_value() && *placement.leader_hint != local_node) {
    return unavailable("distributed worker is no longer the placement leader");
  }
  return common::Status::ok();
}

struct ValidatedWorkerAuthority {
  const manifest::TemporalTabletDescriptor* tablet{};
  std::shared_ptr<const schema::TableSchema> schema_value;
  std::size_t event_ordinal{};
  std::uint32_t aggregate_ordinal{};
};

[[nodiscard]] common::Result<ValidatedWorkerAuthority> validate_worker_authority(
    const DistributedAggregateFragment& fragment, const common::Uuid& dispatch_group,
    const manifest::TemporalDatabaseStorageSnapshot& snapshot, const schema::SchemaLineage& lineage,
    const raft::TabletPlacementMetadata& placement, const common::Uuid& local_group,
    const std::uint64_t local_node, const std::optional<raft::ReadBarrier>& local_barrier) {
  if (local_node == 0U || local_group.is_nil() || dispatch_group != local_group ||
      fragment.serving_node != local_node) {
    return common::make_unexpected(unavailable("distributed worker route differs from dispatch"));
  }
  const common::Status placement_status = validate_local_placement(placement, fragment, local_node);
  if (!placement_status.is_ok())
    return common::make_unexpected(placement_status);
  if (fragment.read_policy.consistency == DistributedReadConsistency::kLeaderLinearizable) {
    if (!local_barrier.has_value() || local_barrier != fragment.linearizable_barrier) {
      return common::make_unexpected(
          unavailable("distributed worker local Raft read barrier differs"));
    }
  } else if (local_barrier.has_value()) {
    return common::make_unexpected(
        invalid("distributed worker non-linearizable request supplied a leader barrier"));
  }
  if (snapshot.generation() != fragment.snapshot_generation ||
      snapshot.database_id() != fragment.database_id) {
    return common::make_unexpected(unavailable("distributed worker snapshot generation differs"));
  }
  const auto tablet = std::ranges::find(snapshot.tablets(), fragment.tablet_id,
                                        &manifest::TemporalTabletDescriptor::tablet_id);
  const std::shared_ptr<const schema::TableSchema> schema_value = lineage.current();
  if (tablet == snapshot.tablets().end() || schema_value == nullptr ||
      lineage.table_id() != fragment.table_id ||
      schema_value->schema_id() != fragment.destination_schema_id ||
      tablet->table_id != fragment.table_id ||
      tablet->recovery_schema_id != schema_value->schema_id() ||
      tablet->recovery_schema_version != schema_value->version()) {
    return common::make_unexpected(unavailable("distributed worker tablet or schema differs"));
  }
  if (tablet->commit_source != manifest::ManifestCommitSource::kRaft ||
      tablet->source_id != local_group || tablet->durable_position != fragment.applied_position) {
    return common::make_unexpected(
        unavailable("distributed worker Raft snapshot boundary differs"));
  }
  if (fragment.aggregate_input_index >= fragment.destination_column_ordinals.size())
    return common::make_unexpected(invalid("distributed worker aggregate input is invalid"));
  const std::uint32_t aggregate_ordinal =
      fragment.destination_column_ordinals[fragment.aggregate_input_index];
  const std::optional<std::size_t> event_ordinal =
      schema_value->column_ordinal(schema_value->event_time_column());
  if (aggregate_ordinal >= schema_value->columns().size() || !event_ordinal.has_value() ||
      schema_value->columns()[aggregate_ordinal].type().kind() !=
          schema::LogicalTypeKind::kFloat64) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported,
                       "distributed worker requires event time and Float64 aggregate input"});
  }
  if (tablet->first_part_index > snapshot.parts().size() ||
      tablet->part_count > snapshot.parts().size() - tablet->first_part_index) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "distributed worker tablet part range is invalid"});
  }
  return ValidatedWorkerAuthority{.tablet = &*tablet,
                                  .schema_value = schema_value,
                                  .event_ordinal = *event_ordinal,
                                  .aggregate_ordinal = aggregate_ordinal};
}

class LocalTemporalPartBatchLoader final : public DistributedTemporalPartBatchLoader {
public:
  explicit LocalTemporalPartBatchLoader(const manifest::ManifestStorage& storage) noexcept
      : storage_(storage) {}

  common::Status load(const manifest::TemporalDatabaseStorageSnapshot& snapshot,
                      const std::span<const cseg::PartId> part_ids,
                      const std::span<const manifest::TabletSchemaBinding> schema_bindings,
                      const manifest::TemporalPartValidationLimits validation_limits,
                      DistributedTemporalPartBatchConsumer& consumer) const override {
    auto images = storage_.get().load_temporal_part_images(snapshot.selected_manifest(), part_ids,
                                                           schema_bindings, validation_limits);
    if (!images.has_value())
      return images.error();
    try {
      std::vector<TemporalManifestCsegPartView> views;
      views.reserve(images->size());
      for (const manifest::LoadedTemporalPartImage& image : *images)
        views.push_back({.descriptor = &image.descriptor(), .bytes = image.bytes()});
      return consumer.consume(views);
    } catch (const std::bad_alloc&) {
      return {common::StatusCode::kResourceExhausted,
              "distributed local part view allocation failed"};
    } catch (const std::length_error&) {
      return {common::StatusCode::kResourceExhausted,
              "distributed local part view exceeds container limits"};
    }
  }

private:
  std::reference_wrapper<const manifest::ManifestStorage> storage_;
};

class AggregatePartBatchConsumer final : public DistributedTemporalPartBatchConsumer {
public:
  AggregatePartBatchConsumer(std::shared_ptr<const schema::TableSchema> schema_value,
                             const schema::SchemaLineage& lineage,
                             const manifest::TemporalTabletDescriptor& tablet,
                             const common::Uuid& source_id,
                             const std::optional<cseg::EventTimePredicate>& predicate,
                             const std::size_t event_ordinal, const std::uint32_t aggregate_ordinal,
                             const TemporalManifestCsegResolutionLimits limits,
                             MergeableAggregateState& partial) noexcept
      : schema_value_(std::move(schema_value)), lineage_(lineage), tablet_(tablet),
        source_id_(source_id), predicate_(predicate), event_ordinal_(event_ordinal),
        aggregate_ordinal_(aggregate_ordinal), limits_(limits), partial_(partial) {}

  common::Status consume(const std::span<const TemporalManifestCsegPartView> parts) override {
    if (called_) {
      repeated_ = true;
      return {common::StatusCode::kCorruption,
              "distributed part loader invoked its consumer more than once"};
    }
    called_ = true;
    auto visible = resolve_manifest_v2_temporal_tablet_snapshot(
        schema_value_, lineage_.get(), tablet_.get(), parts,
        {.source = cseg::temporal_format::CommitSource::kRaft, .source_id = source_id_},
        std::nullopt, limits_);
    if (!visible.has_value())
      return visible.error();
    for (const ScalarInputRow& row : (*visible)->rows()) {
      if (row.columns.size() != schema_value_->columns().size()) {
        return {common::StatusCode::kCorruption,
                "distributed worker resolved row shape is invalid"};
      }
      const auto* event_time = std::get_if<std::int64_t>(&row.columns[event_ordinal_].storage());
      if (event_time == nullptr)
        return {common::StatusCode::kCorruption, "distributed worker event time is invalid"};
      const auto matches =
          cseg::cseg_event_time_range_may_match(*event_time, *event_time, predicate_.get());
      if (!matches.has_value())
        return matches.error();
      if (!*matches || row.columns[aggregate_ordinal_].is_null())
        continue;
      const auto* value = std::get_if<double>(&row.columns[aggregate_ordinal_].storage());
      if (value == nullptr)
        return {common::StatusCode::kCorruption, "distributed worker aggregate value is invalid"};
      const common::Status added = partial_.get().add(*value);
      if (!added.is_ok())
        return added;
    }
    return common::Status::ok();
  }

  [[nodiscard]] bool has_exactly_one_call() const noexcept {
    return called_ && !repeated_;
  }

private:
  std::shared_ptr<const schema::TableSchema> schema_value_;
  std::reference_wrapper<const schema::SchemaLineage> lineage_;
  std::reference_wrapper<const manifest::TemporalTabletDescriptor> tablet_;
  common::Uuid source_id_;
  std::reference_wrapper<const std::optional<cseg::EventTimePredicate>> predicate_;
  std::size_t event_ordinal_{};
  std::uint32_t aggregate_ordinal_{};
  TemporalManifestCsegResolutionLimits limits_;
  std::reference_wrapper<MergeableAggregateState> partial_;
  bool called_{false};
  bool repeated_{false};
};

struct CanonicalGroupedKey {
  bool present{};
  std::uint64_t bits{};

  [[nodiscard]] bool operator<(const CanonicalGroupedKey& other) const noexcept {
    return present != other.present ? present < other.present : bits < other.bits;
  }
};

[[nodiscard]] std::uint64_t canonical_group_bits(const double value) noexcept {
  if (value == 0.0)
    return 0U;
  if (std::isnan(value))
    return grouped_float64_exchange_format::kCanonicalQuietNanBits;
  return std::bit_cast<std::uint64_t>(value);
}

class GroupedPartBatchConsumer final : public DistributedTemporalPartBatchConsumer {
public:
  GroupedPartBatchConsumer(std::shared_ptr<const schema::TableSchema> schema_value,
                           const schema::SchemaLineage& lineage,
                           const manifest::TemporalTabletDescriptor& tablet,
                           const common::Uuid& source_id,
                           const std::optional<cseg::EventTimePredicate>& predicate,
                           const std::size_t event_ordinal, const std::uint32_t key_ordinal,
                           const std::uint32_t aggregate_ordinal,
                           const TemporalManifestCsegResolutionLimits limits) noexcept
      : schema_value_(std::move(schema_value)), lineage_(lineage), tablet_(tablet),
        source_id_(source_id), predicate_(predicate), event_ordinal_(event_ordinal),
        key_ordinal_(key_ordinal), aggregate_ordinal_(aggregate_ordinal), limits_(limits) {}

  common::Status consume(const std::span<const TemporalManifestCsegPartView> parts) override {
    if (called_) {
      repeated_ = true;
      return {common::StatusCode::kCorruption,
              "distributed grouped part consumer was invoked more than once"};
    }
    called_ = true;
    auto visible = resolve_manifest_v2_temporal_tablet_snapshot(
        schema_value_, lineage_.get(), tablet_.get(), parts,
        {.source = cseg::temporal_format::CommitSource::kRaft, .source_id = source_id_},
        std::nullopt, limits_);
    if (!visible.has_value())
      return visible.error();
    try {
      for (const ScalarInputRow& row : (*visible)->rows()) {
        if (row.columns.size() != schema_value_->columns().size())
          return {common::StatusCode::kCorruption,
                  "distributed grouped worker resolved row shape is invalid"};
        const auto* event_time = std::get_if<std::int64_t>(&row.columns[event_ordinal_].storage());
        if (event_time == nullptr)
          return {common::StatusCode::kCorruption,
                  "distributed grouped worker event time is invalid"};
        const auto matches =
            cseg::cseg_event_time_range_may_match(*event_time, *event_time, predicate_.get());
        if (!matches.has_value())
          return matches.error();
        if (!*matches)
          continue;
        CanonicalGroupedKey key;
        if (!row.columns[key_ordinal_].is_null()) {
          const auto* value = std::get_if<double>(&row.columns[key_ordinal_].storage());
          if (value == nullptr)
            return {common::StatusCode::kCorruption,
                    "distributed grouped worker key value is invalid"};
          key = {.present = true, .bits = canonical_group_bits(*value)};
        }
        MergeableAggregateState& partial = groups_[key];
        if (row.columns[aggregate_ordinal_].is_null())
          continue;
        const auto* value = std::get_if<double>(&row.columns[aggregate_ordinal_].storage());
        if (value == nullptr)
          return {common::StatusCode::kCorruption,
                  "distributed grouped worker aggregate value is invalid"};
        const common::Status added = partial.add(*value);
        if (!added.is_ok())
          return added;
      }
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return {common::StatusCode::kResourceExhausted,
              "distributed grouped worker state allocation failed"};
    }
  }

  [[nodiscard]] bool has_exactly_one_call() const noexcept {
    return called_ && !repeated_;
  }

  [[nodiscard]] std::map<CanonicalGroupedKey, MergeableAggregateState> take_groups() && noexcept {
    return std::move(groups_);
  }

private:
  std::shared_ptr<const schema::TableSchema> schema_value_;
  std::reference_wrapper<const schema::SchemaLineage> lineage_;
  std::reference_wrapper<const manifest::TemporalTabletDescriptor> tablet_;
  common::Uuid source_id_;
  std::reference_wrapper<const std::optional<cseg::EventTimePredicate>> predicate_;
  std::size_t event_ordinal_{};
  std::uint32_t key_ordinal_{};
  std::uint32_t aggregate_ordinal_{};
  TemporalManifestCsegResolutionLimits limits_;
  std::map<CanonicalGroupedKey, MergeableAggregateState> groups_;
  bool called_{false};
  bool repeated_{false};
};

} // namespace

common::Result<ExchangeMessage>
execute_distributed_aggregate_fragment(const DistributedAggregateWorkerRequest& request) {
  const LocalTemporalPartBatchLoader loader{request.storage.get()};
  return execute_distributed_aggregate_fragment(request, loader);
}

common::Result<ExchangeMessage>
execute_distributed_aggregate_fragment(const DistributedAggregateWorkerRequest& request,
                                       const DistributedTemporalPartBatchLoader& loader) {
  const DistributedAggregateFragmentDispatch& dispatch = request.dispatch.get();
  const DistributedAggregateFragment& fragment = dispatch.fragment;
  const manifest::TemporalDatabaseStorageSnapshot& snapshot = request.snapshot.get();
  const schema::SchemaLineage& lineage = request.lineage.get();
  const raft::TabletPlacementMetadata& placement = request.placement.get();

  // This validates all in-memory fragment fields even for callers that bypassed the wire decoder.
  const auto structurally_valid = encode_distributed_aggregate_fragment_dispatch(dispatch);
  if (!structurally_valid.has_value())
    return common::make_unexpected(structurally_valid.error());
  auto authority = validate_worker_authority(fragment, dispatch.raft_group_id, snapshot, lineage,
                                             placement, request.raft_group_id, request.local_node,
                                             request.local_linearizable_barrier);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
  const manifest::TemporalTabletDescriptor& tablet = *authority->tablet;

  try {
    MergeableAggregateState partial;
    if (tablet.part_count != 0U) {
      const std::span<const manifest::TemporalPartDescriptor> descriptors =
          snapshot.parts().subspan(static_cast<std::size_t>(tablet.first_part_index),
                                   static_cast<std::size_t>(tablet.part_count));
      std::vector<cseg::PartId> part_ids;
      part_ids.reserve(descriptors.size());
      for (const manifest::TemporalPartDescriptor& descriptor : descriptors)
        part_ids.push_back(descriptor.part_id);
      const std::array bindings{manifest::TabletSchemaBinding{.tablet_id = fragment.tablet_id,
                                                              .lineage = std::cref(lineage)}};
      AggregatePartBatchConsumer consumer{authority->schema_value,
                                          lineage,
                                          tablet,
                                          request.raft_group_id,
                                          fragment.event_time_predicate,
                                          authority->event_ordinal,
                                          authority->aggregate_ordinal,
                                          request.limits.resolution,
                                          partial};
      const common::Status loaded =
          loader.load(snapshot, part_ids, bindings, request.limits.part_validation, consumer);
      if (!loaded.is_ok())
        return common::make_unexpected(loaded);
      if (!consumer.has_exactly_one_call()) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kCorruption,
                           "distributed part loader did not invoke its consumer exactly once"});
      }
    } else if (tablet.durable_version_count != 0U) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kCorruption, "distributed worker empty part set has durable rows"});
    }
    return ExchangeMessage{.query_id = fragment.query_id,
                           .tablet_id = fragment.tablet_id,
                           .sequence = 1U,
                           .partial = partial,
                           .terminal = true};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed worker allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed worker exceeded limits"});
  }
}

common::Result<DistributedGroupedFloat64WorkerResult> execute_distributed_grouped_float64_fragment(
    const DistributedGroupedFloat64WorkerRequest& request) {
  const LocalTemporalPartBatchLoader loader{request.storage.get()};
  return execute_distributed_grouped_float64_fragment(request, loader);
}

common::Result<DistributedGroupedFloat64WorkerResult>
execute_distributed_grouped_float64_fragment(const DistributedGroupedFloat64WorkerRequest& request,
                                             const DistributedTemporalPartBatchLoader& loader) {
  const DistributedGroupedFloat64FragmentDispatch& dispatch = request.dispatch.get();
  const DistributedGroupedFloat64Fragment& grouped = dispatch.fragment;
  const DistributedAggregateFragment& fragment = grouped.aggregate;
  const manifest::TemporalDatabaseStorageSnapshot& snapshot = request.snapshot.get();
  const schema::SchemaLineage& lineage = request.lineage.get();
  const raft::TabletPlacementMetadata& placement = request.placement.get();

  const auto structurally_valid = encode_distributed_grouped_float64_fragment_dispatch(dispatch);
  if (!structurally_valid.has_value())
    return common::make_unexpected(structurally_valid.error());
  auto authority = validate_worker_authority(fragment, dispatch.raft_group_id, snapshot, lineage,
                                             placement, request.raft_group_id, request.local_node,
                                             request.local_linearizable_barrier);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
  if (grouped.group_key_input_index >= fragment.destination_column_ordinals.size()) {
    return common::make_unexpected(
        invalid("distributed grouped worker key input is out of bounds"));
  }
  const std::uint32_t key_ordinal =
      fragment.destination_column_ordinals[grouped.group_key_input_index];
  if (key_ordinal >= authority->schema_value->columns().size() ||
      authority->schema_value->columns()[key_ordinal].type().kind() !=
          schema::LogicalTypeKind::kFloat64) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported,
                       "distributed grouped worker requires a Float64 key input"});
  }
  const manifest::TemporalTabletDescriptor& tablet = *authority->tablet;

  try {
    std::map<CanonicalGroupedKey, MergeableAggregateState> groups;
    if (tablet.part_count != 0U) {
      const std::span<const manifest::TemporalPartDescriptor> descriptors =
          snapshot.parts().subspan(static_cast<std::size_t>(tablet.first_part_index),
                                   static_cast<std::size_t>(tablet.part_count));
      std::vector<cseg::PartId> part_ids;
      part_ids.reserve(descriptors.size());
      for (const manifest::TemporalPartDescriptor& descriptor : descriptors)
        part_ids.push_back(descriptor.part_id);
      const std::array bindings{manifest::TabletSchemaBinding{.tablet_id = fragment.tablet_id,
                                                              .lineage = std::cref(lineage)}};
      GroupedPartBatchConsumer consumer{authority->schema_value,
                                        lineage,
                                        tablet,
                                        request.raft_group_id,
                                        fragment.event_time_predicate,
                                        authority->event_ordinal,
                                        key_ordinal,
                                        authority->aggregate_ordinal,
                                        request.limits.resolution};
      const common::Status loaded =
          loader.load(snapshot, part_ids, bindings, request.limits.part_validation, consumer);
      if (!loaded.is_ok())
        return common::make_unexpected(loaded);
      if (!consumer.has_exactly_one_call()) {
        return common::make_unexpected(common::Status{
            common::StatusCode::kCorruption,
            "distributed grouped part loader did not invoke its consumer exactly once"});
      }
      groups = std::move(consumer).take_groups();
    } else if (tablet.durable_version_count != 0U) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kCorruption, "distributed grouped worker empty part set has rows"});
    }
    if (groups.empty()) {
      return DistributedGroupedFloat64WorkerResult{GroupedExchangeTerminalMessage{
          .query_id = fragment.query_id, .tablet_id = fragment.tablet_id, .sequence = 1U}};
    }
    std::vector<GroupedFloat64ExchangeMessage> messages;
    messages.reserve(groups.size());
    std::uint64_t sequence = 1U;
    for (const auto& [key, partial] : groups) {
      const bool terminal = sequence == static_cast<std::uint64_t>(groups.size());
      messages.push_back({.query_id = fragment.query_id,
                          .tablet_id = fragment.tablet_id,
                          .sequence = sequence,
                          .group_key = key.present
                                           ? std::optional<double>{std::bit_cast<double>(key.bits)}
                                           : std::nullopt,
                          .partial = partial,
                          .terminal = terminal});
      if (!terminal)
        ++sequence;
    }
    return DistributedGroupedFloat64WorkerResult{std::move(messages)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed grouped worker allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed grouped worker exceeded limits"});
  }
}

} // namespace chronos::query
