#include "chronos/query/distributed_fragment_worker.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
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

} // namespace

common::Result<ExchangeMessage>
execute_distributed_aggregate_fragment(const DistributedAggregateWorkerRequest& request) {
  const DistributedAggregateFragmentDispatch& dispatch = request.dispatch.get();
  const DistributedAggregateFragment& fragment = dispatch.fragment;
  const manifest::TemporalDatabaseStorageSnapshot& snapshot = request.snapshot.get();
  const schema::SchemaLineage& lineage = request.lineage.get();
  const raft::TabletPlacementMetadata& placement = request.placement.get();

  // This validates all in-memory fragment fields even for callers that bypassed the wire decoder.
  const auto structurally_valid = encode_distributed_aggregate_fragment_dispatch(dispatch);
  if (!structurally_valid.has_value())
    return common::make_unexpected(structurally_valid.error());
  if (request.local_node == 0U || request.raft_group_id.is_nil() ||
      dispatch.raft_group_id != request.raft_group_id ||
      fragment.serving_node != request.local_node) {
    return common::make_unexpected(unavailable("distributed worker route differs from dispatch"));
  }
  const common::Status placement_status =
      validate_local_placement(placement, fragment, request.local_node);
  if (!placement_status.is_ok())
    return common::make_unexpected(placement_status);
  if (fragment.read_policy.consistency == DistributedReadConsistency::kLeaderLinearizable) {
    if (!request.local_linearizable_barrier.has_value() ||
        request.local_linearizable_barrier != fragment.linearizable_barrier) {
      return common::make_unexpected(
          unavailable("distributed worker local Raft read barrier differs"));
    }
  } else if (request.local_linearizable_barrier.has_value()) {
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
      tablet->source_id != request.raft_group_id ||
      tablet->durable_position != fragment.applied_position) {
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

  try {
    MergeableAggregateState partial;
    if (tablet->part_count != 0U) {
      const std::span<const manifest::TemporalPartDescriptor> descriptors =
          snapshot.parts().subspan(static_cast<std::size_t>(tablet->first_part_index),
                                   static_cast<std::size_t>(tablet->part_count));
      std::vector<cseg::PartId> part_ids;
      part_ids.reserve(descriptors.size());
      for (const manifest::TemporalPartDescriptor& descriptor : descriptors)
        part_ids.push_back(descriptor.part_id);
      const std::array bindings{manifest::TabletSchemaBinding{.tablet_id = fragment.tablet_id,
                                                              .lineage = std::cref(lineage)}};
      auto images = request.storage.get().load_temporal_part_images(
          snapshot.selected_manifest(), part_ids, bindings, request.limits.part_validation);
      if (!images.has_value())
        return common::make_unexpected(images.error());
      std::vector<TemporalManifestCsegPartView> views;
      views.reserve(images->size());
      for (const manifest::LoadedTemporalPartImage& image : *images) {
        views.push_back({.descriptor = &image.descriptor(), .bytes = image.bytes()});
      }
      auto visible = resolve_manifest_v2_temporal_tablet_snapshot(
          schema_value, lineage, *tablet, views,
          {.source = cseg::temporal_format::CommitSource::kRaft,
           .source_id = request.raft_group_id},
          std::nullopt, request.limits.resolution);
      if (!visible.has_value())
        return common::make_unexpected(visible.error());
      for (const ScalarInputRow& row : (*visible)->rows()) {
        if (row.columns.size() != schema_value->columns().size()) {
          return common::make_unexpected(common::Status{
              common::StatusCode::kCorruption, "distributed worker resolved row shape is invalid"});
        }
        const auto* event_time = std::get_if<std::int64_t>(&row.columns[*event_ordinal].storage());
        if (event_time == nullptr)
          return common::make_unexpected(common::Status{
              common::StatusCode::kCorruption, "distributed worker event time is invalid"});
        const auto matches = cseg::cseg_event_time_range_may_match(*event_time, *event_time,
                                                                   fragment.event_time_predicate);
        if (!matches.has_value())
          return common::make_unexpected(matches.error());
        if (!*matches || row.columns[aggregate_ordinal].is_null())
          continue;
        const auto* value = std::get_if<double>(&row.columns[aggregate_ordinal].storage());
        if (value == nullptr)
          return common::make_unexpected(common::Status{
              common::StatusCode::kCorruption, "distributed worker aggregate value is invalid"});
        const common::Status added = partial.add(*value);
        if (!added.is_ok())
          return common::make_unexpected(added);
      }
    } else if (tablet->durable_version_count != 0U) {
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

} // namespace chronos::query
