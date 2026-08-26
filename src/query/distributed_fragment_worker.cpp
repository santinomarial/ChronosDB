#include "chronos/query/distributed_fragment_worker.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/query/column_output.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"

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

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

template <typename Fragment>
[[nodiscard]] common::Status
validate_local_placement(const raft::TabletPlacementMetadata& placement, const Fragment& fragment,
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
  return common::Status::ok();
}

struct ValidatedWorkerAuthority {
  const manifest::TemporalTabletDescriptor* tablet{};
  std::shared_ptr<const schema::TableSchema> schema_value;
  std::size_t event_ordinal{};
};

template <typename Fragment>
[[nodiscard]] common::Result<ValidatedWorkerAuthority> validate_worker_authority(
    const Fragment& fragment, const common::Uuid& dispatch_group,
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
  const std::optional<std::size_t> event_ordinal =
      schema_value->column_ordinal(schema_value->event_time_column());
  if (!event_ordinal.has_value()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotSupported, "distributed worker requires an event-time column"});
  }
  if (tablet->first_part_index > snapshot.parts().size() ||
      tablet->part_count > snapshot.parts().size() - tablet->first_part_index) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "distributed worker tablet part range is invalid"});
  }
  return ValidatedWorkerAuthority{
      .tablet = &*tablet, .schema_value = schema_value, .event_ordinal = *event_ordinal};
}

[[nodiscard]] common::Result<std::uint32_t>
aggregate_ordinal(const DistributedAggregateFragment& fragment,
                  const schema::TableSchema& schema_value) {
  if (fragment.aggregate_input_index >= fragment.destination_column_ordinals.size())
    return common::make_unexpected(invalid("distributed worker aggregate input is invalid"));
  const std::uint32_t ordinal =
      fragment.destination_column_ordinals[fragment.aggregate_input_index];
  if (ordinal >= schema_value.columns().size() ||
      schema_value.columns()[ordinal].type().kind() != schema::LogicalTypeKind::kFloat64) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported,
                       "distributed worker requires a Float64 aggregate input"});
  }
  return ordinal;
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
  // NOLINTBEGIN(bugprone-easily-swappable-parameters)
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
  // NOLINTEND(bugprone-easily-swappable-parameters)

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
    return present != other.present ? !present : bits < other.bits;
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
  // NOLINTBEGIN(bugprone-easily-swappable-parameters)
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
  // NOLINTEND(bugprone-easily-swappable-parameters)

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

[[nodiscard]] TimestampRangePredicate
timestamp_predicate(const cseg::EventTimePredicate& predicate) noexcept {
  return {.lower = predicate.lower.has_value()
                       ? std::optional<TimestampRangeBound>{TimestampRangeBound{
                             predicate.lower->value, predicate.lower->inclusive}}
                       : std::nullopt,
          .upper = predicate.upper.has_value()
                       ? std::optional<TimestampRangeBound>{TimestampRangeBound{
                             predicate.upper->value, predicate.upper->inclusive}}
                       : std::nullopt};
}

[[nodiscard]] common::Result<DistributedVectorRowsWorkerResultV2>
execute_vector_rows_snapshot(const DistributedVectorRowsWorkerRequestV2& request,
                             const ValidatedWorkerAuthority& authority,
                             std::shared_ptr<const ScalarTableSnapshot> scalar_snapshot,
                             DistributedVectorRowsChunkConsumerV2& consumer) {
  const DistributedVectorFragmentDispatchV2& dispatch = request.dispatch.get();
  auto resources = QueryResourceContext::create(request.limits.maximum_query_memory_bytes);
  if (!resources.has_value())
    return common::make_unexpected(resources.error());
  auto pipeline =
      ScalarSnapshotScanOperator::create(std::move(scalar_snapshot), request.limits.scan);
  if (!pipeline.has_value())
    return common::make_unexpected(pipeline.error());
  if (dispatch.dispatch.event_time_predicate.has_value()) {
    pipeline = TimestampRangeFilterOperator::create(
        std::move(*pipeline), authority.event_ordinal,
        timestamp_predicate(*dispatch.dispatch.event_time_predicate));
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());
  }

  try {
    std::vector<std::size_t> output_ordinals;
    output_ordinals.reserve(dispatch.dispatch.plan.row_output_indices.size());
    for (const std::uint32_t projected_index : dispatch.dispatch.plan.row_output_indices)
      output_ordinals.push_back(dispatch.dispatch.destination_column_ordinals[projected_index]);
    pipeline = SourceColumnOutputOperator::create(std::move(*pipeline), std::move(output_ordinals),
                                                  request.limits.output);
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());

    DistributedVectorRowsWorkerResultV2 result;
    for (;;) {
      auto step = (*pipeline)->next(*resources);
      if (!step.has_value())
        return common::make_unexpected(step.error());
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        return result;
      const AccountedVectorChunk* accounted = step->chunk();
      if (accounted == nullptr)
        return common::make_unexpected(corruption("distributed vector worker chunk is missing"));
      const VectorChunk& chunk = accounted->chunk();
      if (chunk.column_count() != dispatch.result_schema.columns.size()) {
        return common::make_unexpected(
            corruption("distributed vector worker output width differs from its schema"));
      }
      for (std::size_t column = 0U; column < chunk.column_count(); ++column) {
        const columnar::PhysicalColumnView* physical = chunk.column(column);
        const DistributedVectorResultColumn& expected = dispatch.result_schema.columns[column];
        if (physical == nullptr || physical->type() != expected.type ||
            physical->nullable() != expected.nullable) {
          return common::make_unexpected(
              corruption("distributed vector worker output shape differs from its schema"));
        }
      }
      const std::size_t rows = chunk.selected_row_count();
      if (rows == 0U)
        continue;
      if (rows > std::numeric_limits<std::uint64_t>::max() - result.output_rows ||
          result.output_chunks == std::numeric_limits<std::size_t>::max()) {
        return common::make_unexpected(exhausted("distributed vector worker output overflows"));
      }
      const common::Status consumed = consumer.consume(chunk);
      if (!consumed.is_ok())
        return common::make_unexpected(consumed);
      result.output_rows += static_cast<std::uint64_t>(rows);
      ++result.output_chunks;
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed vector worker allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("distributed vector worker exceeds container limits"));
  }
}

class VectorRowsPartBatchConsumer final : public DistributedTemporalPartBatchConsumer {
public:
  VectorRowsPartBatchConsumer(const DistributedVectorRowsWorkerRequestV2& request,
                              const ValidatedWorkerAuthority& authority,
                              DistributedVectorRowsChunkConsumerV2& consumer) noexcept
      : request_(request), authority_(authority), consumer_(consumer) {}

  common::Status consume(const std::span<const TemporalManifestCsegPartView> parts) override {
    if (called_) {
      repeated_ = true;
      return corruption("distributed vector part consumer was invoked more than once");
    }
    called_ = true;
    auto visible = resolve_manifest_v2_temporal_tablet_snapshot(
        authority_.get().schema_value, request_.get().lineage.get(), *authority_.get().tablet,
        parts,
        {.source = cseg::temporal_format::CommitSource::kRaft,
         .source_id = request_.get().raft_group_id},
        std::nullopt, request_.get().limits.storage.resolution);
    if (!visible.has_value())
      return visible.error();
    auto executed = execute_vector_rows_snapshot(request_.get(), authority_.get(),
                                                 std::move(*visible), consumer_.get());
    if (!executed.has_value())
      return executed.error();
    result_ = *executed;
    return common::Status::ok();
  }

  [[nodiscard]] bool has_exactly_one_call() const noexcept {
    return called_ && !repeated_;
  }

  [[nodiscard]] const std::optional<DistributedVectorRowsWorkerResultV2>& result() const noexcept {
    return result_;
  }

private:
  std::reference_wrapper<const DistributedVectorRowsWorkerRequestV2> request_;
  std::reference_wrapper<const ValidatedWorkerAuthority> authority_;
  std::reference_wrapper<DistributedVectorRowsChunkConsumerV2> consumer_;
  std::optional<DistributedVectorRowsWorkerResultV2> result_;
  bool called_{};
  bool repeated_{};
};

[[nodiscard]] bool
valid_vector_aggregate_limits(const DistributedVectorAggregateWorkerLimitsV2& limits) noexcept {
  return limits.maximum_query_memory_bytes > 0U &&
         limits.maximum_query_memory_bytes <= kMaximumDistributedVectorRowsWorkerMemoryBytesV2 &&
         limits.scan.maximum_rows_per_chunk > 0U && limits.scan.chunk.maximum_rows > 0U &&
         limits.scan.chunk.maximum_columns > 0U && limits.scan.chunk.maximum_buffer_bytes > 0U &&
         limits.scan.chunk.maximum_retained_buffer_bytes > 0U &&
         limits.projection.maximum_rows > 0U && limits.projection.maximum_columns > 0U &&
         limits.projection.maximum_buffer_bytes > 0U &&
         limits.projection.maximum_retained_buffer_bytes > 0U &&
         limits.projection.maximum_rows >=
             std::min(limits.scan.maximum_rows_per_chunk, limits.scan.chunk.maximum_rows) &&
         limits.maximum_aggregates > 0U &&
         limits.maximum_aggregates <= kMaximumUngroupedAggregateWidth &&
         limits.maximum_variable_extremum_bytes > 0U &&
         limits.maximum_variable_extremum_bytes <=
             distributed_vector_aggregate_state_format::kMaximumExtremumBytes &&
         limits.maximum_retained_configuration_bytes > 0U;
}

struct ValidatedVectorAggregateWorkerBinding {
  ValidatedWorkerAuthority authority;
  std::vector<PhysicalColumnShape> projected_inputs;
  std::vector<VectorAggregateDefinition> definitions;
};

[[nodiscard]] common::Result<ValidatedVectorAggregateWorkerBinding>
validate_vector_aggregate_worker_binding(const DistributedVectorAggregateWorkerRequestV2& request) {
  const DistributedVectorFragmentDispatchV2& dispatch = request.dispatch.get();
  const DistributedVectorFragmentDispatch& fragment = dispatch.dispatch;
  const auto structurally_valid = encode_distributed_vector_fragment_dispatch_v2(dispatch);
  if (!structurally_valid.has_value())
    return common::make_unexpected(structurally_valid.error());
  if (!valid_vector_aggregate_limits(request.limits)) {
    return common::make_unexpected(
        invalid("distributed vector aggregate worker limits are invalid"));
  }
  if (fragment.plan.mode != DistributedVectorPlanMode::kUngroupedAggregate) {
    return common::make_unexpected(
        invalid("distributed vector aggregate worker requires an ungrouped aggregate plan"));
  }
  auto authority = validate_worker_authority(
      fragment, fragment.raft_group_id, request.snapshot.get(), request.lineage.get(),
      request.placement.get(), request.raft_group_id, request.local_node,
      request.local_linearizable_barrier);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());

  try {
    std::vector<PhysicalColumnShape> projected_inputs;
    projected_inputs.reserve(fragment.destination_column_ordinals.size());
    for (const std::uint32_t ordinal : fragment.destination_column_ordinals) {
      if (ordinal >= authority->schema_value->columns().size()) {
        return common::make_unexpected(
            invalid("distributed vector aggregate projection is out of bounds"));
      }
      const schema::ColumnDefinition& column = authority->schema_value->columns()[ordinal];
      projected_inputs.push_back({column.type(), column.nullable()});
    }
    auto definitions = bind_distributed_vector_ungrouped_aggregate_definitions(
        fragment.plan, projected_inputs, dispatch.result_schema);
    if (!definitions.has_value())
      return common::make_unexpected(definitions.error());
    if (definitions->size() > request.limits.maximum_aggregates ||
        projected_inputs.size() > request.limits.projection.maximum_columns) {
      return common::make_unexpected(
          exhausted("distributed vector aggregate worker width exceeds its limit"));
    }
    return ValidatedVectorAggregateWorkerBinding{.authority = std::move(*authority),
                                                 .projected_inputs = std::move(projected_inputs),
                                                 .definitions = std::move(*definitions)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("distributed vector aggregate worker binding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed vector aggregate worker binding exceeds container limits"));
  }
}

[[nodiscard]] common::Result<DistributedVectorAggregateWorkerResultV2>
execute_vector_aggregate_snapshot(const DistributedVectorAggregateWorkerRequestV2& request,
                                  const ValidatedWorkerAuthority& authority,
                                  std::shared_ptr<const ScalarTableSnapshot> scalar_snapshot,
                                  const std::span<const PhysicalColumnShape> projected_inputs,
                                  std::vector<VectorAggregateDefinition> definitions) {
  auto resources = QueryResourceContext::create(request.limits.maximum_query_memory_bytes);
  if (!resources.has_value())
    return common::make_unexpected(resources.error());
  auto pipeline =
      ScalarSnapshotScanOperator::create(std::move(scalar_snapshot), request.limits.scan);
  if (!pipeline.has_value())
    return common::make_unexpected(pipeline.error());
  const DistributedVectorFragmentDispatch& fragment = request.dispatch.get().dispatch;
  if (fragment.event_time_predicate.has_value()) {
    pipeline =
        TimestampRangeFilterOperator::create(std::move(*pipeline), authority.event_ordinal,
                                             timestamp_predicate(*fragment.event_time_predicate));
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());
  }

  const auto retained_state_bytes =
      common::checked_multiply(definitions.size(), sizeof(MergeableVectorAggregateState));
  if (!retained_state_bytes.has_value() ||
      *retained_state_bytes > request.limits.maximum_retained_configuration_bytes) {
    return common::make_unexpected(
        exhausted("distributed vector aggregate state configuration exceeds its limit"));
  }

  try {
    std::vector<std::size_t> output_ordinals;
    output_ordinals.reserve(fragment.destination_column_ordinals.size());
    for (const std::uint32_t ordinal : fragment.destination_column_ordinals)
      output_ordinals.push_back(ordinal);
    pipeline = SourceColumnOutputOperator::create(std::move(*pipeline), std::move(output_ordinals),
                                                  request.limits.projection);
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());

    std::vector<MergeableVectorAggregateState> states;
    states.reserve(definitions.size());
    const auto retained_capacity =
        common::checked_multiply(states.capacity(), sizeof(MergeableVectorAggregateState));
    if (!retained_capacity.has_value() ||
        *retained_capacity > request.limits.maximum_retained_configuration_bytes) {
      return common::make_unexpected(
          exhausted("distributed vector aggregate retained state exceeds its limit"));
    }
    for (const VectorAggregateDefinition& definition : definitions) {
      auto state = MergeableVectorAggregateState::create(
          definition, request.limits.maximum_variable_extremum_bytes);
      if (!state.has_value())
        return common::make_unexpected(state.error());
      states.push_back(std::move(*state));
    }

    std::uint64_t input_rows{};
    for (;;) {
      auto step = (*pipeline)->next(*resources);
      if (!step.has_value())
        return common::make_unexpected(step.error());
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        break;
      const AccountedVectorChunk* accounted = step->chunk();
      if (accounted == nullptr || !accounted->belongs_to(*resources)) {
        return common::make_unexpected(
            corruption("distributed vector aggregate chunk ownership is invalid"));
      }
      const VectorChunk& chunk = accounted->chunk();
      if (chunk.column_count() != projected_inputs.size()) {
        return common::make_unexpected(
            corruption("distributed vector aggregate projection width differs"));
      }
      for (std::size_t column = 0U; column < projected_inputs.size(); ++column) {
        const columnar::PhysicalColumnView* physical = chunk.column(column);
        if (physical == nullptr || physical->type() != projected_inputs[column].type ||
            physical->nullable() != projected_inputs[column].nullable) {
          return common::make_unexpected(
              corruption("distributed vector aggregate projection shape differs"));
        }
      }
      const std::size_t selected_rows = chunk.selected_row_count();
      if (selected_rows > std::numeric_limits<std::uint64_t>::max() - input_rows)
        return common::make_unexpected(exhausted("distributed vector aggregate rows overflow"));
      for (std::size_t row = 0U; row < selected_rows; ++row) {
        if ((row % 256U) == 0U) {
          const auto active = resources->check_cancelled();
          if (!active.has_value())
            return common::make_unexpected(active.error());
        }
        for (MergeableVectorAggregateState& state : states) {
          const VectorAggregateDefinition& definition = state.definition();
          if (definition.operation == VectorAggregateOperation::kCountStar) {
            auto accumulated = state.accumulate_count_star();
            if (!accumulated.has_value())
              return common::make_unexpected(accumulated.error());
            continue;
          }
          if (!definition.input.has_value()) {
            return common::make_unexpected(
                corruption("distributed vector aggregate input definition disappeared"));
          }
          const VectorAggregateInput& input = definition.input.value();
          auto cell = chunk.cell({.column_ordinal = input.column_ordinal, .selected_row = row});
          if (!cell.has_value())
            return common::make_unexpected(cell.error());
          auto accumulated = state.accumulate_cell(*cell, *resources);
          if (!accumulated.has_value())
            return common::make_unexpected(accumulated.error());
        }
      }
      input_rows += static_cast<std::uint64_t>(selected_rows);
    }

    std::vector<DistributedVectorAggregateExchangeMessage> messages;
    messages.reserve(states.size());
    for (std::size_t ordinal = 0U; ordinal < states.size(); ++ordinal) {
      const bool terminal = ordinal + 1U == states.size();
      messages.emplace_back(
          DistributedVectorAggregateExchangePosition{
              .query_id = fragment.query_id,
              .tablet_id = fragment.tablet_id,
              .sequence = static_cast<std::uint64_t>(ordinal) + 1U,
              .aggregate_ordinal = static_cast<std::uint32_t>(ordinal),
              .terminal = terminal},
          std::move(states[ordinal]));
    }
    return DistributedVectorAggregateWorkerResultV2{.definitions = std::move(definitions),
                                                    .messages = std::move(messages),
                                                    .input_rows = input_rows};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed vector aggregate allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed vector aggregate exceeds container limits"));
  }
}

class VectorAggregatePartBatchConsumer final : public DistributedTemporalPartBatchConsumer {
public:
  VectorAggregatePartBatchConsumer(const DistributedVectorAggregateWorkerRequestV2& request,
                                   const ValidatedWorkerAuthority& authority,
                                   std::vector<PhysicalColumnShape> projected_inputs,
                                   std::vector<VectorAggregateDefinition> definitions) noexcept
      : request_(request), authority_(authority), projected_inputs_(std::move(projected_inputs)),
        definitions_(std::move(definitions)) {}

  common::Status consume(const std::span<const TemporalManifestCsegPartView> parts) override {
    if (called_) {
      repeated_ = true;
      return corruption("distributed vector aggregate part consumer was invoked more than once");
    }
    called_ = true;
    auto visible = resolve_manifest_v2_temporal_tablet_snapshot(
        authority_.get().schema_value, request_.get().lineage.get(), *authority_.get().tablet,
        parts,
        {.source = cseg::temporal_format::CommitSource::kRaft,
         .source_id = request_.get().raft_group_id},
        std::nullopt, request_.get().limits.storage.resolution);
    if (!visible.has_value())
      return visible.error();
    auto executed =
        execute_vector_aggregate_snapshot(request_.get(), authority_.get(), std::move(*visible),
                                          projected_inputs_, std::move(definitions_));
    if (!executed.has_value())
      return executed.error();
    result_ = std::move(*executed);
    return common::Status::ok();
  }

  [[nodiscard]] bool has_exactly_one_call() const noexcept {
    return called_ && !repeated_;
  }

  [[nodiscard]] std::optional<DistributedVectorAggregateWorkerResultV2> take_result() && noexcept {
    return std::move(result_);
  }

private:
  std::reference_wrapper<const DistributedVectorAggregateWorkerRequestV2> request_;
  std::reference_wrapper<const ValidatedWorkerAuthority> authority_;
  std::vector<PhysicalColumnShape> projected_inputs_;
  std::vector<VectorAggregateDefinition> definitions_;
  std::optional<DistributedVectorAggregateWorkerResultV2> result_;
  bool called_{};
  bool repeated_{};
};

[[nodiscard]] bool valid_vector_grouped_aggregate_limits(
    const DistributedVectorGroupedAggregateWorkerLimitsV2& limits) noexcept {
  return limits.maximum_query_memory_bytes > 0U &&
         limits.maximum_query_memory_bytes <= kMaximumDistributedVectorRowsWorkerMemoryBytesV2 &&
         limits.maximum_total_encoded_bytes >=
             distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.maximum_total_encoded_bytes <=
             kMaximumDistributedVectorGroupedWorkerEncodedBytesV2 &&
         limits.maximum_retained_configuration_bytes > 0U &&
         limits.scan.maximum_rows_per_chunk > 0U && limits.scan.chunk.maximum_rows > 0U &&
         limits.scan.chunk.maximum_columns > 0U && limits.scan.chunk.maximum_buffer_bytes > 0U &&
         limits.scan.chunk.maximum_retained_buffer_bytes > 0U &&
         limits.projection.maximum_rows > 0U && limits.projection.maximum_columns > 0U &&
         limits.projection.maximum_buffer_bytes > 0U &&
         limits.projection.maximum_retained_buffer_bytes > 0U &&
         limits.projection.maximum_rows >=
             std::min(limits.scan.maximum_rows_per_chunk, limits.scan.chunk.maximum_rows);
}

struct ValidatedVectorGroupedAggregateWorkerBinding {
  ValidatedWorkerAuthority authority;
  std::vector<PhysicalColumnShape> projected_inputs;
  DistributedVectorGroupedAggregateAuthority grouped;
};

[[nodiscard]] common::Result<ValidatedVectorGroupedAggregateWorkerBinding>
validate_vector_grouped_aggregate_worker_binding(
    const DistributedVectorGroupedAggregateWorkerRequestV2& request) {
  const DistributedVectorFragmentDispatchV2& dispatch = request.dispatch.get();
  const DistributedVectorFragmentDispatch& fragment = dispatch.dispatch;
  const auto structurally_valid = encode_distributed_vector_fragment_dispatch_v2(dispatch);
  if (!structurally_valid.has_value())
    return common::make_unexpected(structurally_valid.error());
  if (!valid_vector_grouped_aggregate_limits(request.limits)) {
    return common::make_unexpected(
        invalid("distributed vector grouped aggregate worker limits are invalid"));
  }
  if (fragment.plan.mode != DistributedVectorPlanMode::kGroupedAggregate) {
    return common::make_unexpected(
        invalid("distributed vector grouped aggregate worker requires a grouped plan"));
  }
  auto authority = validate_worker_authority(
      fragment, fragment.raft_group_id, request.snapshot.get(), request.lineage.get(),
      request.placement.get(), request.raft_group_id, request.local_node,
      request.local_linearizable_barrier);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());

  try {
    std::vector<PhysicalColumnShape> projected_inputs;
    projected_inputs.reserve(fragment.destination_column_ordinals.size());
    for (const std::uint32_t ordinal : fragment.destination_column_ordinals) {
      if (ordinal >= authority->schema_value->columns().size()) {
        return common::make_unexpected(
            invalid("distributed vector grouped aggregate projection is out of bounds"));
      }
      const schema::ColumnDefinition& column = authority->schema_value->columns()[ordinal];
      projected_inputs.push_back({column.type(), column.nullable()});
    }
    auto grouped = bind_distributed_vector_grouped_aggregate_authority(
        fragment.plan, projected_inputs, dispatch.result_schema);
    if (!grouped.has_value())
      return common::make_unexpected(grouped.error());
    if (projected_inputs.size() > request.limits.projection.maximum_columns) {
      return common::make_unexpected(
          exhausted("distributed vector grouped aggregate projection exceeds its width limit"));
    }
    const auto projected_bytes =
        common::checked_multiply(projected_inputs.capacity(), sizeof(PhysicalColumnShape));
    const auto key_bytes =
        common::checked_multiply(grouped->keys.capacity(), sizeof(VectorGroupKeyDefinition) * 2U);
    const auto aggregate_bytes = common::checked_multiply(grouped->aggregates.capacity(),
                                                          sizeof(VectorAggregateDefinition) * 2U);
    const auto first = projected_bytes.has_value() && key_bytes.has_value()
                           ? common::checked_add(*projected_bytes, *key_bytes)
                           : std::nullopt;
    const auto retained = first.has_value() && aggregate_bytes.has_value()
                              ? common::checked_add(*first, *aggregate_bytes)
                              : std::nullopt;
    if (!retained.has_value() || *retained > request.limits.maximum_retained_configuration_bytes) {
      return common::make_unexpected(exhausted(
          "distributed vector grouped aggregate retained configuration exceeds its limit"));
    }
    auto table = MergeableVectorGroupedAggregateTable::create(grouped->keys, grouped->aggregates,
                                                              request.limits.table);
    if (!table.has_value())
      return common::make_unexpected(table.error());
    return ValidatedVectorGroupedAggregateWorkerBinding{.authority = std::move(*authority),
                                                        .projected_inputs =
                                                            std::move(projected_inputs),
                                                        .grouped = std::move(*grouped)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("distributed vector grouped aggregate worker binding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed vector grouped aggregate worker binding exceeds limits"));
  }
}

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateWorkerResultV2>
execute_vector_grouped_aggregate_pipeline(
    const common::Uuid query_id, const schema::TabletId tablet_id,
    std::unique_ptr<PhysicalOperator> pipeline, const QueryResourceContext& resources,
    const std::span<const PhysicalColumnShape> projected_inputs,
    DistributedVectorGroupedAggregateAuthority grouped,
    const DistributedVectorGroupedAggregateWorkerLimitsV2& limits) {
  try {
    auto table = MergeableVectorGroupedAggregateTable::create(grouped.keys, grouped.aggregates,
                                                              limits.table);
    if (!table.has_value())
      return common::make_unexpected(table.error());

    std::uint64_t input_rows{};
    for (;;) {
      auto step = pipeline->next(resources);
      if (!step.has_value())
        return common::make_unexpected(step.error());
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        break;
      const AccountedVectorChunk* chunk = step->chunk();
      if (chunk == nullptr || !chunk->belongs_to(resources)) {
        return common::make_unexpected(
            corruption("distributed vector grouped aggregate chunk ownership is invalid"));
      }
      if (chunk->chunk().column_count() != projected_inputs.size()) {
        return common::make_unexpected(
            corruption("distributed vector grouped aggregate projection width differs"));
      }
      for (std::size_t column = 0U; column < projected_inputs.size(); ++column) {
        const columnar::PhysicalColumnView* physical = chunk->chunk().column(column);
        if (physical == nullptr || physical->type() != projected_inputs[column].type ||
            physical->nullable() != projected_inputs[column].nullable) {
          return common::make_unexpected(
              corruption("distributed vector grouped aggregate projection shape differs"));
        }
      }
      const std::size_t selected_rows = chunk->chunk().selected_row_count();
      if (selected_rows > std::numeric_limits<std::uint64_t>::max() - input_rows) {
        return common::make_unexpected(
            exhausted("distributed vector grouped aggregate rows overflow"));
      }
      auto accumulated = table->accumulate(*chunk, resources);
      if (!accumulated.has_value())
        return common::make_unexpected(accumulated.error());
      input_rows += static_cast<std::uint64_t>(selected_rows);
    }

    const std::size_t group_count = table->group_count();
    std::vector<EncodedDistributedVectorGroupedAggregateExchangeMessage> messages;
    messages.reserve(std::max(group_count, std::size_t{1U}));
    std::size_t encoded_bytes{};
    const auto retain_encoded = [&](EncodedDistributedVectorGroupedAggregateExchangeMessage encoded)
        -> common::Result<void> {
      if (encoded.bytes().size() > limits.maximum_total_encoded_bytes - encoded_bytes) {
        return common::make_unexpected(exhausted(
            "distributed vector grouped aggregate encoded result exceeds its byte limit"));
      }
      encoded_bytes += encoded.bytes().size();
      messages.push_back(std::move(encoded));
      return {};
    };
    if (group_count == 0U) {
      auto encoded = encode_distributed_vector_grouped_aggregate_exchange_message(
          {.query_id = query_id,
           .tablet_id = tablet_id,
           .sequence = 1U,
           .group_ordinal = 0U,
           .group_count = 0U,
           .terminal = true,
           .empty = true},
          std::span<const ScalarValue>{}, std::span<const MergeableVectorAggregateState>{},
          grouped.keys, grouped.aggregates);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      auto retained = retain_encoded(std::move(*encoded));
      if (!retained.has_value())
        return common::make_unexpected(retained.error());
    } else {
      for (std::size_t ordinal = 0U; ordinal < group_count; ++ordinal) {
        auto keys = table->group_keys(ordinal);
        if (!keys.has_value())
          return common::make_unexpected(keys.error());
        auto states = table->group_states(ordinal);
        if (!states.has_value())
          return common::make_unexpected(states.error());
        auto encoded = encode_distributed_vector_grouped_aggregate_exchange_message(
            {.query_id = query_id,
             .tablet_id = tablet_id,
             .sequence = static_cast<std::uint64_t>(ordinal) + 1U,
             .group_ordinal = static_cast<std::uint32_t>(ordinal),
             .group_count = static_cast<std::uint32_t>(group_count),
             .terminal = ordinal + 1U == group_count,
             .empty = false},
            *keys, *states, grouped.keys, grouped.aggregates);
        if (!encoded.has_value())
          return common::make_unexpected(encoded.error());
        auto retained = retain_encoded(std::move(*encoded));
        if (!retained.has_value())
          return common::make_unexpected(retained.error());
      }
    }
    return DistributedVectorGroupedAggregateWorkerResultV2{.authority = std::move(grouped),
                                                           .messages = std::move(messages),
                                                           .input_rows = input_rows,
                                                           .group_count = group_count,
                                                           .encoded_bytes = encoded_bytes};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("distributed vector grouped aggregate worker allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed vector grouped aggregate worker exceeds container limits"));
  }
}

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateWorkerResultV2>
execute_vector_grouped_aggregate_snapshot(
    const DistributedVectorGroupedAggregateWorkerRequestV2& request,
    const ValidatedWorkerAuthority& authority,
    std::shared_ptr<const ScalarTableSnapshot> scalar_snapshot,
    const std::span<const PhysicalColumnShape> projected_inputs,
    DistributedVectorGroupedAggregateAuthority grouped) {
  auto resources = QueryResourceContext::create(request.limits.maximum_query_memory_bytes);
  if (!resources.has_value())
    return common::make_unexpected(resources.error());
  auto pipeline =
      ScalarSnapshotScanOperator::create(std::move(scalar_snapshot), request.limits.scan);
  if (!pipeline.has_value())
    return common::make_unexpected(pipeline.error());
  const DistributedVectorFragmentDispatch& fragment = request.dispatch.get().dispatch;
  if (fragment.event_time_predicate.has_value()) {
    pipeline =
        TimestampRangeFilterOperator::create(std::move(*pipeline), authority.event_ordinal,
                                             timestamp_predicate(*fragment.event_time_predicate));
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());
  }
  try {
    std::vector<std::size_t> output_ordinals;
    output_ordinals.reserve(fragment.destination_column_ordinals.size());
    for (const std::uint32_t ordinal : fragment.destination_column_ordinals)
      output_ordinals.push_back(ordinal);
    pipeline = SourceColumnOutputOperator::create(std::move(*pipeline), std::move(output_ordinals),
                                                  request.limits.projection);
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());
    return execute_vector_grouped_aggregate_pipeline(
        fragment.query_id, fragment.tablet_id, std::move(*pipeline), *resources, projected_inputs,
        std::move(grouped), request.limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("distributed vector grouped aggregate worker allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed vector grouped aggregate worker exceeds container limits"));
  }
}

class VectorGroupedAggregatePartBatchConsumer final : public DistributedTemporalPartBatchConsumer {
public:
  VectorGroupedAggregatePartBatchConsumer(
      const DistributedVectorGroupedAggregateWorkerRequestV2& request,
      const ValidatedWorkerAuthority& authority, std::vector<PhysicalColumnShape> projected_inputs,
      DistributedVectorGroupedAggregateAuthority grouped) noexcept
      : request_(request), authority_(authority), projected_inputs_(std::move(projected_inputs)),
        grouped_(std::move(grouped)) {}

  common::Status consume(const std::span<const TemporalManifestCsegPartView> parts) override {
    if (called_) {
      repeated_ = true;
      return corruption(
          "distributed vector grouped aggregate part consumer was invoked more than once");
    }
    called_ = true;
    auto visible = resolve_manifest_v2_temporal_tablet_snapshot(
        authority_.get().schema_value, request_.get().lineage.get(), *authority_.get().tablet,
        parts,
        {.source = cseg::temporal_format::CommitSource::kRaft,
         .source_id = request_.get().raft_group_id},
        std::nullopt, request_.get().limits.storage.resolution);
    if (!visible.has_value())
      return visible.error();
    auto executed = execute_vector_grouped_aggregate_snapshot(
        request_.get(), authority_.get(), std::move(*visible), projected_inputs_,
        std::move(grouped_));
    if (!executed.has_value())
      return executed.error();
    result_ = std::move(*executed);
    return common::Status::ok();
  }

  [[nodiscard]] bool has_exactly_one_call() const noexcept {
    return called_ && !repeated_;
  }

  [[nodiscard]] std::optional<DistributedVectorGroupedAggregateWorkerResultV2>
  take_result() && noexcept {
    return std::move(result_);
  }

private:
  std::reference_wrapper<const DistributedVectorGroupedAggregateWorkerRequestV2> request_;
  std::reference_wrapper<const ValidatedWorkerAuthority> authority_;
  std::vector<PhysicalColumnShape> projected_inputs_;
  DistributedVectorGroupedAggregateAuthority grouped_;
  std::optional<DistributedVectorGroupedAggregateWorkerResultV2> result_;
  bool called_{};
  bool repeated_{};
};

struct ValidatedMutableVectorGroupedAggregateWorkerBinding {
  std::shared_ptr<const schema::TableSchema> schema_value;
  std::size_t event_ordinal{};
  std::vector<PhysicalColumnShape> projected_inputs;
  DistributedVectorGroupedAggregateAuthority grouped;
};

[[nodiscard]] common::Result<ValidatedMutableVectorGroupedAggregateWorkerBinding>
validate_mutable_vector_grouped_aggregate_worker_binding(
    const DistributedMutableVectorGroupedAggregateWorkerRequest& request) {
  const DistributedMutableVectorFragment& fragment = request.fragment.get();
  const ingest::TabletSnapshot& snapshot = request.snapshot.get();
  const schema::SchemaLineage& lineage = request.lineage.get();
  const raft::TabletPlacementMetadata& placement = request.placement.get();
  const common::Status structural = validate_distributed_mutable_vector_fragment(fragment);
  if (!structural.is_ok())
    return common::make_unexpected(structural);
  if (!valid_vector_grouped_aggregate_limits(request.limits)) {
    return common::make_unexpected(
        invalid("mutable vector grouped aggregate worker limits are invalid"));
  }
  if (fragment.plan.mode != DistributedVectorPlanMode::kGroupedAggregate) {
    return common::make_unexpected(
        invalid("mutable vector grouped aggregate worker requires a grouped plan"));
  }
  if (request.local_node == 0U || request.raft_group_id.is_nil() ||
      fragment.raft_group_id != request.raft_group_id ||
      fragment.serving_node != request.local_node) {
    return common::make_unexpected(unavailable("mutable vector grouped worker route differs"));
  }
  const common::Status placement_status =
      validate_local_placement(placement, fragment, request.local_node);
  if (!placement_status.is_ok())
    return common::make_unexpected(placement_status);
  if (fragment.read_policy.consistency == DistributedReadConsistency::kLeaderLinearizable) {
    if (!request.local_linearizable_barrier.has_value() ||
        request.local_linearizable_barrier != fragment.linearizable_barrier) {
      return common::make_unexpected(
          unavailable("mutable vector grouped worker local Raft read barrier differs"));
    }
  } else if (request.local_linearizable_barrier.has_value()) {
    return common::make_unexpected(
        invalid("mutable vector grouped non-linearizable request supplied a leader barrier"));
  }
  std::shared_ptr<const schema::TableSchema> schema_value = lineage.current();
  const std::optional<head::HeadCommitPosition>& position = snapshot.applied_position();
  if (schema_value == nullptr || lineage.table_id() != fragment.table_id ||
      schema_value->schema_id() != fragment.destination_schema_id ||
      snapshot.table_id() != fragment.table_id || snapshot.tablet_id() != fragment.tablet_id ||
      snapshot.schema_ptr()->schema_id() != fragment.destination_schema_id ||
      !position.has_value() || position->source != head::CommitSource::kRaft ||
      position->raft_group_id != request.raft_group_id ||
      position->record_sequence != fragment.applied_position) {
    return common::make_unexpected(
        unavailable("mutable vector grouped worker publication authority differs"));
  }
  const std::optional<std::size_t> event_ordinal =
      schema_value->column_ordinal(schema_value->event_time_column());
  if (!event_ordinal.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported,
                       "mutable vector grouped worker requires an event-time column"});
  }

  try {
    std::vector<PhysicalColumnShape> projected_inputs;
    std::size_t pre_group_configuration_bytes = 0U;
    if (fragment.pre_group_program.has_value()) {
      projected_inputs.reserve(fragment.pre_group_program->outputs.size());
      for (const VectorExpression& expression : fragment.pre_group_program->outputs) {
        for (const VectorExpressionInstruction& instruction : expression.instructions()) {
          const auto* input = std::get_if<VectorInputExpression>(&instruction);
          if (input == nullptr)
            continue;
          if (input->input_column_ordinal >= schema_value->columns().size())
            return common::make_unexpected(
                invalid("mutable pre-group source ordinal is out of bounds"));
          const schema::ColumnDefinition& column =
              schema_value->columns()[input->input_column_ordinal];
          if (input->type != column.type() || input->nullable != column.nullable())
            return common::make_unexpected(
                unavailable("mutable pre-group source shape differs from local schema"));
        }
        const auto next = common::checked_add(pre_group_configuration_bytes,
                                              expression.retained_configuration_bytes());
        if (!next.has_value())
          return common::make_unexpected(
              exhausted("mutable pre-group configuration size overflowed"));
        pre_group_configuration_bytes = *next;
        projected_inputs.push_back(
            {expression.result_shape().type, expression.result_shape().nullable});
      }
    } else {
      projected_inputs.reserve(fragment.destination_column_ordinals.size());
      for (const std::uint32_t ordinal : fragment.destination_column_ordinals) {
        if (ordinal >= schema_value->columns().size())
          return common::make_unexpected(
              invalid("mutable vector grouped aggregate projection is out of bounds"));
        const schema::ColumnDefinition& column = schema_value->columns()[ordinal];
        projected_inputs.push_back({column.type(), column.nullable()});
      }
    }
    auto grouped = bind_distributed_vector_grouped_aggregate_authority(
        fragment.plan, projected_inputs, fragment.result_schema);
    if (!grouped.has_value())
      return common::make_unexpected(grouped.error());
    if (projected_inputs.size() > request.limits.projection.maximum_columns) {
      return common::make_unexpected(
          exhausted("mutable vector grouped aggregate projection exceeds its width limit"));
    }
    const auto projected_bytes =
        common::checked_multiply(projected_inputs.capacity(), sizeof(PhysicalColumnShape));
    const auto key_bytes =
        common::checked_multiply(grouped->keys.capacity(), sizeof(VectorGroupKeyDefinition) * 2U);
    const auto aggregate_bytes = common::checked_multiply(grouped->aggregates.capacity(),
                                                          sizeof(VectorAggregateDefinition) * 2U);
    const auto first = projected_bytes.has_value() && key_bytes.has_value()
                           ? common::checked_add(*projected_bytes, *key_bytes)
                           : std::nullopt;
    const auto second = first.has_value() && aggregate_bytes.has_value()
                            ? common::checked_add(*first, *aggregate_bytes)
                            : std::nullopt;
    const auto retained = second.has_value()
                              ? common::checked_add(*second, pre_group_configuration_bytes)
                              : std::nullopt;
    if (!retained.has_value() || *retained > request.limits.maximum_retained_configuration_bytes) {
      return common::make_unexpected(
          exhausted("mutable vector grouped aggregate retained configuration exceeds its limit"));
    }
    auto table = MergeableVectorGroupedAggregateTable::create(grouped->keys, grouped->aggregates,
                                                              request.limits.table);
    if (!table.has_value())
      return common::make_unexpected(table.error());
    return ValidatedMutableVectorGroupedAggregateWorkerBinding{
        .schema_value = std::move(schema_value),
        .event_ordinal = *event_ordinal,
        .projected_inputs = std::move(projected_inputs),
        .grouped = std::move(*grouped)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable vector grouped aggregate worker binding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("mutable vector grouped aggregate worker binding exceeds limits"));
  }
}

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
  const auto aggregate_column = aggregate_ordinal(fragment, *authority->schema_value);
  if (!aggregate_column.has_value())
    return common::make_unexpected(aggregate_column.error());
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
                                          *aggregate_column,
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
  const auto aggregate_column = aggregate_ordinal(fragment, *authority->schema_value);
  if (!aggregate_column.has_value())
    return common::make_unexpected(aggregate_column.error());
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
                                        *aggregate_column,
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

common::Result<DistributedVectorRowsWorkerResultV2>
execute_distributed_mutable_vector_rows_fragment(
    const DistributedMutableVectorRowsWorkerRequest& request,
    DistributedVectorRowsChunkConsumerV2& consumer) {
  const DistributedMutableVectorFragment& fragment = request.fragment.get();
  const ingest::TabletSnapshot& snapshot = request.snapshot.get();
  const schema::SchemaLineage& lineage = request.lineage.get();
  const raft::TabletPlacementMetadata& placement = request.placement.get();
  const common::Status structural = validate_distributed_mutable_vector_fragment(fragment);
  if (!structural.is_ok())
    return common::make_unexpected(structural);
  if (fragment.pre_group_program.has_value())
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotSupported, "mutable pre-group rows execution is not supported"});
  if (request.limits.maximum_query_memory_bytes == 0U ||
      request.limits.maximum_query_memory_bytes >
          kMaximumDistributedVectorRowsWorkerMemoryBytesV2 ||
      request.limits.scan.maximum_rows_per_chunk == 0U ||
      request.limits.scan.chunk.maximum_rows == 0U ||
      request.limits.scan.chunk.maximum_columns == 0U ||
      request.limits.scan.chunk.maximum_buffer_bytes == 0U ||
      request.limits.scan.chunk.maximum_retained_buffer_bytes == 0U ||
      request.limits.output.maximum_rows == 0U || request.limits.output.maximum_columns == 0U ||
      request.limits.output.maximum_buffer_bytes == 0U ||
      request.limits.output.maximum_retained_buffer_bytes == 0U ||
      request.limits.output.maximum_rows < std::min(request.limits.scan.maximum_rows_per_chunk,
                                                    request.limits.scan.chunk.maximum_rows)) {
    return common::make_unexpected(invalid("mutable vector worker limits are invalid"));
  }
  if (fragment.plan.mode != DistributedVectorPlanMode::kRows) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported,
                       "mutable vector row worker requires a row-mode fragment"});
  }
  if (request.local_node == 0U || request.raft_group_id.is_nil() ||
      fragment.raft_group_id != request.raft_group_id ||
      fragment.serving_node != request.local_node) {
    return common::make_unexpected(unavailable("mutable vector worker route differs"));
  }
  const common::Status placement_status =
      validate_local_placement(placement, fragment, request.local_node);
  if (!placement_status.is_ok())
    return common::make_unexpected(placement_status);
  if (fragment.read_policy.consistency == DistributedReadConsistency::kLeaderLinearizable) {
    if (!request.local_linearizable_barrier.has_value() ||
        request.local_linearizable_barrier != fragment.linearizable_barrier) {
      return common::make_unexpected(
          unavailable("mutable vector worker local Raft read barrier differs"));
    }
  } else if (request.local_linearizable_barrier.has_value()) {
    return common::make_unexpected(
        invalid("mutable vector non-linearizable request supplied a leader barrier"));
  }
  const std::shared_ptr<const schema::TableSchema> schema_value = lineage.current();
  const std::optional<head::HeadCommitPosition>& position = snapshot.applied_position();
  if (schema_value == nullptr || lineage.table_id() != fragment.table_id ||
      schema_value->schema_id() != fragment.destination_schema_id ||
      snapshot.table_id() != fragment.table_id || snapshot.tablet_id() != fragment.tablet_id ||
      snapshot.schema_ptr()->schema_id() != fragment.destination_schema_id ||
      !position.has_value() || position->source != head::CommitSource::kRaft ||
      position->raft_group_id != request.raft_group_id ||
      position->record_sequence != fragment.applied_position) {
    return common::make_unexpected(
        unavailable("mutable vector worker publication authority differs"));
  }
  const std::optional<std::size_t> event_ordinal =
      schema_value->column_ordinal(schema_value->event_time_column());
  if (!event_ordinal.has_value()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotSupported, "mutable vector worker requires an event-time column"});
  }

  try {
    std::vector<PhysicalColumnShape> projected_inputs;
    projected_inputs.reserve(fragment.destination_column_ordinals.size());
    for (const std::uint32_t ordinal : fragment.destination_column_ordinals) {
      if (ordinal >= schema_value->columns().size()) {
        return common::make_unexpected(
            invalid("mutable vector worker projection is out of bounds"));
      }
      const schema::ColumnDefinition& column = schema_value->columns()[ordinal];
      projected_inputs.push_back({column.type(), column.nullable()});
    }
    const common::Status result_status = validate_distributed_vector_result_schema(
        fragment.plan, projected_inputs, fragment.result_schema);
    if (!result_status.is_ok())
      return common::make_unexpected(result_status);

    auto resources = QueryResourceContext::create(request.limits.maximum_query_memory_bytes);
    if (!resources.has_value())
      return common::make_unexpected(resources.error());
    std::vector<PhysicalColumnShape> source_shape;
    source_shape.reserve(schema_value->columns().size());
    for (const schema::ColumnDefinition& column : schema_value->columns())
      source_shape.push_back({column.type(), column.nullable()});
    auto pipeline = create_tablet_states_source(*resources, std::span{&snapshot, 1U}, lineage,
                                                fragment.destination_schema_id, source_shape,
                                                {.scan = {.chunk = request.limits.scan.chunk}});
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());
    if (fragment.event_time_predicate.has_value()) {
      pipeline =
          TimestampRangeFilterOperator::create(std::move(*pipeline), *event_ordinal,
                                               timestamp_predicate(*fragment.event_time_predicate));
      if (!pipeline.has_value())
        return common::make_unexpected(pipeline.error());
    }
    std::vector<std::size_t> output_ordinals;
    output_ordinals.reserve(fragment.plan.row_output_indices.size());
    for (const std::uint32_t projected_index : fragment.plan.row_output_indices)
      output_ordinals.push_back(fragment.destination_column_ordinals[projected_index]);
    pipeline = SourceColumnOutputOperator::create(std::move(*pipeline), std::move(output_ordinals),
                                                  request.limits.output);
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());

    DistributedVectorRowsWorkerResultV2 result;
    for (;;) {
      auto step = (*pipeline)->next(*resources);
      if (!step.has_value())
        return common::make_unexpected(step.error());
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        return result;
      const AccountedVectorChunk* accounted = step->chunk();
      if (accounted == nullptr || !accounted->belongs_to(*resources)) {
        return common::make_unexpected(
            corruption("mutable vector worker chunk ownership is invalid"));
      }
      const VectorChunk& chunk = accounted->chunk();
      if (chunk.column_count() != fragment.result_schema.columns.size()) {
        return common::make_unexpected(
            corruption("mutable vector worker output width differs from its schema"));
      }
      for (std::size_t column = 0U; column < chunk.column_count(); ++column) {
        const columnar::PhysicalColumnView* physical = chunk.column(column);
        const DistributedVectorResultColumn& expected = fragment.result_schema.columns[column];
        if (physical == nullptr || physical->type() != expected.type ||
            physical->nullable() != expected.nullable) {
          return common::make_unexpected(
              corruption("mutable vector worker output shape differs from its schema"));
        }
      }
      const std::size_t rows = chunk.selected_row_count();
      if (rows == 0U)
        continue;
      if (rows > std::numeric_limits<std::uint64_t>::max() - result.output_rows ||
          result.output_chunks == std::numeric_limits<std::size_t>::max()) {
        return common::make_unexpected(exhausted("mutable vector worker output overflows"));
      }
      const common::Status consumed = consumer.consume(chunk);
      if (!consumed.is_ok())
        return common::make_unexpected(consumed);
      result.output_rows += static_cast<std::uint64_t>(rows);
      ++result.output_chunks;
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector worker allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable vector worker exceeds limits"));
  }
}

common::Result<DistributedVectorRowsWorkerResultV2>
execute_distributed_vector_rows_fragment_v2(const DistributedVectorRowsWorkerRequestV2& request,
                                            DistributedVectorRowsChunkConsumerV2& consumer) {
  const LocalTemporalPartBatchLoader loader{request.storage.get()};
  return execute_distributed_vector_rows_fragment_v2(request, loader, consumer);
}

common::Result<DistributedVectorRowsWorkerResultV2>
execute_distributed_vector_rows_fragment_v2(const DistributedVectorRowsWorkerRequestV2& request,
                                            const DistributedTemporalPartBatchLoader& loader,
                                            DistributedVectorRowsChunkConsumerV2& consumer) {
  const DistributedVectorFragmentDispatchV2& dispatch = request.dispatch.get();
  const DistributedVectorFragmentDispatch& fragment = dispatch.dispatch;
  const manifest::TemporalDatabaseStorageSnapshot& snapshot = request.snapshot.get();
  const schema::SchemaLineage& lineage = request.lineage.get();
  const raft::TabletPlacementMetadata& placement = request.placement.get();

  const auto structurally_valid = encode_distributed_vector_fragment_dispatch_v2(dispatch);
  if (!structurally_valid.has_value())
    return common::make_unexpected(structurally_valid.error());
  if (request.limits.maximum_query_memory_bytes == 0U ||
      request.limits.maximum_query_memory_bytes >
          kMaximumDistributedVectorRowsWorkerMemoryBytesV2 ||
      request.limits.scan.maximum_rows_per_chunk == 0U ||
      request.limits.scan.chunk.maximum_rows == 0U ||
      request.limits.scan.chunk.maximum_columns == 0U ||
      request.limits.scan.chunk.maximum_buffer_bytes == 0U ||
      request.limits.scan.chunk.maximum_retained_buffer_bytes == 0U ||
      request.limits.output.maximum_rows == 0U || request.limits.output.maximum_columns == 0U ||
      request.limits.output.maximum_buffer_bytes == 0U ||
      request.limits.output.maximum_retained_buffer_bytes == 0U ||
      request.limits.output.maximum_rows < std::min(request.limits.scan.maximum_rows_per_chunk,
                                                    request.limits.scan.chunk.maximum_rows)) {
    return common::make_unexpected(invalid("distributed vector worker limits are invalid"));
  }
  if (fragment.plan.mode != DistributedVectorPlanMode::kRows) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported,
                       "distributed vector worker requires mergeable aggregate-state transport"});
  }
  auto authority = validate_worker_authority(fragment, fragment.raft_group_id, snapshot, lineage,
                                             placement, request.raft_group_id, request.local_node,
                                             request.local_linearizable_barrier);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());

  try {
    std::vector<PhysicalColumnShape> projected_inputs;
    projected_inputs.reserve(fragment.destination_column_ordinals.size());
    for (const std::uint32_t ordinal : fragment.destination_column_ordinals) {
      if (ordinal >= authority->schema_value->columns().size()) {
        return common::make_unexpected(
            invalid("distributed vector worker projection is out of bounds"));
      }
      const schema::ColumnDefinition& column = authority->schema_value->columns()[ordinal];
      projected_inputs.push_back({column.type(), column.nullable()});
    }
    const common::Status result_schema_status = validate_distributed_vector_result_schema(
        fragment.plan, projected_inputs, dispatch.result_schema);
    if (!result_schema_status.is_ok())
      return common::make_unexpected(result_schema_status);

    const manifest::TemporalTabletDescriptor& tablet = *authority->tablet;
    if (tablet.part_count == 0U) {
      if (tablet.durable_version_count != 0U) {
        return common::make_unexpected(
            corruption("distributed vector worker empty part set has durable rows"));
      }
      auto empty =
          ScalarTableSnapshot::create(authority->schema_value, tablet.durable_position, {});
      if (!empty.has_value())
        return common::make_unexpected(empty.error());
      return execute_vector_rows_snapshot(
          request, *authority, std::make_shared<const ScalarTableSnapshot>(std::move(*empty)),
          consumer);
    }

    const std::span<const manifest::TemporalPartDescriptor> descriptors =
        snapshot.parts().subspan(static_cast<std::size_t>(tablet.first_part_index),
                                 static_cast<std::size_t>(tablet.part_count));
    std::vector<cseg::PartId> part_ids;
    part_ids.reserve(descriptors.size());
    for (const manifest::TemporalPartDescriptor& descriptor : descriptors)
      part_ids.push_back(descriptor.part_id);
    const std::array bindings{manifest::TabletSchemaBinding{.tablet_id = fragment.tablet_id,
                                                            .lineage = std::cref(lineage)}};
    VectorRowsPartBatchConsumer part_consumer{request, *authority, consumer};
    const common::Status loaded = loader.load(
        snapshot, part_ids, bindings, request.limits.storage.part_validation, part_consumer);
    if (!loaded.is_ok())
      return common::make_unexpected(loaded);
    if (!part_consumer.has_exactly_one_call() || !part_consumer.result().has_value()) {
      return common::make_unexpected(
          corruption("distributed vector part loader did not invoke its consumer exactly once"));
    }
    // Guarded by the complete exactly-once result check above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    return *part_consumer.result();
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed vector worker allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("distributed vector worker exceeds container limits"));
  }
}

common::Result<DistributedVectorAggregateWorkerResultV2>
execute_distributed_vector_aggregate_fragment_v2(
    const DistributedVectorAggregateWorkerRequestV2& request) {
  const LocalTemporalPartBatchLoader loader{request.storage.get()};
  return execute_distributed_vector_aggregate_fragment_v2(request, loader);
}

common::Result<std::vector<VectorAggregateDefinition>>
bind_distributed_vector_aggregate_worker_definitions_v2(
    const DistributedVectorAggregateWorkerRequestV2& request) {
  auto binding = validate_vector_aggregate_worker_binding(request);
  if (!binding.has_value())
    return common::make_unexpected(binding.error());
  return std::move(binding->definitions);
}

common::Result<DistributedVectorAggregateWorkerResultV2>
execute_distributed_vector_aggregate_fragment_v2(
    const DistributedVectorAggregateWorkerRequestV2& request,
    const DistributedTemporalPartBatchLoader& loader) {
  auto binding = validate_vector_aggregate_worker_binding(request);
  if (!binding.has_value())
    return common::make_unexpected(binding.error());
  const DistributedVectorFragmentDispatchV2& dispatch = request.dispatch.get();
  const DistributedVectorFragmentDispatch& fragment = dispatch.dispatch;
  const manifest::TemporalDatabaseStorageSnapshot& snapshot = request.snapshot.get();
  const schema::SchemaLineage& lineage = request.lineage.get();
  ValidatedWorkerAuthority* const authority = &binding->authority;

  try {
    std::vector<PhysicalColumnShape> projected_inputs = std::move(binding->projected_inputs);
    std::vector<VectorAggregateDefinition> definitions = std::move(binding->definitions);

    const manifest::TemporalTabletDescriptor& tablet = *authority->tablet;
    if (tablet.part_count == 0U) {
      if (tablet.durable_version_count != 0U) {
        return common::make_unexpected(
            corruption("distributed vector aggregate empty part set has durable rows"));
      }
      auto empty =
          ScalarTableSnapshot::create(authority->schema_value, tablet.durable_position, {});
      if (!empty.has_value())
        return common::make_unexpected(empty.error());
      return execute_vector_aggregate_snapshot(
          request, *authority, std::make_shared<const ScalarTableSnapshot>(std::move(*empty)),
          projected_inputs, std::move(definitions));
    }

    const std::span<const manifest::TemporalPartDescriptor> descriptors =
        snapshot.parts().subspan(static_cast<std::size_t>(tablet.first_part_index),
                                 static_cast<std::size_t>(tablet.part_count));
    std::vector<cseg::PartId> part_ids;
    part_ids.reserve(descriptors.size());
    for (const manifest::TemporalPartDescriptor& descriptor : descriptors)
      part_ids.push_back(descriptor.part_id);
    const std::array bindings{manifest::TabletSchemaBinding{.tablet_id = fragment.tablet_id,
                                                            .lineage = std::cref(lineage)}};
    VectorAggregatePartBatchConsumer consumer{request, *authority, std::move(projected_inputs),
                                              std::move(definitions)};
    const common::Status loaded =
        loader.load(snapshot, part_ids, bindings, request.limits.storage.part_validation, consumer);
    if (!loaded.is_ok())
      return common::make_unexpected(loaded);
    if (!consumer.has_exactly_one_call()) {
      return common::make_unexpected(corruption(
          "distributed vector aggregate part loader did not invoke its consumer exactly once"));
    }
    auto result = std::move(consumer).take_result();
    if (!result.has_value()) {
      return common::make_unexpected(
          corruption("distributed vector aggregate part consumer produced no result"));
    }
    return std::move(*result);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("distributed vector aggregate worker allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed vector aggregate worker exceeds container limits"));
  }
}

common::Result<DistributedVectorGroupedAggregateWorkerResultV2>
execute_distributed_vector_grouped_aggregate_fragment_v2(
    const DistributedVectorGroupedAggregateWorkerRequestV2& request) {
  const LocalTemporalPartBatchLoader loader{request.storage.get()};
  return execute_distributed_vector_grouped_aggregate_fragment_v2(request, loader);
}

common::Result<DistributedVectorGroupedAggregateAuthority>
bind_distributed_vector_grouped_aggregate_worker_authority_v2(
    const DistributedVectorGroupedAggregateWorkerRequestV2& request) {
  auto binding = validate_vector_grouped_aggregate_worker_binding(request);
  if (!binding.has_value())
    return common::make_unexpected(binding.error());
  return std::move(binding->grouped);
}

common::Result<DistributedVectorGroupedAggregateWorkerResultV2>
execute_distributed_vector_grouped_aggregate_fragment_v2(
    const DistributedVectorGroupedAggregateWorkerRequestV2& request,
    const DistributedTemporalPartBatchLoader& loader) {
  auto binding = validate_vector_grouped_aggregate_worker_binding(request);
  if (!binding.has_value())
    return common::make_unexpected(binding.error());
  const DistributedVectorFragmentDispatch& fragment = request.dispatch.get().dispatch;
  const manifest::TemporalDatabaseStorageSnapshot& snapshot = request.snapshot.get();
  const schema::SchemaLineage& lineage = request.lineage.get();
  ValidatedWorkerAuthority* const authority = &binding->authority;

  try {
    std::vector<PhysicalColumnShape> projected_inputs = std::move(binding->projected_inputs);
    DistributedVectorGroupedAggregateAuthority grouped = std::move(binding->grouped);
    const manifest::TemporalTabletDescriptor& tablet = *authority->tablet;
    if (tablet.part_count == 0U) {
      if (tablet.durable_version_count != 0U) {
        return common::make_unexpected(
            corruption("distributed vector grouped aggregate empty part set has durable rows"));
      }
      auto empty =
          ScalarTableSnapshot::create(authority->schema_value, tablet.durable_position, {});
      if (!empty.has_value())
        return common::make_unexpected(empty.error());
      return execute_vector_grouped_aggregate_snapshot(
          request, *authority, std::make_shared<const ScalarTableSnapshot>(std::move(*empty)),
          projected_inputs, std::move(grouped));
    }

    const std::span<const manifest::TemporalPartDescriptor> descriptors =
        snapshot.parts().subspan(static_cast<std::size_t>(tablet.first_part_index),
                                 static_cast<std::size_t>(tablet.part_count));
    std::vector<cseg::PartId> part_ids;
    part_ids.reserve(descriptors.size());
    for (const manifest::TemporalPartDescriptor& descriptor : descriptors)
      part_ids.push_back(descriptor.part_id);
    const std::array bindings{manifest::TabletSchemaBinding{.tablet_id = fragment.tablet_id,
                                                            .lineage = std::cref(lineage)}};
    VectorGroupedAggregatePartBatchConsumer consumer{
        request, *authority, std::move(projected_inputs), std::move(grouped)};
    const common::Status loaded =
        loader.load(snapshot, part_ids, bindings, request.limits.storage.part_validation, consumer);
    if (!loaded.is_ok())
      return common::make_unexpected(loaded);
    if (!consumer.has_exactly_one_call()) {
      return common::make_unexpected(corruption("distributed vector grouped aggregate part loader "
                                                "did not invoke its consumer exactly once"));
    }
    auto result = std::move(consumer).take_result();
    if (!result.has_value()) {
      return common::make_unexpected(
          corruption("distributed vector grouped aggregate part consumer produced no result"));
    }
    return std::move(*result);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("distributed vector grouped aggregate worker allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("distributed vector grouped aggregate worker exceeds container limits"));
  }
}

common::Result<DistributedVectorGroupedAggregateAuthority>
bind_distributed_mutable_vector_grouped_aggregate_worker_authority(
    const DistributedMutableVectorGroupedAggregateWorkerRequest& request) {
  auto binding = validate_mutable_vector_grouped_aggregate_worker_binding(request);
  if (!binding.has_value())
    return common::make_unexpected(binding.error());
  return std::move(binding->grouped);
}

common::Result<DistributedVectorGroupedAggregateWorkerResultV2>
execute_distributed_mutable_vector_grouped_aggregate_fragment(
    const DistributedMutableVectorGroupedAggregateWorkerRequest& request) {
  auto binding = validate_mutable_vector_grouped_aggregate_worker_binding(request);
  if (!binding.has_value())
    return common::make_unexpected(binding.error());
  const DistributedMutableVectorFragment& fragment = request.fragment.get();
  const ingest::TabletSnapshot& snapshot = request.snapshot.get();
  const schema::SchemaLineage& lineage = request.lineage.get();
  try {
    auto resources = QueryResourceContext::create(request.limits.maximum_query_memory_bytes);
    if (!resources.has_value())
      return common::make_unexpected(resources.error());
    std::vector<PhysicalColumnShape> source_shape;
    source_shape.reserve(binding->schema_value->columns().size());
    for (const schema::ColumnDefinition& column : binding->schema_value->columns())
      source_shape.push_back({column.type(), column.nullable()});
    auto pipeline = create_tablet_states_source(*resources, std::span{&snapshot, 1U}, lineage,
                                                fragment.destination_schema_id, source_shape,
                                                {.scan = {.chunk = request.limits.scan.chunk}});
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());
    if (fragment.event_time_predicate.has_value()) {
      pipeline =
          TimestampRangeFilterOperator::create(std::move(*pipeline), binding->event_ordinal,
                                               timestamp_predicate(*fragment.event_time_predicate));
      if (!pipeline.has_value())
        return common::make_unexpected(pipeline.error());
    }
    if (fragment.pre_group_program.has_value()) {
      std::vector<ColumnOutputPosition> outputs;
      outputs.reserve(fragment.pre_group_program->outputs.size());
      for (const VectorExpression& expression : fragment.pre_group_program->outputs)
        outputs.emplace_back(ComputedColumnOutputPosition{expression});
      pipeline = ColumnOutputOperator::create(std::move(*pipeline), std::move(outputs),
                                              request.limits.projection);
    } else {
      std::vector<std::size_t> output_ordinals;
      output_ordinals.reserve(fragment.destination_column_ordinals.size());
      for (const std::uint32_t ordinal : fragment.destination_column_ordinals)
        output_ordinals.push_back(ordinal);
      pipeline = SourceColumnOutputOperator::create(
          std::move(*pipeline), std::move(output_ordinals), request.limits.projection);
    }
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());
    return execute_vector_grouped_aggregate_pipeline(
        fragment.query_id, fragment.tablet_id, std::move(*pipeline), *resources,
        binding->projected_inputs, std::move(binding->grouped), request.limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable vector grouped aggregate worker allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("mutable vector grouped aggregate worker exceeds container limits"));
  }
}

} // namespace chronos::query
