#ifndef CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_BINDING_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_BINDING_HPP_

#include "chronos/common/result.hpp"
#include "chronos/cseg/pruning.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/query/distributed_fragment_dispatch.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/raft/types.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <vector>

namespace chronos::query {

// Borrows one acquire-loaded storage epoch and one committed placement view only for the duration
// of binding. The returned dispatch owns every field needed by the authenticated worker boundary.
struct DistributedAggregateFragmentBinding {
  std::reference_wrapper<const DistributedAggregatePlan> plan;
  std::reference_wrapper<const DistributedReadAdmission> admission;
  std::reference_wrapper<const manifest::TemporalDatabaseStorageSnapshot> snapshot;
  std::reference_wrapper<const schema::TableSchema> destination_schema;
  common::Uuid raft_group_id;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::uint32_t aggregate_input_index{};
  std::optional<cseg::EventTimePredicate> event_time_predicate;
};

// Reproves admission, committed placement, Raft source identity, exact durable boundary, and
// destination schema/projection against one immutable Manifest v2 epoch before constructing a
// group-scoped executable request. It performs no I/O and publishes no state.
[[nodiscard]] common::Result<DistributedAggregateFragmentDispatch>
bind_distributed_aggregate_fragment(const DistributedAggregateFragmentBinding& binding);

struct DistributedAggregateSnapshotFragmentBinding {
  std::reference_wrapper<const DistributedReadAdmission> admission;
  std::reference_wrapper<const schema::TableSchema> destination_schema;
  common::Uuid raft_group_id;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::uint32_t aggregate_input_index{};
  std::optional<cseg::EventTimePredicate> event_time_predicate;
};

inline constexpr std::size_t kMaximumDistributedSnapshotProjectionOrdinals =
    DistributedPlanLimits{}.maximum_fragments * schema::kMaximumSchemaColumnCount;

struct DistributedAggregateSnapshotBindingLimits {
  std::size_t maximum_fragments{DistributedPlanLimits{}.maximum_fragments};
  std::size_t maximum_total_projection_ordinals{65'536U};
};

// Owns the one acquire-loaded Manifest v2 epoch that supplied every dispatch. The dispatch vector
// is in exact plan order and cannot contain mixed database/generation snapshots.
class CompatibleDistributedAggregateSnapshot {
public:
  CompatibleDistributedAggregateSnapshot() = delete;
  CompatibleDistributedAggregateSnapshot(const CompatibleDistributedAggregateSnapshot&) = delete;
  CompatibleDistributedAggregateSnapshot&
  operator=(const CompatibleDistributedAggregateSnapshot&) = delete;
  CompatibleDistributedAggregateSnapshot(CompatibleDistributedAggregateSnapshot&&) noexcept =
      default;
  CompatibleDistributedAggregateSnapshot&
  operator=(CompatibleDistributedAggregateSnapshot&&) noexcept = default;

  [[nodiscard]] const manifest::TemporalDatabaseStorageSnapshot& snapshot() const noexcept;
  [[nodiscard]] std::span<const DistributedAggregateFragmentDispatch> dispatches() const noexcept;

private:
  CompatibleDistributedAggregateSnapshot(
      manifest::TemporalDatabaseStorageSnapshot snapshot,
      std::vector<DistributedAggregateFragmentDispatch> dispatches) noexcept;

  manifest::TemporalDatabaseStorageSnapshot snapshot_;
  std::vector<DistributedAggregateFragmentDispatch> dispatches_;

  friend common::Result<CompatibleDistributedAggregateSnapshot>
  bind_compatible_distributed_aggregate_snapshot(
      const DistributedAggregatePlan&, manifest::TemporalDatabaseStorageSnapshot,
      std::span<const DistributedAggregateSnapshotFragmentBinding>,
      DistributedAggregateSnapshotBindingLimits);
};

// Binds every planned tablet against one acquire-pinned database epoch. Binding count/order,
// per-tablet admission/placement/group/schema authority, and aggregate projection limits are
// validated before the owning compatible snapshot is returned.
[[nodiscard]] common::Result<CompatibleDistributedAggregateSnapshot>
bind_compatible_distributed_aggregate_snapshot(
    const DistributedAggregatePlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    std::span<const DistributedAggregateSnapshotFragmentBinding> bindings,
    DistributedAggregateSnapshotBindingLimits limits = {});

// One plan-ordered runtime proof. Placement, group, and schema authority are deliberately absent:
// the metadata-backed binder resolves those fields from one committed catalog snapshot. A
// follower-bounded read must carry a fresh leader-commit observation; a leader-linearizable read
// must instead carry the completed barrier from the observed leader term.
struct DistributedAggregateReplicaProof {
  std::reference_wrapper<const raft::RaftGroupObservation> observation;
  std::optional<raft::LogIndex> observed_leader_commit_position;
  std::optional<raft::ReadBarrier> linearizable_barrier;
};

struct MetadataBackedDistributedAggregateSnapshotBinding {
  std::reference_wrapper<const raft::MetadataCatalogSnapshot> catalog;
  schema::TableId table_id;
  std::span<const DistributedAggregateReplicaProof> replica_proofs;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::uint32_t aggregate_input_index{};
  std::optional<cseg::EventTimePredicate> event_time_predicate;
};

// Resolves every planned tablet's active schema, committed placement, and immutable Raft group
// from one canonical metadata snapshot. Stable membership and policy-specific runtime proofs are
// exact-validated before the existing compatible Manifest snapshot binder constructs requests.
[[nodiscard]] common::Result<CompatibleDistributedAggregateSnapshot>
bind_metadata_backed_distributed_aggregate_snapshot(
    const DistributedAggregatePlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const MetadataBackedDistributedAggregateSnapshotBinding& binding,
    DistributedAggregateSnapshotBindingLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_BINDING_HPP_
