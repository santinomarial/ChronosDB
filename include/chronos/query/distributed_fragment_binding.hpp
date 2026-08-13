#ifndef CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_BINDING_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_BINDING_HPP_

#include "chronos/common/result.hpp"
#include "chronos/cseg/pruning.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/query/distributed_fragment_dispatch.hpp"
#include "chronos/query/distributed_vector_fragment.hpp"
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

struct DistributedVectorFragmentBinding {
  std::reference_wrapper<const DistributedVectorQueryPlan> plan;
  std::reference_wrapper<const DistributedReadAdmission> admission;
  std::reference_wrapper<const manifest::TemporalDatabaseStorageSnapshot> snapshot;
  std::reference_wrapper<const schema::TableSchema> destination_schema;
  common::Uuid raft_group_id;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::optional<cseg::EventTimePredicate> event_time_predicate;
};

// Derives one owning vector dispatch only after exact read admission, committed placement,
// Manifest-v2 source/position/schema, projection, and plan input types agree.
[[nodiscard]] common::Result<DistributedVectorFragmentDispatch>
bind_distributed_vector_fragment(const DistributedVectorFragmentBinding& binding);

struct DistributedGroupedFloat64FragmentBinding {
  DistributedAggregateFragmentBinding aggregate;
  std::uint32_t group_key_input_index{};
};

// Owns the exact Raft group and grouped intent after the existing aggregate binder has revalidated
// all snapshot/placement/proof authority and this binder has proved the projected key is FLOAT64.
struct BoundDistributedGroupedFloat64Fragment {
  common::Uuid raft_group_id;
  DistributedGroupedFloat64Fragment fragment;

  friend bool operator==(const BoundDistributedGroupedFloat64Fragment&,
                         const BoundDistributedGroupedFloat64Fragment&) = default;
};

[[nodiscard]] common::Result<BoundDistributedGroupedFloat64Fragment>
bind_distributed_grouped_float64_fragment(const DistributedGroupedFloat64FragmentBinding& binding);

// Packages the authority-bound owned values directly into the sole canonical grouped worker
// dispatch value, so callers never separately join the validated group and intent.
[[nodiscard]] common::Result<DistributedGroupedFloat64FragmentDispatch>
bind_distributed_grouped_float64_fragment_dispatch(
    const DistributedGroupedFloat64FragmentBinding& binding);

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

struct DistributedVectorSnapshotFragmentBinding {
  std::reference_wrapper<const DistributedReadAdmission> admission;
  std::reference_wrapper<const schema::TableSchema> destination_schema;
  common::Uuid raft_group_id;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::optional<cseg::EventTimePredicate> event_time_predicate;
};

struct DistributedVectorSnapshotBindingLimits {
  std::size_t maximum_fragments{DistributedPlanLimits{}.maximum_fragments};
  std::size_t maximum_total_projection_ordinals{65'536U};
};

class CompatibleDistributedVectorSnapshot {
public:
  CompatibleDistributedVectorSnapshot() = delete;
  CompatibleDistributedVectorSnapshot(const CompatibleDistributedVectorSnapshot&) = delete;
  CompatibleDistributedVectorSnapshot&
  operator=(const CompatibleDistributedVectorSnapshot&) = delete;
  CompatibleDistributedVectorSnapshot(CompatibleDistributedVectorSnapshot&&) noexcept = default;
  CompatibleDistributedVectorSnapshot&
  operator=(CompatibleDistributedVectorSnapshot&&) noexcept = default;

  [[nodiscard]] const manifest::TemporalDatabaseStorageSnapshot& snapshot() const noexcept;
  [[nodiscard]] std::span<const DistributedVectorFragmentDispatch> dispatches() const noexcept;

private:
  CompatibleDistributedVectorSnapshot(
      manifest::TemporalDatabaseStorageSnapshot snapshot,
      std::vector<DistributedVectorFragmentDispatch> dispatches) noexcept;

  manifest::TemporalDatabaseStorageSnapshot snapshot_;
  std::vector<DistributedVectorFragmentDispatch> dispatches_;

  friend common::Result<CompatibleDistributedVectorSnapshot>
  bind_compatible_distributed_vector_snapshot(
      const DistributedVectorQueryPlan&, manifest::TemporalDatabaseStorageSnapshot,
      std::span<const DistributedVectorSnapshotFragmentBinding>,
      DistributedVectorSnapshotBindingLimits);
};

[[nodiscard]] common::Result<CompatibleDistributedVectorSnapshot>
bind_compatible_distributed_vector_snapshot(
    const DistributedVectorQueryPlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    std::span<const DistributedVectorSnapshotFragmentBinding> bindings,
    DistributedVectorSnapshotBindingLimits limits = {});

// Owns the same one acquire-pinned Manifest epoch as the compatible aggregate snapshot plus one
// plan-ordered grouped dispatch per aggregate dispatch. Group-key type proof is performed under the
// exact destination schema binding used for each nested aggregate fragment.
class CompatibleDistributedGroupedFloat64Snapshot {
public:
  CompatibleDistributedGroupedFloat64Snapshot() = delete;
  CompatibleDistributedGroupedFloat64Snapshot(const CompatibleDistributedGroupedFloat64Snapshot&) =
      delete;
  CompatibleDistributedGroupedFloat64Snapshot&
  operator=(const CompatibleDistributedGroupedFloat64Snapshot&) = delete;
  CompatibleDistributedGroupedFloat64Snapshot(
      CompatibleDistributedGroupedFloat64Snapshot&&) noexcept = default;
  CompatibleDistributedGroupedFloat64Snapshot&
  operator=(CompatibleDistributedGroupedFloat64Snapshot&&) noexcept = default;

  [[nodiscard]] const manifest::TemporalDatabaseStorageSnapshot& snapshot() const noexcept;
  [[nodiscard]] std::span<const DistributedGroupedFloat64FragmentDispatch>
  dispatches() const noexcept;

private:
  CompatibleDistributedGroupedFloat64Snapshot(
      CompatibleDistributedAggregateSnapshot aggregate_snapshot,
      std::vector<DistributedGroupedFloat64FragmentDispatch> dispatches) noexcept;

  CompatibleDistributedAggregateSnapshot aggregate_snapshot_;
  std::vector<DistributedGroupedFloat64FragmentDispatch> dispatches_;

  friend common::Result<CompatibleDistributedGroupedFloat64Snapshot>
  bind_compatible_distributed_grouped_float64_snapshot(
      const DistributedAggregatePlan&, manifest::TemporalDatabaseStorageSnapshot,
      std::span<const DistributedAggregateSnapshotFragmentBinding>, std::uint32_t,
      DistributedAggregateSnapshotBindingLimits);
  friend common::Result<CompatibleDistributedGroupedFloat64Snapshot>
  bind_compatible_distributed_grouped_float64_snapshot(CompatibleDistributedAggregateSnapshot,
                                                       const schema::TableSchema&, std::uint32_t);
};

// Delegates every plan/order/placement/proof/projection/Manifest check to the aggregate batch
// binder, then proves the shared projected group-key input is FLOAT64 under every exact schema.
[[nodiscard]] common::Result<CompatibleDistributedGroupedFloat64Snapshot>
bind_compatible_distributed_grouped_float64_snapshot(
    const DistributedAggregatePlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    std::span<const DistributedAggregateSnapshotFragmentBinding> bindings,
    std::uint32_t group_key_input_index, DistributedAggregateSnapshotBindingLimits limits = {});

// Specializes an already-compatible aggregate snapshot without reopening its authority join. The
// supplied exact destination schema must match every bound dispatch; the projected key is proved
// FLOAT64 before the same Manifest pin transfers into the grouped owner.
[[nodiscard]] common::Result<CompatibleDistributedGroupedFloat64Snapshot>
bind_compatible_distributed_grouped_float64_snapshot(
    CompatibleDistributedAggregateSnapshot aggregate_snapshot,
    const schema::TableSchema& destination_schema, std::uint32_t group_key_input_index);

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

using DistributedVectorReplicaProof = DistributedAggregateReplicaProof;

struct MetadataBackedDistributedVectorSnapshotBinding {
  std::reference_wrapper<const raft::MetadataCatalogSnapshot> catalog;
  schema::TableId table_id;
  std::span<const DistributedVectorReplicaProof> replica_proofs;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::optional<cseg::EventTimePredicate> event_time_predicate;
};

// Resolves every vector fragment's schema, placement, group, and policy-specific read admission
// from one committed catalog before entering the compatible vector snapshot binder.
[[nodiscard]] common::Result<CompatibleDistributedVectorSnapshot>
bind_metadata_backed_distributed_vector_snapshot(
    const DistributedVectorQueryPlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const MetadataBackedDistributedVectorSnapshotBinding& binding,
    DistributedVectorSnapshotBindingLimits limits = {});

struct DistributedAggregateGroupReadAuthority {
  raft::GroupReadBarrier barrier;
  raft::RaftGroupObservation observation;
};

struct GroupBackedDistributedAggregateSnapshotBinding {
  std::reference_wrapper<const raft::MetadataCatalogSnapshot> catalog;
  schema::TableId table_id;
  // Canonical unique order by observation.group_id. Extra groups (for example metadata) are
  // ignored; each selected tablet's immutable group must be present.
  std::span<const DistributedAggregateGroupReadAuthority> group_authorities;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::uint32_t aggregate_input_index{};
  std::optional<cseg::EventTimePredicate> event_time_predicate;
};

// Resolves every planned tablet through committed tablet-to-group metadata before selecting its
// exact correlated leader barrier/observation. This removes the caller's plan-order join while
// retaining the same metadata/Manifest binder.
[[nodiscard]] common::Result<CompatibleDistributedAggregateSnapshot>
bind_group_backed_distributed_aggregate_snapshot(
    const DistributedAggregatePlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const GroupBackedDistributedAggregateSnapshotBinding& binding,
    DistributedAggregateSnapshotBindingLimits limits = {});

struct DistributedAggregateFollowerReadAuthority {
  raft::RaftGroupObservation leader_observation;
  raft::RaftGroupObservation follower_observation;
};

// Validates the complete same-group, same-term, stable-membership leader/follower correlation
// required before a remote acquisition may be used as bounded-stale read authority.
[[nodiscard]] bool is_valid_distributed_aggregate_follower_read_authority(
    const DistributedAggregateFollowerReadAuthority& authority);

struct FollowerGroupBackedDistributedAggregateSnapshotBinding {
  std::reference_wrapper<const raft::MetadataCatalogSnapshot> catalog;
  schema::TableId table_id;
  // Canonical unique order by follower_observation.group_id. Each selected tablet group must have
  // one same-term current-leader/follower pair; unrelated groups are ignored.
  std::span<const DistributedAggregateFollowerReadAuthority> group_authorities;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::uint32_t aggregate_input_index{};
  std::optional<cseg::EventTimePredicate> event_time_predicate;
};

// Derives the leader-commit position only from a correlated same-group, same-term leader/follower
// observation pair, then delegates placement, lag, and Manifest coverage to the metadata binder.
[[nodiscard]] common::Result<CompatibleDistributedAggregateSnapshot>
bind_follower_group_backed_distributed_aggregate_snapshot(
    const DistributedAggregatePlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const FollowerGroupBackedDistributedAggregateSnapshotBinding& binding,
    DistributedAggregateSnapshotBindingLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_BINDING_HPP_
