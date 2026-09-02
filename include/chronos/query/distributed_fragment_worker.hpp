#ifndef CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_WORKER_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_WORKER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_part_validation.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/distributed_fragment_dispatch.hpp"
#include "chronos/query/distributed_grouped_exchange.hpp"
#include "chronos/query/distributed_mutable_vector_fragment.hpp"
#include "chronos/query/distributed_vector_aggregate_exchange.hpp"
#include "chronos/query/distributed_vector_fragment_v2.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"
#include "chronos/query/scalar_snapshot_scan.hpp"
#include "chronos/query/temporal_cseg_snapshot.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <variant>
#include <vector>

namespace chronos::query {

struct DistributedAggregateWorkerLimits {
  manifest::TemporalPartValidationLimits part_validation;
  TemporalManifestCsegResolutionLimits resolution;
};

struct DistributedAggregateWorkerRequest {
  std::reference_wrapper<const DistributedAggregateFragmentDispatch> dispatch;
  std::reference_wrapper<const manifest::ManifestStorage> storage;
  std::reference_wrapper<const manifest::TemporalDatabaseStorageSnapshot> snapshot;
  std::reference_wrapper<const schema::SchemaLineage> lineage;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  common::Uuid raft_group_id;
  std::uint64_t local_node{};
  std::optional<raft::ReadBarrier> local_linearizable_barrier;
  DistributedAggregateWorkerLimits limits;
};

// Synchronous storage seam for proof-gated worker execution. A successful load calls consume()
// exactly once while all views are valid; failures call it zero times. Implementations must return
// only fully validated images for the exact supplied snapshot and identities.
class DistributedTemporalPartBatchConsumer {
public:
  DistributedTemporalPartBatchConsumer() = default;
  DistributedTemporalPartBatchConsumer(const DistributedTemporalPartBatchConsumer&) = delete;
  DistributedTemporalPartBatchConsumer&
  operator=(const DistributedTemporalPartBatchConsumer&) = delete;
  virtual ~DistributedTemporalPartBatchConsumer() = default;

  [[nodiscard]] virtual common::Status
  consume(std::span<const TemporalManifestCsegPartView> parts) = 0;
};

class DistributedTemporalPartBatchLoader {
public:
  DistributedTemporalPartBatchLoader() = default;
  DistributedTemporalPartBatchLoader(const DistributedTemporalPartBatchLoader&) = delete;
  DistributedTemporalPartBatchLoader& operator=(const DistributedTemporalPartBatchLoader&) = delete;
  virtual ~DistributedTemporalPartBatchLoader() = default;

  [[nodiscard]] virtual common::Status
  load(const manifest::TemporalDatabaseStorageSnapshot& snapshot,
       std::span<const cseg::PartId> part_ids,
       std::span<const manifest::TabletSchemaBinding> schema_bindings,
       manifest::TemporalPartValidationLimits validation_limits,
       DistributedTemporalPartBatchConsumer& consumer) const = 0;
};

// Reproves local group/node/placement and exact Manifest v2 authority before any part I/O, loads
// generation-pinned validated temporal CSEGs, resolves current visible winners, applies the pushed
// event-time predicate, and emits one terminal mergeable Float64 aggregate message.
[[nodiscard]] common::Result<ExchangeMessage>
execute_distributed_aggregate_fragment(const DistributedAggregateWorkerRequest& request);

// Uses a caller-supplied validated storage path after all dispatch/placement/snapshot proof gates.
// The request snapshot remains the sole logical authority; the loader only locates its exact bytes.
[[nodiscard]] common::Result<ExchangeMessage>
execute_distributed_aggregate_fragment(const DistributedAggregateWorkerRequest& request,
                                       const DistributedTemporalPartBatchLoader& loader);

struct DistributedGroupedFloat64WorkerRequest {
  std::reference_wrapper<const DistributedGroupedFloat64FragmentDispatch> dispatch;
  std::reference_wrapper<const manifest::ManifestStorage> storage;
  std::reference_wrapper<const manifest::TemporalDatabaseStorageSnapshot> snapshot;
  std::reference_wrapper<const schema::SchemaLineage> lineage;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  common::Uuid raft_group_id;
  std::uint64_t local_node{};
  std::optional<raft::ReadBarrier> local_linearizable_barrier;
  DistributedAggregateWorkerLimits limits;
};

using DistributedGroupedFloat64WorkerResult =
    std::variant<std::vector<GroupedFloat64ExchangeMessage>, GroupedExchangeTerminalMessage>;

// Reuses the aggregate worker's complete local authority gates, then groups visible temporal
// winners by the projected FLOAT64 key. Empty input returns the distinct terminal-only value.
[[nodiscard]] common::Result<DistributedGroupedFloat64WorkerResult>
execute_distributed_grouped_float64_fragment(const DistributedGroupedFloat64WorkerRequest& request);

[[nodiscard]] common::Result<DistributedGroupedFloat64WorkerResult>
execute_distributed_grouped_float64_fragment(const DistributedGroupedFloat64WorkerRequest& request,
                                             const DistributedTemporalPartBatchLoader& loader);

inline constexpr std::size_t kDefaultDistributedVectorRowsWorkerMemoryBytesV2 =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorRowsWorkerMemoryBytesV2 =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorRowsWorkerLimitsV2 {
  DistributedAggregateWorkerLimits storage{};
  std::size_t maximum_query_memory_bytes{kDefaultDistributedVectorRowsWorkerMemoryBytesV2};
  ScalarSnapshotScanLimits scan{};
  VectorChunkLimits output{};
};

struct DistributedVectorRowsWorkerRequestV2 {
  std::reference_wrapper<const DistributedVectorFragmentDispatchV2> dispatch;
  std::reference_wrapper<const manifest::ManifestStorage> storage;
  std::reference_wrapper<const manifest::TemporalDatabaseStorageSnapshot> snapshot;
  std::reference_wrapper<const schema::SchemaLineage> lineage;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  common::Uuid raft_group_id;
  std::uint64_t local_node{};
  std::optional<raft::ReadBarrier> local_linearizable_barrier;
  DistributedVectorRowsWorkerLimitsV2 limits;
};

struct DistributedVectorRowsWorkerResultV2 {
  std::uint64_t output_rows{};
  std::size_t output_chunks{};
};

struct DistributedMutableVectorRowsWorkerRequest {
  std::reference_wrapper<const DistributedMutableVectorFragment> fragment;
  std::reference_wrapper<const ingest::TabletSnapshot> snapshot;
  std::reference_wrapper<const schema::SchemaLineage> lineage;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  common::Uuid raft_group_id;
  std::uint64_t local_node{};
  std::optional<raft::ReadBarrier> local_linearizable_barrier;
  DistributedVectorRowsWorkerLimitsV2 limits;
};

// Synchronous, borrowed-chunk publication seam. A successful call may invoke consume repeatedly;
// each chunk remains valid only for that invocation. The caller must discard prior consumed output
// if this method or the enclosing worker later fails.
class DistributedVectorRowsChunkConsumerV2 {
public:
  DistributedVectorRowsChunkConsumerV2() = default;
  DistributedVectorRowsChunkConsumerV2(const DistributedVectorRowsChunkConsumerV2&) = delete;
  DistributedVectorRowsChunkConsumerV2&
  operator=(const DistributedVectorRowsChunkConsumerV2&) = delete;
  virtual ~DistributedVectorRowsChunkConsumerV2() = default;

  [[nodiscard]] virtual common::Status consume(const VectorChunk& chunk) = 0;
};

// Executes row-mode projection over exactly one immutable committed/applied TabletState
// publication. No durable Manifest position is inferred or substituted. Output remains
// fragment-local and unordered; final ORDER BY/LIMIT are coordinator responsibilities.
[[nodiscard]] common::Result<DistributedVectorRowsWorkerResultV2>
execute_distributed_mutable_vector_rows_fragment(
    const DistributedMutableVectorRowsWorkerRequest& request,
    DistributedVectorRowsChunkConsumerV2& consumer);

// Reuses the complete worker authority and real-CSEG winner-resolution gates, then emits every
// fragment-local row in source order under the Fragment-v2 result schema. Final ORDER BY and LIMIT
// remain coordinator semantics and are deliberately not applied here. Aggregate modes use the
// distinct sufficient-state worker below and fail closed at this row-only API.
[[nodiscard]] common::Result<DistributedVectorRowsWorkerResultV2>
execute_distributed_vector_rows_fragment_v2(const DistributedVectorRowsWorkerRequestV2& request,
                                            DistributedVectorRowsChunkConsumerV2& consumer);

[[nodiscard]] common::Result<DistributedVectorRowsWorkerResultV2>
execute_distributed_vector_rows_fragment_v2(const DistributedVectorRowsWorkerRequestV2& request,
                                            const DistributedTemporalPartBatchLoader& loader,
                                            DistributedVectorRowsChunkConsumerV2& consumer);

struct DistributedVectorAggregateWorkerLimitsV2 {
  DistributedAggregateWorkerLimits storage;
  std::size_t maximum_query_memory_bytes{kDefaultDistributedVectorRowsWorkerMemoryBytesV2};
  ScalarSnapshotScanLimits scan;
  VectorChunkLimits projection;
  std::size_t maximum_aggregates{kMaximumUngroupedAggregateWidth};
  std::size_t maximum_variable_extremum_bytes{kDefaultAggregateExtremumByteLimit};
  std::size_t maximum_retained_configuration_bytes{
      kDefaultUngroupedAggregateConfigurationByteLimit};
};

struct DistributedVectorAggregateWorkerRequestV2 {
  std::reference_wrapper<const DistributedVectorFragmentDispatchV2> dispatch;
  std::reference_wrapper<const manifest::ManifestStorage> storage;
  std::reference_wrapper<const manifest::TemporalDatabaseStorageSnapshot> snapshot;
  std::reference_wrapper<const schema::SchemaLineage> lineage;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  common::Uuid raft_group_id;
  std::uint64_t local_node{};
  std::optional<raft::ReadBarrier> local_linearizable_barrier;
  DistributedVectorAggregateWorkerLimitsV2 limits;
};

struct DistributedVectorAggregateWorkerResultV2 {
  // The exact local fragment-derived definitions authorize every corresponding message and remain
  // available for encoding or comparison with the coordinator's pinned cross-tablet authority.
  std::vector<VectorAggregateDefinition> definitions{}; // NOLINT(readability-redundant-member-init)
  std::vector<DistributedVectorAggregateExchangeMessage>
      messages{}; // NOLINT(readability-redundant-member-init)
  std::uint64_t input_rows{};
};

// Reuses the complete row-worker authority and real-CSEG winner-resolution gates, but accepts only
// ungrouped aggregate plans. The exact projected shape is materialized before every supported
// operation accumulates into sufficient mergeable state. One canonical correlated message is
// returned per definition; tablet-local ORDER BY/LIMIT and finalization are never applied.
// The binding-only entry point runs the same structural, limit, placement, snapshot, schema, group,
// and read-barrier gates without loading parts, and returns only the exact definition authority.
[[nodiscard]] common::Result<std::vector<VectorAggregateDefinition>>
bind_distributed_vector_aggregate_worker_definitions_v2(
    const DistributedVectorAggregateWorkerRequestV2& request);

[[nodiscard]] common::Result<DistributedVectorAggregateWorkerResultV2>
execute_distributed_vector_aggregate_fragment_v2(
    const DistributedVectorAggregateWorkerRequestV2& request);

[[nodiscard]] common::Result<DistributedVectorAggregateWorkerResultV2>
execute_distributed_vector_aggregate_fragment_v2(
    const DistributedVectorAggregateWorkerRequestV2& request,
    const DistributedTemporalPartBatchLoader& loader);

inline constexpr std::size_t kDefaultDistributedVectorGroupedWorkerEncodedBytesV2 =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedWorkerEncodedBytesV2 =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorGroupedAggregateWorkerLimitsV2 {
  DistributedAggregateWorkerLimits storage;
  std::size_t maximum_query_memory_bytes{kDefaultDistributedVectorRowsWorkerMemoryBytesV2};
  std::size_t maximum_total_encoded_bytes{kDefaultDistributedVectorGroupedWorkerEncodedBytesV2};
  std::size_t maximum_retained_configuration_bytes{kDefaultGroupedAggregateConfigurationByteLimit};
  ScalarSnapshotScanLimits scan;
  VectorChunkLimits projection;
  GroupedAggregateLimits table;
};

struct DistributedVectorGroupedAggregateWorkerRequestV2 {
  std::reference_wrapper<const DistributedVectorFragmentDispatchV2> dispatch;
  std::reference_wrapper<const manifest::ManifestStorage> storage;
  std::reference_wrapper<const manifest::TemporalDatabaseStorageSnapshot> snapshot;
  std::reference_wrapper<const schema::SchemaLineage> lineage;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  common::Uuid raft_group_id;
  std::uint64_t local_node{};
  std::optional<raft::ReadBarrier> local_linearizable_barrier;
  DistributedVectorGroupedAggregateWorkerLimitsV2 limits;
};

struct DistributedMutableVectorGroupedAggregateWorkerRequest {
  std::reference_wrapper<const DistributedMutableVectorFragment> fragment;
  std::reference_wrapper<const ingest::TabletSnapshot> snapshot;
  std::reference_wrapper<const schema::SchemaLineage> lineage;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  common::Uuid raft_group_id;
  std::uint64_t local_node{};
  std::optional<raft::ReadBarrier> local_linearizable_barrier;
  DistributedVectorGroupedAggregateWorkerLimitsV2 limits;
};

struct DistributedVectorGroupedAggregateWorkerResultV2 {
  DistributedVectorGroupedAggregateAuthority authority;
  std::vector<EncodedDistributedVectorGroupedAggregateExchangeMessage>
      messages{}; // NOLINT(readability-redundant-member-init)
  std::uint64_t input_rows{};
  std::size_t group_count{};
  std::size_t encoded_bytes{};
};

// Accepts only the direct-input grouped Fragment-v2 subset. It reuses every row-worker proof and
// real-CSEG winner-resolution gate, groups the projected input with the shared query-accounted
// table, and returns one owned canonical frame per local group or one distinct empty terminal.
// ORDER BY, LIMIT, finalization, computed pre-group expressions, and transport remain enclosing
// coordinator responsibilities. The binding-only entry point performs no part I/O.
[[nodiscard]] common::Result<DistributedVectorGroupedAggregateAuthority>
bind_distributed_vector_grouped_aggregate_worker_authority_v2(
    const DistributedVectorGroupedAggregateWorkerRequestV2& request);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateWorkerResultV2>
execute_distributed_vector_grouped_aggregate_fragment_v2(
    const DistributedVectorGroupedAggregateWorkerRequestV2& request);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateWorkerResultV2>
execute_distributed_vector_grouped_aggregate_fragment_v2(
    const DistributedVectorGroupedAggregateWorkerRequestV2& request,
    const DistributedTemporalPartBatchLoader& loader);

// Mutable-TabletState counterpart of the Fragment-v2 sufficient-state worker. It exact-matches one
// immutable applied TabletSnapshot, derives the same key/aggregate authority from the distinct
// mutable fragment, and emits the same canonical grouped-state frames. It performs no Manifest I/O.
[[nodiscard]] common::Result<DistributedVectorGroupedAggregateAuthority>
bind_distributed_mutable_vector_grouped_aggregate_worker_authority(
    const DistributedMutableVectorGroupedAggregateWorkerRequest& request);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateWorkerResultV2>
execute_distributed_mutable_vector_grouped_aggregate_fragment(
    const DistributedMutableVectorGroupedAggregateWorkerRequest& request);

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_WORKER_HPP_
