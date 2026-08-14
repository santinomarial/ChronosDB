#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_HPP_

#include "chronos/cluster/distributed_grouped_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_query_execution.hpp"
#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_aggregate_query_tcp_execution_v2.hpp"
#include "chronos/cluster/raft_observation_tcp_batch_acquisition.hpp"
#include "chronos/common/result.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/distributed_fragment_binding.hpp"
#include "chronos/service/replicated_read_barrier.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::service {

struct ReplicatedDistributedAggregateQueryConfig {
  raft::NodeId source_node_id{};
  ReplicatedReadBarrier* read_barrier{};
  raft::GroupId metadata_group_id;
  std::reference_wrapper<const raft::MetadataCatalogSnapshot> catalog;
  schema::TableId table_id;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::uint32_t aggregate_input_index{};
  std::optional<cseg::EventTimePredicate> event_time_predicate;
  std::span<const cluster::DistributedQueryNodeTlsContext> tls_contexts;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  query::DistributedAggregateSnapshotBindingLimits binding_limits;
  cluster::DistributedQueryRouteResolutionLimits route_limits;
  cluster::DistributedQueryExecutionLimits execution_limits;
  cluster::DistributedQueryTlsClientLimits carrier_limits;
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline;
  std::size_t maximum_rebindings{3U};
};

// Synchronously acquires exact leader-linearizable authority, requires the catalog to cover the
// metadata-group barrier, binds the Manifest snapshot, resolves authenticated routes, and returns
// the single-threaded TCP lifecycle owner. Borrowed authentication/TLS policy must outlive it.
[[nodiscard]] common::Result<cluster::DistributedQueryTcpExecution>
create_replicated_distributed_aggregate_query(
    query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const ReplicatedDistributedAggregateQueryConfig& config);

// Uses the same metadata-barrier, route, execution, and lifecycle composition for a bounded-stale
// plan. The caller owns transport acquisition of the already-correlated follower authorities.
[[nodiscard]] common::Result<cluster::DistributedQueryTcpExecution>
create_replicated_follower_distributed_aggregate_query(
    query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    std::span<const query::DistributedAggregateFollowerReadAuthority> follower_authorities,
    const ReplicatedDistributedAggregateQueryConfig& config);

struct ReplicatedDistributedVectorAggregateQueryConfigV2 {
  raft::NodeId source_node_id{};
  ReplicatedReadBarrier* read_barrier{};
  raft::GroupId metadata_group_id;
  std::reference_wrapper<const raft::MetadataCatalogSnapshot> catalog;
  schema::TableId table_id;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::optional<cseg::EventTimePredicate> event_time_predicate;
  std::span<const cluster::DistributedQueryNodeTlsContext> tls_contexts;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  query::DistributedVectorSnapshotBindingLimits binding_limits;
  cluster::DistributedQueryRouteResolutionLimits route_limits;
  cluster::DistributedVectorAggregateQueryExecutionLimitsV2 execution_limits;
  cluster::DistributedVectorAggregateQueryTlsLimitsV2 carrier_limits;
  cluster::DistributedVectorAggregateFinalizationLimitsV2 finalization_limits;
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline;
};

// Acquires exact leader-linearizable authority and transfers one caller-owned result schema through
// compatible v2 binding, committed route resolution, aggregate execution, and TCP finalization.
// Borrowed authentication/TLS policy must outlive the returned single-threaded owner.
[[nodiscard]] common::Result<cluster::DistributedVectorAggregateQueryTcpExecutionV2>
create_replicated_distributed_vector_aggregate_query_v2(
    const query::DistributedVectorQueryPlan& plan,
    manifest::TemporalDatabaseStorageSnapshot snapshot,
    query::DistributedVectorResultSchema&& result_schema,
    const ReplicatedDistributedVectorAggregateQueryConfigV2& config);

// Applies the same schema/route/execution lifecycle to one canonical already-correlated follower
// authority vector. Remote observation acquisition remains caller-owned.
[[nodiscard]] common::Result<cluster::DistributedVectorAggregateQueryTcpExecutionV2>
create_replicated_follower_distributed_vector_aggregate_query_v2(
    const query::DistributedVectorQueryPlan& plan,
    manifest::TemporalDatabaseStorageSnapshot snapshot,
    query::DistributedVectorResultSchema&& result_schema,
    std::span<const query::DistributedAggregateFollowerReadAuthority> follower_authorities,
    const ReplicatedDistributedVectorAggregateQueryConfigV2& config);

struct ReplicatedDistributedGroupedFloat64QueryConfig {
  raft::NodeId source_node_id{};
  ReplicatedReadBarrier* read_barrier{};
  raft::GroupId metadata_group_id;
  std::reference_wrapper<const raft::MetadataCatalogSnapshot> catalog;
  schema::TableId table_id;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::uint32_t aggregate_input_index{};
  std::uint32_t group_key_input_index{};
  std::optional<cseg::EventTimePredicate> event_time_predicate;
  std::span<const cluster::DistributedQueryNodeTlsContext> tls_contexts;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  query::DistributedAggregateSnapshotBindingLimits binding_limits;
  cluster::DistributedQueryRouteResolutionLimits route_limits;
  cluster::DistributedGroupedQueryExecutionLimits execution_limits;
  cluster::DistributedGroupedQueryTlsLimits carrier_limits;
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline;
  std::size_t maximum_rebindings{3U};
};

// Acquires exact leader-linearizable authority and creates the grouped TCP lifecycle without
// exposing aggregate/group/schema/route correlation to the embedding.
[[nodiscard]] common::Result<cluster::DistributedGroupedQueryTcpExecution>
create_replicated_distributed_grouped_float64_query(
    query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const ReplicatedDistributedGroupedFloat64QueryConfig& config);

// Applies the same grouped specialization, route, execution, and lifecycle gates to a canonical
// bounded-stale leader/follower authority vector. Observation acquisition remains caller-owned.
[[nodiscard]] common::Result<cluster::DistributedGroupedQueryTcpExecution>
create_replicated_follower_distributed_grouped_float64_query(
    query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    std::span<const query::DistributedAggregateFollowerReadAuthority> follower_authorities,
    const ReplicatedDistributedGroupedFloat64QueryConfig& config);

enum class ReplicatedFollowerDistributedAggregateQueryState : std::uint8_t {
  kAcquiringAuthority = 1,
  kExecuting = 2,
  kComplete = 3,
  kFailed = 4,
  kCancelled = 5,
};

struct ReplicatedFollowerDistributedAggregateQueryMetrics {
  cluster::RaftObservationTcpBatchAcquisitionMetrics authority;
  std::optional<cluster::DistributedQueryTcpExecutionMetrics> execution;
};

// Owns the complete bounded-stale remote lifecycle: placement-backed observation acquisition,
// metadata/Manifest binding, and distributed-query TCP execution. Borrowed catalog, barrier,
// authentication, projection, and TLS objects in both configs must outlive this owner.
class ReplicatedFollowerDistributedAggregateQuery {
public:
  ReplicatedFollowerDistributedAggregateQuery() noexcept;
  ~ReplicatedFollowerDistributedAggregateQuery();
  ReplicatedFollowerDistributedAggregateQuery(const ReplicatedFollowerDistributedAggregateQuery&) =
      delete;
  ReplicatedFollowerDistributedAggregateQuery&
  operator=(const ReplicatedFollowerDistributedAggregateQuery&) = delete;
  ReplicatedFollowerDistributedAggregateQuery(
      ReplicatedFollowerDistributedAggregateQuery&&) noexcept;
  ReplicatedFollowerDistributedAggregateQuery&
  operator=(ReplicatedFollowerDistributedAggregateQuery&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedFollowerDistributedAggregateQuery>
  create(query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
         cluster::RaftObservationTcpBatchConstructionConfig authority_config,
         ReplicatedDistributedAggregateQueryConfig query_config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] ReplicatedFollowerDistributedAggregateQueryState state() const noexcept;
  [[nodiscard]] ReplicatedFollowerDistributedAggregateQueryMetrics metrics() const noexcept;
  [[nodiscard]] common::Result<query::MergeableAggregateState> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit ReplicatedFollowerDistributedAggregateQuery(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

enum class ReplicatedFollowerDistributedVectorAggregateQueryStateV2 : std::uint8_t {
  kAcquiringAuthority = 1,
  kExecuting = 2,
  kComplete = 3,
  kFailed = 4,
  kCancelled = 5,
};

struct ReplicatedFollowerDistributedVectorAggregateQueryMetricsV2 {
  cluster::RaftObservationTcpBatchAcquisitionMetrics authority;
  std::optional<cluster::DistributedVectorAggregateQueryTcpExecutionMetricsV2> execution;
};

// Owns the complete remote bounded-stale aggregate-v2 lifecycle. The plan, result schema, and
// Manifest pin survive placement-backed authority acquisition and transfer together into packaged
// follower execution. Catalog/barrier/projection views and all authentication/TLS policies in both
// configs are borrowed and must outlive this single-threaded owner. A successful result reference
// remains valid until the owner is moved, destroyed, or otherwise mutated.
class ReplicatedFollowerDistributedVectorAggregateQueryV2 {
public:
  ReplicatedFollowerDistributedVectorAggregateQueryV2() noexcept;
  ~ReplicatedFollowerDistributedVectorAggregateQueryV2();
  ReplicatedFollowerDistributedVectorAggregateQueryV2(
      const ReplicatedFollowerDistributedVectorAggregateQueryV2&) = delete;
  ReplicatedFollowerDistributedVectorAggregateQueryV2&
  operator=(const ReplicatedFollowerDistributedVectorAggregateQueryV2&) = delete;
  ReplicatedFollowerDistributedVectorAggregateQueryV2(
      ReplicatedFollowerDistributedVectorAggregateQueryV2&&) noexcept;
  ReplicatedFollowerDistributedVectorAggregateQueryV2&
  operator=(ReplicatedFollowerDistributedVectorAggregateQueryV2&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedFollowerDistributedVectorAggregateQueryV2>
  create(query::DistributedVectorQueryPlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
         query::DistributedVectorResultSchema&& result_schema,
         cluster::RaftObservationTcpBatchConstructionConfig authority_config,
         ReplicatedDistributedVectorAggregateQueryConfigV2 query_config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] ReplicatedFollowerDistributedVectorAggregateQueryStateV2 state() const noexcept;
  [[nodiscard]] ReplicatedFollowerDistributedVectorAggregateQueryMetricsV2 metrics() const noexcept;
  [[nodiscard]] common::Result<
      std::reference_wrapper<const cluster::DistributedVectorAggregateFinalizedResultV2>>
  result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit ReplicatedFollowerDistributedVectorAggregateQueryV2(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

enum class ReplicatedFollowerDistributedGroupedFloat64QueryState : std::uint8_t {
  kAcquiringAuthority = 1,
  kExecuting = 2,
  kComplete = 3,
  kFailed = 4,
  kCancelled = 5,
};

struct ReplicatedFollowerDistributedGroupedFloat64QueryMetrics {
  cluster::RaftObservationTcpBatchAcquisitionMetrics authority;
  std::optional<cluster::DistributedGroupedQueryTcpExecutionMetrics> execution;
};

// Owns remote placement-backed follower authority acquisition and transfers its complete canonical
// vector directly into packaged grouped execution. All policy objects in both configs are borrowed
// and must outlive this single-threaded owner.
class ReplicatedFollowerDistributedGroupedFloat64Query {
public:
  ReplicatedFollowerDistributedGroupedFloat64Query() noexcept;
  ~ReplicatedFollowerDistributedGroupedFloat64Query();
  ReplicatedFollowerDistributedGroupedFloat64Query(
      const ReplicatedFollowerDistributedGroupedFloat64Query&) = delete;
  ReplicatedFollowerDistributedGroupedFloat64Query&
  operator=(const ReplicatedFollowerDistributedGroupedFloat64Query&) = delete;
  ReplicatedFollowerDistributedGroupedFloat64Query(
      ReplicatedFollowerDistributedGroupedFloat64Query&&) noexcept;
  ReplicatedFollowerDistributedGroupedFloat64Query&
  operator=(ReplicatedFollowerDistributedGroupedFloat64Query&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedFollowerDistributedGroupedFloat64Query>
  create(query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
         cluster::RaftObservationTcpBatchConstructionConfig authority_config,
         ReplicatedDistributedGroupedFloat64QueryConfig query_config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] ReplicatedFollowerDistributedGroupedFloat64QueryState state() const noexcept;
  [[nodiscard]] ReplicatedFollowerDistributedGroupedFloat64QueryMetrics metrics() const noexcept;
  [[nodiscard]] common::Result<std::vector<query::GroupedFloat64AggregateResult>> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit ReplicatedFollowerDistributedGroupedFloat64Query(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_HPP_
