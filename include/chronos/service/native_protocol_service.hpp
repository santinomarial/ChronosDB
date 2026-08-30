#ifndef CHRONOS_SERVICE_NATIVE_PROTOCOL_SERVICE_HPP_
#define CHRONOS_SERVICE_NATIVE_PROTOCOL_SERVICE_HPP_

#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_shuffle_execution.hpp"
#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_shuffle_job_execution.hpp"
#include "chronos/cluster/distributed_mutable_vector_rows_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_vector_aggregate_rows_finalization_v2.hpp"
#include "chronos/cluster/distributed_vector_physical_rows_finalization_v2.hpp"
#include "chronos/cluster/raft_read_authority_tcp_batch_acquisition.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/common/uuid_generator.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/protocol.hpp"
#include "chronos/network/spsc_queue.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/snapshot_pipeline.hpp"
#include "chronos/query/statement_binder.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"
#include "chronos/service/replicated_ingest_database.hpp"
#include "chronos/service/single_node_database.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace chronos::service {

class ReplicatedReadBarrier;

struct NativeProtocolServiceLimits {
  network::ProtocolLimits protocol{};
  ingest::ColumnarAppendDecodeLimits columnar_append{};
  query::SqlParserLimits sql_parser{};
  query::SqlBinderLimits sql_binder{};
  query::SqlInsertBinderLimits sql_insert{};
  query::PhysicalSelectLoweringLimits physical_lowering{};
  query::SnapshotTabletPipelineLimits tablet_pipeline{};
  query::TabletStatePipelineLimits replicated_tablet_pipeline{};
  columnar::ColumnarBatchLimits insert_batch{};
  network::QueryResultLimits query_result{};
  std::size_t maximum_query_memory_bytes{std::size_t{64U} * 1024U * 1024U};
  std::uint64_t maximum_result_rows{1'048'576U};
  std::size_t maximum_result_batches{1024U};
  std::size_t maximum_response_payload_bytes{std::size_t{64U} * 1024U * 1024U};
  std::uint64_t ddl_retry_retention_positions{1'000'000U};
};

struct NativeProtocolResponseSequence {
  std::vector<network::NetworkTask> responses;
  std::uint64_t result_rows{};
  std::size_t payload_bytes{};
};

// Cross-thread cooperative cancellation publication for one Native query. The queue owner is the
// sole publisher and the query thread is the sole observer. Cancellation is sticky and carries no
// response ownership by itself.
class NativeQueryCancellation {
public:
  void request_cancel() noexcept {
    requested_.store(true, std::memory_order_release);
  }
  [[nodiscard]] bool requested() const noexcept {
    return requested_.load(std::memory_order_acquire);
  }

private:
  std::atomic<bool> requested_;
};

class NativeQueryDispatcher {
public:
  virtual ~NativeQueryDispatcher() = default;
  [[nodiscard]] virtual common::Result<NativeProtocolResponseSequence>
  execute_query(network::NetworkTask request, const NativeQueryCancellation* cancellation) = 0;
};

using NativeIdentityGenerator = common::UuidGenerator;

struct NativeDistributedGroupedShufflePlan {
  bool selected{true};
  cluster::DistributedVectorGroupedAggregateShuffleQueryExecutionConfig execution;
  std::optional<cluster::DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig>
      reducer_jobs;
  cluster::DistributedVectorGroupedAggregateShuffleAuthorityLimits authority;
};

// Deployment seam for per-query destination listeners, routes, TLS contexts, and reducer resource
// owners. Returned borrowed dependencies must outlive the synchronous execute_query() call.
class NativeDistributedGroupedShuffleProvider {
public:
  virtual ~NativeDistributedGroupedShuffleProvider() = default;
  [[nodiscard]] virtual common::Result<NativeDistributedGroupedShufflePlan>
  prepare(std::span<const query::DistributedMutableVectorFragment> fragments,
          std::span<const query::VectorGroupKeyDefinition> keys,
          std::span<const query::VectorAggregateDefinition> aggregates,
          std::span<const cluster::DistributedQueryNodeRoute> routes,
          std::chrono::steady_clock::time_point execution_deadline) = 0;
};

// Borrowed split-leader mutable-query client policy for direct rows, transitional global
// aggregates, and direct-input grouped sufficient state. source_node_id names the coordinator
// transport identity. Fragments led by that node execute through the matching local worker; all
// others use authenticated TLS routes because the carrier rejects self-routes. The config, workers,
// security owners, TLS contexts, and every referenced TLS client context must outlive the
// NativeProtocolService.
struct NativeDistributedMutableVectorRowsQueryConfig {
  raft::NodeId source_node_id{};
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  // Optional synchronous worker for fragments whose serving node equals source_node_id. Required
  // when a prepared query contains such a fragment; it must outlive the service.
  cluster::DistributedMutableVectorQueryWorkerService* local_worker{};
  // Optional synchronous sufficient-state GROUP BY worker for fragments served by
  // source_node_id. Required when an eligible grouped query contains such a fragment; it must
  // outlive the service.
  cluster::DistributedMutableVectorGroupedAggregateQueryWorkerService* local_grouped_worker{};
  // Optional explicit selection of the partitioned grouped path. The provider and every borrowed
  // dependency it returns must outlive the service/synchronous query call.
  NativeDistributedGroupedShuffleProvider* grouped_shuffle_provider{};
  std::span<const cluster::DistributedQueryNodeTlsContext> tls_contexts;
  cluster::DistributedQueryRouteResolutionLimits route_resolution;
  query::DistributedVectorRowsSqlLoweringLimits sql_lowering;
  query::DistributedVectorAggregateSqlLoweringLimits aggregate_sql_lowering;
  query::DistributedVectorGroupedSqlLoweringLimits grouped_sql_lowering;
  query::DistributedVectorGroupedAggregateSqlLoweringLimits grouped_aggregate_sql_lowering;
  cluster::DistributedMutableVectorQueryExecutionLimits execution;
  cluster::DistributedMutableVectorGroupedAggregateQueryExecutionLimits grouped_aggregate_execution;
  cluster::DistributedMutableVectorQueryTlsLimits carrier;
  cluster::DistributedMutableVectorGroupedAggregateQueryTlsLimits grouped_aggregate_carrier;
  cluster::RaftReadAuthorityTlsClientLimits authority_carrier;
  cluster::RaftReadAuthorityTcpRetryLimits authority_retry;
  cluster::DistributedVectorRowFinalizationLimitsV2 finalization;
  cluster::DistributedVectorAggregateRowsFinalizationLimitsV2 aggregate_finalization;
  cluster::DistributedVectorPhysicalRowsFinalizationLimitsV2 grouped_finalization;
  cluster::DistributedVectorGroupedAggregateFinalizationLimitsV2 grouped_aggregate_finalization;
  std::chrono::milliseconds connect_timeout{5000};
  std::chrono::milliseconds authority_connect_timeout{5000};
  std::chrono::milliseconds execution_timeout{30000};
  std::chrono::milliseconds maximum_poll_wait{10};
  std::size_t maximum_authority_rebindings{3U};
};

// Thread-affine synchronous translation between an already accepted native request and one
// database owner. The one-argument replicated constructor serves current local-applied SELECT;
// the read-barrier constructor first confirms every resident group and then requires publication
// coverage. Replicated ingest remains asynchronous and CREATE/INSERT/ASOF fail explicitly.
// Returned tasks retain the connection/principal routing envelope.
// Ingest returns one terminal response; query returns a bounded result sequence ending in
// QUERY_END, one terminal ERROR, or an authoritative negotiated redirect before output. Queueing
// and socket backpressure remain owned by the reactor worker.
class NativeProtocolService final : public NativeQueryDispatcher {
public:
  explicit NativeProtocolService(SingleNodeDatabase& database,
                                 NativeProtocolServiceLimits limits = {}) noexcept;
  NativeProtocolService(SingleNodeDatabase& database, NativeIdentityGenerator& identities,
                        NativeProtocolServiceLimits limits = {}) noexcept;
  explicit NativeProtocolService(ReplicatedIngestDatabase& database,
                                 NativeProtocolServiceLimits limits = {}) noexcept;
  NativeProtocolService(ReplicatedIngestDatabase& database, ReplicatedReadBarrier& read_barrier,
                        NativeProtocolServiceLimits limits = {}) noexcept;
  NativeProtocolService(ReplicatedIngestDatabase& database, ReplicatedReadBarrier& read_barrier,
                        const NativeDistributedMutableVectorRowsQueryConfig& distributed_rows,
                        NativeProtocolServiceLimits limits = {}) noexcept;
  NativeProtocolService(ReplicatedIngestDatabase& database, ReplicatedReadBarrier& read_barrier,
                        NativeIdentityGenerator& identities,
                        const NativeDistributedMutableVectorRowsQueryConfig& distributed_rows,
                        NativeProtocolServiceLimits limits = {}) noexcept;

  [[nodiscard]] common::Result<network::NetworkTask> execute_ingest(network::NetworkTask request);
  [[nodiscard]] common::Result<NativeProtocolResponseSequence>
  execute_query(network::NetworkTask request);
  [[nodiscard]] common::Result<NativeProtocolResponseSequence>
  execute_query(network::NetworkTask request, const NativeQueryCancellation* cancellation) override;

private:
  SingleNodeDatabase* database_{};
  ReplicatedIngestDatabase* replicated_database_{};
  ReplicatedReadBarrier* replicated_read_barrier_{};
  NativeIdentityGenerator* identities_{};
  const NativeDistributedMutableVectorRowsQueryConfig* distributed_mutable_query_{};
  NativeProtocolServiceLimits limits_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_NATIVE_PROTOCOL_SERVICE_HPP_
