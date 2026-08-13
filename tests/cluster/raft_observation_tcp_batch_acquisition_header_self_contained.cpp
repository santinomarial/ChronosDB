#include "chronos/cluster/raft_observation_tcp_batch_acquisition.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::RaftObservationTcpBatchAcquisition>);
static_assert(std::is_move_constructible_v<chronos::cluster::RaftObservationTcpBatchAcquisition>);

namespace {
using AggregateBatchConstructor =
    chronos::common::Result<chronos::cluster::RaftObservationTcpBatchAcquisitionConfig> (*)(
        const chronos::query::DistributedAggregatePlan&,
        const chronos::raft::MetadataCatalogSnapshot&,
        const chronos::cluster::RaftObservationTcpBatchConstructionConfig&);
using VectorBatchConstructor =
    chronos::common::Result<chronos::cluster::RaftObservationTcpBatchAcquisitionConfig> (*)(
        const chronos::query::DistributedVectorQueryPlan&,
        const chronos::raft::MetadataCatalogSnapshot&,
        const chronos::cluster::RaftObservationTcpBatchConstructionConfig&);
[[maybe_unused]] const AggregateBatchConstructor kConstructAggregateBatch =
    &chronos::cluster::construct_raft_observation_tcp_batch;
[[maybe_unused]] const VectorBatchConstructor kConstructVectorBatch =
    &chronos::cluster::construct_raft_observation_tcp_batch;
} // namespace
