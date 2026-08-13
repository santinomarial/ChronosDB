#include "chronos/cluster/distributed_vector_query_tls_v2.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::cluster::DistributedVectorQueryTlsClientV2>);
static_assert(std::is_move_constructible_v<chronos::cluster::DistributedVectorQueryTlsServerV2>);
static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorQueryAttemptV2>);

namespace {
[[maybe_unused]] const auto kCreateClient =
    &chronos::cluster::DistributedVectorQueryTlsClientV2::create;
[[maybe_unused]] const auto kCreateServer =
    &chronos::cluster::DistributedVectorQueryTlsServerV2::create;
} // namespace
