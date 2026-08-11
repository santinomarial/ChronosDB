#include "chronos/cluster/distributed_query_transport.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::cluster::DistributedQueryRequest>);
static_assert(std::is_aggregate_v<chronos::cluster::DistributedQueryResponse>);

namespace {
[[maybe_unused]] const auto kEncodeRequest = &chronos::cluster::encode_distributed_query_request_v1;
[[maybe_unused]] const auto kDecodeResponse =
    &chronos::cluster::decode_distributed_query_response_v1;
} // namespace
