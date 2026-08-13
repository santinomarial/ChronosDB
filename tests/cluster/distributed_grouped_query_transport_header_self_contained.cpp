#include "chronos/cluster/distributed_grouped_query_transport.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::cluster::DistributedGroupedQueryRequest>);
static_assert(std::is_aggregate_v<chronos::cluster::DistributedGroupedQueryResponse>);
static_assert(
    !std::is_move_constructible_v<chronos::cluster::DistributedGroupedQueryRequestReader>);
static_assert(
    !std::is_move_constructible_v<chronos::cluster::DistributedGroupedQueryResponseReader>);
static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedGroupedQueryFrameWriteCursor>);

namespace {
[[maybe_unused]] const auto kEncodeRequest =
    &chronos::cluster::encode_distributed_grouped_query_request_v1;
[[maybe_unused]] const auto kDecodeResponse =
    &chronos::cluster::decode_distributed_grouped_query_response_v1;
} // namespace
