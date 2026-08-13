#include "chronos/cluster/distributed_vector_query_transport_v2.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorQueryRequestV2>);
static_assert(
    !std::is_move_constructible_v<chronos::cluster::DistributedVectorQueryRequestV2Reader>);
static_assert(
    !std::is_move_constructible_v<chronos::cluster::DistributedVectorQueryResponseV2Reader>);
static_assert(
    std::is_nothrow_constructible_v<chronos::cluster::DistributedVectorQueryResponseV2Reader,
                                    chronos::query::DistributedVectorResultSchema&&>);
static_assert(!std::is_constructible_v<chronos::cluster::DistributedVectorQueryResponseV2Reader,
                                       const chronos::query::DistributedVectorResultSchema&>);
static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedVectorQueryFrameV2WriteCursor>);
static_assert(std::is_abstract_v<chronos::cluster::DistributedVectorQueryWorkerServiceV2>);

namespace {
[[maybe_unused]] const auto kEncodeRequest =
    &chronos::cluster::encode_distributed_vector_query_request_v2;
[[maybe_unused]] const auto kDecodeRequest =
    &chronos::cluster::decode_distributed_vector_query_request_v2_exact;
[[maybe_unused]] const auto kEncodeResponse =
    &chronos::cluster::encode_distributed_vector_query_response_v2;
[[maybe_unused]] const auto kDecodeResponse =
    &chronos::cluster::decode_distributed_vector_query_response_v2_exact;
[[maybe_unused]] const auto kCreateRequestCursor =
    &chronos::cluster::DistributedVectorQueryFrameV2WriteCursor::create_request;
[[maybe_unused]] const auto kCreateResponseCursor =
    &chronos::cluster::DistributedVectorQueryFrameV2WriteCursor::create_response;
[[maybe_unused]] const auto kCreateReceiver =
    &chronos::cluster::DistributedVectorQueryReceiverV2::create;
} // namespace
