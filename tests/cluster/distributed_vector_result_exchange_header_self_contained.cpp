#include "chronos/cluster/distributed_vector_result_exchange.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorResultExchangeMessage>);
static_assert(
    !std::is_copy_constructible_v<chronos::cluster::EncodedDistributedVectorResultExchangeMessage>);
static_assert(
    !std::is_copy_constructible_v<chronos::cluster::DistributedVectorResultExchangeReader>);
static_assert(
    !std::is_move_constructible_v<chronos::cluster::DistributedVectorResultExchangeReader>);
static_assert(
    std::is_nothrow_constructible_v<chronos::cluster::DistributedVectorResultExchangeReader,
                                    chronos::query::DistributedVectorResultSchema&&>);
static_assert(!std::is_constructible_v<chronos::cluster::DistributedVectorResultExchangeReader,
                                       const chronos::query::DistributedVectorResultSchema&>);
static_assert(
    !std::is_copy_constructible_v<chronos::cluster::DistributedVectorResultExchangeWriteCursor>);

namespace {
[[maybe_unused]] const auto kEncode =
    &chronos::cluster::encode_distributed_vector_result_exchange_message_v2;
[[maybe_unused]] const auto kDecode =
    &chronos::cluster::decode_distributed_vector_result_exchange_message_v2_exact;
[[maybe_unused]] const auto kCreateCursor =
    &chronos::cluster::DistributedVectorResultExchangeWriteCursor::create;
} // namespace
