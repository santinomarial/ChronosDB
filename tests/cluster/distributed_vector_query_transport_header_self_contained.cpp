#include "chronos/cluster/distributed_vector_query_transport.hpp"

namespace {
[[maybe_unused]] const auto kEncode = &chronos::cluster::encode_distributed_vector_query_request_v1;
[[maybe_unused]] const auto kDecode = &chronos::cluster::decode_distributed_vector_query_request_v1;
[[maybe_unused]] const auto kEncodeResponse =
    &chronos::cluster::encode_distributed_vector_query_response_v1;
[[maybe_unused]] const auto kDecodeResponse =
    &chronos::cluster::decode_distributed_vector_query_response_v1;
} // namespace
